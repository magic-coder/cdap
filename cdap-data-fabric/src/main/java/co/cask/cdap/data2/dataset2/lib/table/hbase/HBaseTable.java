/*
 * Copyright © 2014-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.dataset2.lib.table.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Filter;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.lib.table.BufferingTable;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.IncrementValue;
import co.cask.cdap.data2.dataset2.lib.table.PutValue;
import co.cask.cdap.data2.dataset2.lib.table.Update;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.PrefixedNamespaces;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionCodec;
import co.cask.tephra.TxConstants;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * Dataset client for HBase tables.
 */
// todo: do periodic flush when certain threshold is reached
// todo: extract separate "no delete inside tx" table?
// todo: consider writing & reading using HTable to do in multi-threaded way
public class HBaseTable extends BufferingTable {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTable.class);

  public static final String DELTA_WRITE = "d";

  private final HTable hTable;
  private final String hTableName;
  private final byte[] columnFamily;
  private final TransactionCodec txCodec;
  // name length + name of the table: handy to have one cached
  private final byte[] nameAsTxChangePrefix;

  private Transaction tx;

  public HBaseTable(DatasetContext datasetContext, DatasetSpecification spec,
                    CConfiguration cConf, Configuration hConf, HBaseTableUtil tableUtil) throws IOException {
    super(PrefixedNamespaces.namespace(cConf, datasetContext.getNamespaceId(), spec.getName()),
          ConflictDetection.valueOf(spec.getProperty(PROPERTY_CONFLICT_LEVEL, ConflictDetection.ROW.name())),
          HBaseTableAdmin.supportsReadlessIncrements(spec));
    TableId tableId = TableId.from(datasetContext.getNamespaceId(), spec.getName());
    HTable hTable = tableUtil.createHTable(hConf, tableId);
    // todo: make configurable
    hTable.setWriteBufferSize(HBaseTableUtil.DEFAULT_WRITE_BUFFER_SIZE);
    hTable.setAutoFlush(false);
    this.hTable = hTable;
    this.hTableName = Bytes.toStringBinary(hTable.getTableName());
    this.columnFamily = HBaseTableAdmin.getColumnFamily(spec);
    this.txCodec = new TransactionCodec();
    // Overriding the hbase tx change prefix so it resembles the hbase table name more closely, since the HBase
    // table name is not the same as the dataset name anymore
    this.nameAsTxChangePrefix = Bytes.add(new byte[]{(byte) this.hTableName.length()}, Bytes.toBytes(this.hTableName));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("hTable", hTable)
                  .add("hTableName", hTableName)
                  .add("nameAsTxChangePrefix", nameAsTxChangePrefix)
                  .toString();
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    this.tx = tx;
  }

  @Override
  public List<Row> get(List<co.cask.cdap.api.dataset.table.Get> gets) {
    List<Get> hbaseGets = Lists.transform(gets, new Function<co.cask.cdap.api.dataset.table.Get, Get>() {
      @Nullable
      @Override
      public Get apply(co.cask.cdap.api.dataset.table.Get get) {
        List<byte[]> cols = get.getColumns();
        return createGet(get.getRow(), cols == null ? null : cols.toArray(new byte[cols.size()][]));
      }
    });
    try {
      Result[] results = hTable.get(hbaseGets);
      return Lists.transform(Arrays.asList(results), new Function<Result, Row>() {
        @Nullable
        @Override
        public Row apply(Result result) {
          Map<byte[], byte[]> familyMap = result.getFamilyMap(columnFamily);
          return new co.cask.cdap.api.dataset.table.Result(result.getRow(),
              familyMap != null ? familyMap : ImmutableMap.<byte[], byte[]>of());
        }
      });
    } catch (IOException ioe) {
      throw new DataSetException("Multi-get failed on table " + hTableName, ioe);
    }
  }

  @Override
  public byte[] getNameAsTxChangePrefix() {
    return nameAsTxChangePrefix;
  }

  @Override
  protected void persist(NavigableMap<byte[], NavigableMap<byte[], Update>> buff) throws Exception {
    List<Put> puts = Lists.newArrayList();
    for (Map.Entry<byte[], NavigableMap<byte[], Update>> row : buff.entrySet()) {
      Put put = new Put(row.getKey());
      Put incrementPut = null;
      for (Map.Entry<byte[], Update> column : row.getValue().entrySet()) {
        // we want support tx and non-tx modes
        if (tx != null) {
          // TODO: hijacking timestamp... bad
          Update val = column.getValue();
          if (val instanceof IncrementValue) {
            incrementPut = getIncrementalPut(incrementPut, row.getKey());
            incrementPut.add(columnFamily, column.getKey(), tx.getWritePointer(),
                             Bytes.toBytes(((IncrementValue) val).getValue()));
          } else if (val instanceof PutValue) {
            put.add(columnFamily, column.getKey(), tx.getWritePointer(),
                    wrapDeleteIfNeeded(((PutValue) val).getValue()));
          }
        } else {
          Update val = column.getValue();
          if (val instanceof IncrementValue) {
            incrementPut = getIncrementalPut(incrementPut, row.getKey());
            incrementPut.add(columnFamily, column.getKey(),
                             Bytes.toBytes(((IncrementValue) val).getValue()));
          } else if (val instanceof PutValue) {
            put.add(columnFamily, column.getKey(), ((PutValue) val).getValue());
          }
        }
      }
      if (incrementPut != null) {
        puts.add(incrementPut);
      }
      if (!put.isEmpty()) {
        puts.add(put);
      }
    }
    if (!puts.isEmpty()) {
      hTable.put(puts);
      hTable.flushCommits();
    } else {
      LOG.info("No writes to persist!");
    }
  }

  private Put getIncrementalPut(Put existing, byte[] row) {
    if (existing != null) {
      return existing;
    }
    Put put = new Put(row);
    put.setAttribute(DELTA_WRITE, Bytes.toBytes(true));
    return put;
  }

  @Override
  protected void undo(NavigableMap<byte[], NavigableMap<byte[], Update>> persisted) throws Exception {
    // NOTE: we use Delete with the write pointer as the specific version to delete.
    List<Delete> deletes = Lists.newArrayList();
    for (Map.Entry<byte[], NavigableMap<byte[], Update>> row : persisted.entrySet()) {
      Delete delete = new Delete(row.getKey());
      for (Map.Entry<byte[], Update> column : row.getValue().entrySet()) {
        // we want support tx and non-tx modes
        if (tx != null) {
          // TODO: hijacking timestamp... bad
          delete.deleteColumn(columnFamily, column.getKey(), tx.getWritePointer());
        } else {
          delete.deleteColumns(columnFamily, column.getKey());
        }
      }
      deletes.add(delete);
    }
    hTable.delete(deletes);
    hTable.flushCommits();
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, byte[] startColumn, byte[] stopColumn, int limit)
    throws Exception {

    // todo: this is very inefficient: column range + limit should be pushed down via server-side filters
    return getRange(getInternal(row, null), startColumn, stopColumn, limit);
  }

  @Override
  protected NavigableMap<byte[], byte[]> getPersisted(byte[] row, @Nullable byte[][] columns) throws Exception {
    return getInternal(row, columns);
  }

  @Override
  protected Scanner scanPersisted(co.cask.cdap.api.dataset.table.Scan scan) throws Exception {
    Scan hScan = new Scan();
    hScan.addFamily(columnFamily);
    // todo: should be configurable
    // NOTE: by default we assume scanner is used in mapreduce job, hence no cache blocks
    hScan.setCacheBlocks(false);
    hScan.setCaching(1000);

    byte[] startRow = scan.getStartRow();
    byte[] stopRow = scan.getStopRow();
    if (startRow != null) {
      hScan.setStartRow(startRow);
    }
    if (stopRow != null) {
      hScan.setStopRow(stopRow);
    }

    setFilterIfNeeded(hScan, scan.getFilter());

    addToOperation(hScan, tx);

    ResultScanner resultScanner = hTable.getScanner(hScan);
    return new HBaseScanner(resultScanner, columnFamily);
  }

  private void setFilterIfNeeded(Scan scan, @Nullable Filter filter) {
    if (filter == null) {
      return;
    }

    if (filter instanceof FuzzyRowFilter) {
      FuzzyRowFilter fuzzyRowFilter = (FuzzyRowFilter) filter;
      List<Pair<byte[], byte[]>> fuzzyPairs =
        Lists.newArrayListWithExpectedSize(fuzzyRowFilter.getFuzzyKeysData().size());
      for (ImmutablePair<byte[], byte[]> pair : fuzzyRowFilter.getFuzzyKeysData()) {
        fuzzyPairs.add(Pair.newPair(pair.getFirst(), pair.getSecond()));
      }
      scan.setFilter(new org.apache.hadoop.hbase.filter.FuzzyRowFilter(fuzzyPairs));
    } else {
      throw new IllegalArgumentException("Unsupported filter: " + filter);
    }
  }

  private Get createGet(byte[] row, @Nullable byte[][] columns) {
    Get get = new Get(row);
    get.addFamily(columnFamily);
    if (columns != null && columns.length > 0) {
      for (byte[] column : columns) {
        get.addColumn(columnFamily, column);
      }
    } else {
      get.addFamily(columnFamily);
    }

    try {
      // no tx logic needed
      if (tx == null) {
        get.setMaxVersions(1);
      } else {
        addToOperation(get, tx);
      }
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
    return get;
  }

  private NavigableMap<byte[], byte[]> getInternal(byte[] row, @Nullable byte[][] columns) throws IOException {
    Get get = createGet(row, columns);

    // no tx logic needed
    if (tx == null) {
      get.setMaxVersions(1);
      Result result = hTable.get(get);
      return result.isEmpty() ? EMPTY_ROW_MAP : result.getFamilyMap(columnFamily);
    }

    addToOperation(get, tx);

    Result result = hTable.get(get);
    return getRowMap(result, columnFamily);
  }

  static NavigableMap<byte[], byte[]> getRowMap(Result result, byte[] columnFamily) {
    if (result.isEmpty()) {
      return EMPTY_ROW_MAP;
    }

    // note: server-side filters all everything apart latest visible for us, so we can flatten it here
    NavigableMap<byte[], NavigableMap<Long, byte[]>> versioned =
      result.getMap().get(columnFamily);

    NavigableMap<byte[], byte[]> rowMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> column : versioned.entrySet()) {
      rowMap.put(column.getKey(), column.getValue().firstEntry().getValue());
    }

    return unwrapDeletes(rowMap);
  }

  private void addToOperation(OperationWithAttributes op, Transaction tx) throws IOException {
    op.setAttribute(TxConstants.TX_OPERATION_ATTRIBUTE_KEY, txCodec.encode(tx));
  }
}
