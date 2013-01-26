/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.dataset;

import com.continuuity.api.data.OperationException;

import java.util.List;

/**
 * Defines simple timeseries dataset.
 */
public interface TimeseriesTable {

  /**
   * Stores entry in dataset. This write operation is executed synchronously.
   * See {@link Entry} for more details.
   * @param entry to store.
   * @throws OperationException
   */
  void write(Entry entry) throws OperationException;

  /**
   * Stores entry in dataset. This write operation is executed asynchronously.
   * See {@link Entry} for more details.
   * @param entry to store.
   * @throws OperationException
   */
  void stage(Entry entry) throws OperationException;

  /**
   * Reads entries of a time range with given key and tags set.
   *
   * @param key key of the entries to read
   * @param startTime defines start of the time range to read, inclusive.
   * @param endTime defines end of the time range to read, inclusive.
   * @param tags defines a set of tags ALL of which MUST present in every returned entry. Can be absent, in that case
   *             no filtering by tags will be applied.
   *        NOTE: return entries contain all tags that were providing during writing, NOT passed with this param.
   *
   * @return list of entries that satisfy provided conditions.
   *
   * @throws OperationException when underlying {@link com.continuuity.api.data.dataset.table.Table} throws one
   * @throws IllegalArgumentException when provided condition is incorrect.
   */
  List<Entry> read(byte key[], long startTime, long endTime, byte[]... tags) throws OperationException;

  /**
   * Timeseries dataset entry.
   */
  public static final class Entry {
    private byte[] key;
    private byte[] value;
    private long timestamp;
    private byte[][] tags;

    /**
     * Creates instance of the timeseries entry.
     * @param key key of the entry. E.g. "metric1"
     * @param value value to store
     * @param timestamp timestamp of the entry
     * @param tags optional list of tags associated with the entry
     */
    public Entry(final byte[] key, final byte[] value, final long timestamp, final byte[]... tags) {
      this.key = key;
      this.value = value;
      this.timestamp = timestamp;
      this.tags = tags;
    }

    public byte[] getKey() {
      return key;
    }

    public byte[] getValue() {
      return value;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public byte[][] getTags() {
      return tags;
    }
  }

}
