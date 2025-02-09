/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.metrics.store.cube;

import co.cask.cdap.api.metrics.TagValue;
import co.cask.cdap.api.metrics.TimeSeriesInterpolator;
import co.cask.cdap.api.metrics.TimeValue;
import co.cask.cdap.metrics.store.timeseries.Fact;
import co.cask.cdap.metrics.store.timeseries.FactScan;
import co.cask.cdap.metrics.store.timeseries.FactScanResult;
import co.cask.cdap.metrics.store.timeseries.FactScanner;
import co.cask.cdap.metrics.store.timeseries.FactTable;
import co.cask.cdap.metrics.store.timeseries.MeasureType;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link Cube}.
 */
public class DefaultCube implements Cube {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultCube.class);

  private static final TagValueComparator TAG_VALUE_COMPARATOR = new TagValueComparator();

  private final Map<Integer, FactTable> resolutionToFactTable;

  private final Collection<? extends Aggregation> aggregations;

  public DefaultCube(int[] resolutions, FactTableSupplier factTableSupplier,
                     Collection<? extends Aggregation> aggregations) {
    this.aggregations = aggregations;
    this.resolutionToFactTable = Maps.newHashMap();
    for (int resolution : resolutions) {
      resolutionToFactTable.put(resolution, factTableSupplier.get(resolution, 3600));
    }
  }

  @Override
  public void add(CubeFact fact) throws Exception {
    add(ImmutableList.of(fact));
  }

  @Override
  public void add(Collection<? extends CubeFact> facts) throws Exception {
    List<Fact> toWrite = Lists.newArrayList();
    for (CubeFact fact : facts) {
      for (Aggregation agg : aggregations) {
        if (agg.accept(fact)) {
          List<TagValue> tagValues = Lists.newArrayList();
          for (String tagName : agg.getTagNames()) {
            tagValues.add(new TagValue(tagName, fact.getTagValues().get(tagName)));
          }
          toWrite.add(new Fact(tagValues, fact.getMeasureType(), fact.getMeasureName(), fact.getTimeValue()));
        }
      }
    }

    for (FactTable table : resolutionToFactTable.values()) {
      table.add(toWrite);
    }
  }

  @Override
  public Collection<TimeSeries> query(CubeQuery query) throws Exception {
    /*
      CubeQuery example: "dataset read ops for app per dataset". Or:

      SELECT count('read.ops')                                     << measure name and type
      FROM Cube
      GROUP BY dataset,                                            << groupByTags
      WHERE namespace='ns1' AND app='myApp' AND program='myFlow'   << sliceByTags

      Execution:

      1) find aggregation to supply results

      Here, we need aggregation that has following dimensions: 'namespace', 'app', 'program', 'dataset'.

      Ideally (to reduce the scan range), 'dataset' should be in the end, other tags as close to the beginning
      as possible, and minimal number of other "unspecified" tags.

      Let's say we found aggregation: 'namespace', 'app', 'program', 'instance', 'dataset'

      2) build a scan in the aggregation

      For scan we set "any" into the dimension values that aggregation has but query doesn't define value for:

      'namespace'='ns1', 'app'='myApp', 'program'='myFlow', 'instance'=*, 'dataset'=*

      Plus specified measure & aggregation?:

      'measureName'='read.ops'
      'measureType'='COUNTER'

      3) While scanning build a table: tag values -> time -> value. Use measureType as values aggregate
         function if needed.
    */

    if (!resolutionToFactTable.containsKey(query.getResolution())) {
      throw new IllegalArgumentException("There's no data aggregated for specified resolution to satisfy the query: " +
                                           query.toString());
    }

    Aggregation agg = findAggregation(query);
    if (agg == null) {
      throw new IllegalArgumentException("There's no data aggregated for specified tags to satisfy the query: " +
                                           query.toString());
    }

    List<TagValue> tagValues = Lists.newArrayList();
    for (String tagName : agg.getTagNames()) {
      // if not defined in query, will be set as null, which means "any"
      tagValues.add(new TagValue(tagName, query.getSliceByTags().get(tagName)));
    }

    FactScan scan = new FactScan(query.getStartTs(), query.getEndTs(), query.getMeasureName(), tagValues);
    FactTable table = resolutionToFactTable.get(query.getResolution());
    FactScanner scanner = table.scan(scan);
    Table<Map<String, String>, Long, Long> resultTable = getTimeSeries(query, scanner);
    return convertToQueryResult(query, resultTable);
  }

  @Override
  public void delete(CubeDeleteQuery query) throws Exception {
    //this may be very inefficient and its better to use TTL, this is to only support existing old functionality.
    List<TagValue> tagValues = Lists.newArrayList();
    // find all the aggregations that match the sliceByTags in the query and
    // use the tag values of the aggregation to delete entries in all the fact-tables.
    for (Aggregation agg : aggregations) {
      if (agg.getTagNames().containsAll(query.getSliceByTags().keySet())) {
        tagValues.clear();
        for (String tagName : agg.getTagNames()) {
          tagValues.add(new TagValue(tagName, query.getSliceByTags().get(tagName)));
        }
        FactTable factTable = resolutionToFactTable.get(query.getResolution());
        FactScan scan = new FactScan(query.getStartTs(), query.getEndTs(), query.getMeasureName(), tagValues);
        factTable.delete(scan);
      }
    }
  }

  public Collection<TagValue> findNextAvailableTags(CubeExploreQuery query) throws Exception {
    LOG.trace("Searching for next-level context, query: {}", query);

    // In each aggregation that matches given tags, try to fill in value in a single null-valued given tag.
    // NOTE: that we try to fill in first value that is non-null-valued in a stored record
    //       (see FactTable#findSingleTagValue)
    SortedSet<TagValue> result = Sets.newTreeSet(TAG_VALUE_COMPARATOR);

    // todo: the passed query should have map instead
    LinkedHashMap<String, String> slice = Maps.newLinkedHashMap();
    for (TagValue tagValue : query.getTagValues()) {
      slice.put(tagValue.getTagName(), tagValue.getValue());
    }

    FactTable table = resolutionToFactTable.get(query.getResolution());

    for (Aggregation agg : aggregations) {
      if (agg.getTagNames().containsAll(slice.keySet())) {
        result.addAll(table.findSingleTagValue(agg.getTagNames(), slice, query.getStartTs(), query.getEndTs()));
      }
    }

    return result;
  }

  @Override
  public Collection<String> findMeasureNames(CubeExploreQuery query) throws Exception {
    LOG.trace("Searching for metrics, query: {}", query);

    // In each aggregation that matches given tags, try to find metric names
    SortedSet<String> result = Sets.newTreeSet();

    // todo: the passed query should have map instead
    LinkedHashMap<String, String> slice = Maps.newLinkedHashMap();
    for (TagValue tagValue : query.getTagValues()) {
      slice.put(tagValue.getTagName(), tagValue.getValue());
    }

    FactTable table = resolutionToFactTable.get(query.getResolution());

    for (Aggregation agg : aggregations) {
      if (agg.getTagNames().containsAll(slice.keySet())) {
        result.addAll(table.findMeasureNames(agg.getTagNames(), slice, query.getStartTs(), query.getEndTs()));
      }
    }

    return result;
  }

  @Nullable
  private Aggregation findAggregation(CubeQuery query) {
    Aggregation currentBest = null;

    for (Aggregation agg : aggregations) {
      if (agg.getTagNames().containsAll(query.getGroupByTags()) &&
        agg.getTagNames().containsAll(query.getSliceByTags().keySet())) {

        // todo: choose aggregation smarter than just by number of tags :)
        if (currentBest == null || currentBest.getTagNames().size() > agg.getTagNames().size()) {
          currentBest = agg;
        }
      }
    }

    return currentBest;
  }

  private Table<Map<String, String>, Long, Long> getTimeSeries(CubeQuery query, FactScanner scanner) {
    // tag values -> time -> values
    Table<Map<String, String>, Long, Long> resultTable = HashBasedTable.create();
    while (scanner.hasNext()) {
      FactScanResult next = scanner.next();

      boolean skip = false;
      // using tree map, as we are using it as a key for a map
      Map<String, String> seriesTags = Maps.newTreeMap();
      for (String tagName : query.getGroupByTags()) {
        // todo: use Map<String, String> instead of List<TagValue> into a String, String, everywhere
        for (TagValue tagValue : next.getTagValues()) {
          if (tagName.equals(tagValue.getTagName())) {
            if (tagValue.getValue() == null) {
              // Currently, we do NOT return null as grouped by value.
              // Depending on whether tag is required or not the records with null value in it may or may not be in
              // aggregation. At this moment, the choosing of the aggregation for query doesn't look at this, so
              // potentially null may or may not be included in results, depending on the aggregation selected
              // querying. We don't want to produce inconsistent results varying due to different aggregations selected,
              // so don't return nulls in any of those cases.
              skip = true;
              continue;
            }
            seriesTags.put(tagName, tagValue.getValue());
            break;
          }
        }
      }

      if (skip) {
        continue;
      }

      for (TimeValue timeValue : next) {
        if (MeasureType.COUNTER == query.getMeasureType()) {
          Long value = resultTable.get(seriesTags, timeValue.getTimestamp());
          value = value == null ? 0 : value;
          value += timeValue.getValue();
          resultTable.put(seriesTags, timeValue.getTimestamp(), value);
        } else if (MeasureType.GAUGE == query.getMeasureType()) {
          resultTable.put(seriesTags, timeValue.getTimestamp(), timeValue.getValue());
        } else {
          // should never happen: developer error
          throw new RuntimeException("Unknown MeasureType: " + query.getMeasureType());
        }
      }
    }
    return resultTable;
  }

  private Collection<TimeSeries> convertToQueryResult(CubeQuery query,
                                                      Table<Map<String, String>, Long, Long> aggValuesToTimeValues) {
    List<TimeSeries> result = Lists.newArrayList();
    for (Map.Entry<Map<String, String>, Map<Long, Long>> row : aggValuesToTimeValues.rowMap().entrySet()) {
      List<TimeValue> timeValues = Lists.newArrayList();
      for (Map.Entry<Long, Long> timeValue : row.getValue().entrySet()) {
        timeValues.add(new TimeValue(timeValue.getKey(), timeValue.getValue()));
      }
      Collections.sort(timeValues);
      PeekingIterator<TimeValue> timeValueItor = Iterators.peekingIterator(
        new TimeSeriesInterpolator(timeValues, query.getInterpolator(), query.getResolution()).iterator());
      List<TimeValue> resultTimeValues = Lists.newArrayList();
      while (timeValueItor.hasNext()) {
        TimeValue timeValue = timeValueItor.next();
        resultTimeValues.add(new TimeValue(timeValue.getTimestamp(), timeValue.getValue()));
      }
      result.add(new TimeSeries(query.getMeasureName(), row.getKey(), resultTimeValues));
    }

    return result;
  }

  private static final class TagValueComparator implements Comparator<TagValue> {
    @Override
    public int compare(TagValue t1, TagValue t2) {
      int cmp = t1.getTagName().compareTo(t2.getTagName());
      if (cmp != 0) {
        return cmp;
      }
      if (t1.getValue() == null) {
        if (t2.getValue() == null) {
          return 0;
        } else {
          return -1;
        }
      }
      if (t2.getValue() == null) {
        return 1;
      }
      return t1.getValue().compareTo(t2.getValue());
    }

  }
}
