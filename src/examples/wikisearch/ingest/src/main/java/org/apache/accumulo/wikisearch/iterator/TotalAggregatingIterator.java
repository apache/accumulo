/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.wikisearch.iterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;
import org.apache.accumulo.core.iterators.conf.ColumnToClassMapping;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;

/**
 * Aggregate all values with the same key (row, colf, colq, colVis.).
 * 
 */

public class TotalAggregatingIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
  
  private SortedKeyValueIterator<Key,Value> iterator;
  
  private Key workKey = new Key();
  
  private Key aggrKey;
  private Value aggrValue;
  
  private Aggregator agg;
  
  public TotalAggregatingIterator deepCopy(IteratorEnvironment env) {
    return new TotalAggregatingIterator(this, env);
  }
  
  private TotalAggregatingIterator(TotalAggregatingIterator other, IteratorEnvironment env) {
    iterator = other.iterator.deepCopy(env);
    agg = other.agg;
  }
  
  public TotalAggregatingIterator() {}
  
  private void aggregateRowColumn(Aggregator aggr) throws IOException {
    // this function assumes that first value is not delete
    
    workKey.set(iterator.getTopKey());
    
    Key keyToAggregate = workKey;
    
    aggr.reset();
    
    aggr.collect(iterator.getTopValue());
    iterator.next();
    
    while (iterator.hasTop() && iterator.getTopKey().equals(keyToAggregate, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      aggr.collect(iterator.getTopValue());
      iterator.next();
    }
    
    aggrKey = workKey;
    aggrValue = aggr.aggregate();
    
  }
  
  private void findTop() throws IOException {
    // check if aggregation is needed
    if (iterator.hasTop()) {
      aggregateRowColumn(agg);
    }
  }
  
  public TotalAggregatingIterator(SortedKeyValueIterator<Key,Value> iterator, ColumnToClassMapping<Aggregator> aggregators) throws IOException {
    this.iterator = iterator;
  }
  
  @Override
  public Key getTopKey() {
    if (aggrKey != null) {
      return aggrKey;
    }
    return iterator.getTopKey();
  }
  
  @Override
  public Value getTopValue() {
    if (aggrKey != null) {
      return aggrValue;
    }
    return iterator.getTopValue();
  }
  
  @Override
  public boolean hasTop() {
    return aggrKey != null || iterator.hasTop();
  }
  
  @Override
  public void next() throws IOException {
    if (aggrKey != null) {
      aggrKey = null;
      aggrValue = null;
    } else {
      iterator.next();
    }
    
    findTop();
  }
  
  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    // do not want to seek to the middle of a value that should be
    // aggregated...
    
    Range seekRange = maximizeStartKeyTimeStamp(range);
    
    iterator.seek(seekRange, columnFamilies, inclusive);
    findTop();
    
    if (range.getStartKey() != null) {
      while (hasTop() && getTopKey().equals(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
          && getTopKey().getTimestamp() > range.getStartKey().getTimestamp()) {
        // the value has a more recent time stamp, so
        // pass it up
        // log.debug("skipping "+getTopKey());
        next();
      }
      
      while (hasTop() && range.beforeStartKey(getTopKey())) {
        next();
      }
    }
    
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    agg = createAggregator(options);
    this.iterator = source;
  }
  
  @Override
  public IteratorOptions describeOptions() {
    return new IteratorOptions("agg", "Aggregators apply aggregating functions to values with identical keys", null,
        Collections.singletonList("* <aggregatorClass>"));
  }
  
  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (options.size() > 1)
      throw new IllegalArgumentException("This iterator only accepts one configuration option, the name of the aggregating class");
    agg = createAggregator(options);
    return true;
  }
  
  private Aggregator createAggregator(Map<String,String> options) {
    Aggregator a = null;
    for (Entry<String,String> entry : options.entrySet()) {
      try {
        Class<? extends Aggregator> clazz = AccumuloClassLoader.loadClass(entry.getValue(), Aggregator.class);
        a = clazz.newInstance();
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("class not found: " + entry.getValue());
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("instantiation exception: " + entry.getValue());
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("illegal access exception: " + entry.getValue());
      }
    }
    return a;
  }
  
  static Range maximizeStartKeyTimeStamp(Range range) {
    Range seekRange = range;
    
    if (range.getStartKey() != null && range.getStartKey().getTimestamp() != Long.MAX_VALUE) {
      Key seekKey = new Key(seekRange.getStartKey());
      seekKey.setTimestamp(Long.MAX_VALUE);
      seekRange = new Range(seekKey, true, range.getEndKey(), range.isEndKeyInclusive());
    }
    
    return seekRange;
  }
}
