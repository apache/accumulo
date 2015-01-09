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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;
import org.apache.accumulo.core.iterators.conf.ColumnToClassMapping;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.log4j.Logger;

/**
 * This iterator wraps another iterator. It automatically aggregates.
 *
 * @deprecated since 1.4, replaced by {@link org.apache.accumulo.core.iterators.Combiner}
 */

@Deprecated
public class AggregatingIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

  private SortedKeyValueIterator<Key,Value> iterator;
  private ColumnToClassMapping<Aggregator> aggregators;

  private Key workKey = new Key();

  private Key aggrKey;
  private Value aggrValue;
  // private boolean propogateDeletes;
  private static final Logger log = Logger.getLogger(AggregatingIterator.class);

  public AggregatingIterator deepCopy(IteratorEnvironment env) {
    return new AggregatingIterator(this, env);
  }

  private AggregatingIterator(AggregatingIterator other, IteratorEnvironment env) {
    iterator = other.iterator.deepCopy(env);
    aggregators = other.aggregators;
  }

  public AggregatingIterator() {}

  private void aggregateRowColumn(Aggregator aggr) throws IOException {
    // this function assumes that first value is not delete

    if (iterator.getTopKey().isDeleted())
      return;

    workKey.set(iterator.getTopKey());

    Key keyToAggregate = workKey;

    aggr.reset();

    aggr.collect(iterator.getTopValue());
    iterator.next();

    while (iterator.hasTop() && !iterator.getTopKey().isDeleted() && iterator.getTopKey().equals(keyToAggregate, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      aggr.collect(iterator.getTopValue());
      iterator.next();
    }

    aggrKey = workKey;
    aggrValue = aggr.aggregate();

  }

  private void findTop() throws IOException {
    // check if aggregation is needed
    if (iterator.hasTop()) {
      Aggregator aggr = aggregators.getObject(iterator.getTopKey());
      if (aggr != null) {
        aggregateRowColumn(aggr);
      }
    }
  }

  public AggregatingIterator(SortedKeyValueIterator<Key,Value> iterator, ColumnToClassMapping<Aggregator> aggregators) throws IOException {
    this.iterator = iterator;
    this.aggregators = aggregators;
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

    Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);

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

    this.iterator = source;

    try {
      String context = null;
      if (null != env)
        context = env.getConfig().get(Property.TABLE_CLASSPATH);
      this.aggregators = new ColumnToClassMapping<Aggregator>(options, Aggregator.class, context);
    } catch (ClassNotFoundException e) {
      log.error(e.toString());
      throw new IllegalArgumentException(e);
    } catch (InstantiationException e) {
      log.error(e.toString());
      throw new IllegalArgumentException(e);
    } catch (IllegalAccessException e) {
      log.error(e.toString());
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public IteratorOptions describeOptions() {
    return new IteratorOptions("agg", "Aggregators apply aggregating functions to values with identical keys", null,
        Collections.singletonList("<columnName> <aggregatorClass>"));
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    for (Entry<String,String> entry : options.entrySet()) {
      String classname = entry.getValue();
      if (classname == null)
        throw new IllegalArgumentException("classname null");
      Class<? extends Aggregator> clazz;
      try {
        clazz = AccumuloVFSClassLoader.loadClass(classname, Aggregator.class);
        clazz.newInstance();
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("class not found: " + classname);
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("instantiation exception: " + classname);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("illegal access exception: " + classname);
      }
    }
    return true;
  }
}
