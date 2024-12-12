/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.metadata.iterators;

import static org.apache.accumulo.server.metadata.iterators.SetEncodingIterator.getTabletRow;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.server.metadata.ConditionalTabletMutatorImpl;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * Iterator that checks if a column family size is less than or equal a limit as part of a
 * conditional mutation.
 */
public class ColumnFamilySizeLimitIterator extends WrappingIterator {

  private static final String LIMIT_OPT = "limit";
  private static final Text EMPTY = new Text();

  private Long limit;

  private Key startKey = null;
  private Value topValue = null;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    limit = Long.parseLong(options.get(LIMIT_OPT));
    Preconditions.checkState(limit >= 0);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    Text tabletRow = getTabletRow(range);
    Text family = range.getStartKey().getColumnFamily();

    Preconditions.checkArgument(
        family.getLength() > 0 && range.getStartKey().getColumnQualifier().getLength() == 0);

    startKey = new Key(tabletRow, family);
    Key endKey = startKey.followingKey(PartialKey.ROW_COLFAM);

    Range r = new Range(startKey, true, endKey, false);

    var source = getSource();
    source.seek(r, Set.of(startKey.getColumnFamilyData()), true);

    long count = 0;
    while (source.hasTop()) {
      source.next();
      count++;
    }

    if (count <= limit) {
      topValue = new Value("1");
    } else {
      topValue = null;
    }
  }

  @Override
  public boolean hasTop() {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    return topValue != null;
  }

  @Override
  public void next() throws IOException {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    topValue = null;
  }

  @Override
  public Key getTopKey() {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    if (topValue == null) {
      throw new NoSuchElementException();
    }

    return startKey;
  }

  @Override
  public Value getTopValue() {
    if (startKey == null) {
      throw new IllegalStateException("never been seeked");
    }
    if (topValue == null) {
      throw new NoSuchElementException();
    }
    return topValue;
  }

  /**
   * Create a condition that checks if the specified column family's size is less than or equal to
   * the given limit.
   */
  public static Condition createCondition(Text family, long limit) {
    IteratorSetting is = new IteratorSetting(ConditionalTabletMutatorImpl.INITIAL_ITERATOR_PRIO,
        ColumnFamilySizeLimitIterator.class);
    is.addOption(LIMIT_OPT, limit + "");
    return new Condition(family, EMPTY).setValue("1").setIterators(is);
  }
}
