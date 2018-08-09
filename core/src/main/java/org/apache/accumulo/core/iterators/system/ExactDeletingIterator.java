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

package org.apache.accumulo.core.iterators.system;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.ServerWrappingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class ExactDeletingIterator extends ServerWrappingIterator {
  private boolean propogateDeletes;
  private Key workKey = new Key();

  @Override
  public ExactDeletingIterator deepCopy(IteratorEnvironment env) {
    return new ExactDeletingIterator(this, env);
  }

  public ExactDeletingIterator(ExactDeletingIterator other, IteratorEnvironment env) {
    super(other.source.deepCopy(env));
    propogateDeletes = other.propogateDeletes;
  }

  public ExactDeletingIterator(SortedKeyValueIterator<Key,Value> iterator,
      boolean propogateDeletes) {
    super(iterator);
    this.propogateDeletes = propogateDeletes;
  }

  @Override
  public void next() throws IOException {
    if (source.getTopKey().isDeleted())
      skipRowColumnTime();
    else
      source.next();
    findTop();
  }

  private static Range adjustRange(Range range) {
    Range seekRange = range;

    if (range.getStartKey() != null
        && (range.isStartKeyInclusive() ^ range.getStartKey().isDeleted())) {
      Key seekKey = new Key(seekRange.getStartKey());
      seekKey.setDeleted(true);
      seekRange = new Range(seekKey, true, range.getEndKey(), range.isEndKeyInclusive());
    }

    return seekRange;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    // do not want to seek to the middle of a row
    Range seekRange = adjustRange(range);

    source.seek(seekRange, columnFamilies, inclusive);
    findTop();

    if (propogateDeletes && range != seekRange) {
      while (source.hasTop() && range.beforeStartKey(source.getTopKey())) {
        next();
      }
    }
  }

  private void findTop() throws IOException {
    if (!propogateDeletes) {
      while (source.hasTop() && source.getTopKey().isDeleted()) {
        skipRowColumnTime();
      }
    }
  }

  private void skipRowColumnTime() throws IOException {
    workKey.set(source.getTopKey());

    Key keyToSkip = workKey;
    source.next();

    while (source.hasTop()
        && source.getTopKey().equals(keyToSkip, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME)) {
      source.next();
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }
}
