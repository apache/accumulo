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

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SynchronizedServerFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.commons.collections.map.LRUMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VisibilityFilter extends SynchronizedServerFilter {
  protected VisibilityEvaluator ve;
  protected ByteSequence defaultVisibility;
  protected LRUMap cache;
  protected Authorizations authorizations;

  private static final Logger log = LoggerFactory.getLogger(VisibilityFilter.class);

  public VisibilityFilter(SortedKeyValueIterator<Key,Value> iterator, Authorizations authorizations, byte[] defaultVisibility) {
    super(iterator);
    this.ve = new VisibilityEvaluator(authorizations);
    this.authorizations = authorizations;
    this.defaultVisibility = new ArrayByteSequence(defaultVisibility);
    this.cache = new LRUMap(1000);
  }

  @Override
  public synchronized SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new VisibilityFilter(source.deepCopy(env), authorizations, defaultVisibility.toArray());
  }

  @Override
  protected boolean accept(Key k, Value v) {
    ByteSequence testVis = k.getColumnVisibilityData();

    if (testVis.length() == 0 && defaultVisibility.length() == 0)
      return true;
    else if (testVis.length() == 0)
      testVis = defaultVisibility;

    Boolean b = (Boolean) cache.get(testVis);
    if (b != null)
      return b;

    try {
      Boolean bb = ve.evaluate(new ColumnVisibility(testVis.toArray()));
      cache.put(testVis, bb);
      return bb;
    } catch (VisibilityParseException e) {
      log.error("Parse Error", e);
      return false;
    } catch (BadArgumentException e) {
      log.error("Parse Error", e);
      return false;
    }
  }
}
