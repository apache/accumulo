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
package org.apache.accumulo.test.functional;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Iterator used in ScannerContextIT that reverses the bytes of the value
 *
 */
public class ValueReversingIterator implements SortedKeyValueIterator<Key,Value> {

  protected SortedKeyValueIterator<Key,Value> source;

  public ValueReversingIterator deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  public Key getTopKey() {
    return source.getTopKey();
  }

  public Value getTopValue() {
    byte[] buf = source.getTopValue().get();
    ArrayUtils.reverse(buf);
    return new Value(buf);
  }

  public boolean hasTop() {
    return source.hasTop();
  }

  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
  }

  public void next() throws IOException {
    source.next();
  }

  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
  }
}
