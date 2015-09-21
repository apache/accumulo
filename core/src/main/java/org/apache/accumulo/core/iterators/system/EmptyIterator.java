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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class EmptyIterator implements InterruptibleIterator {

  public static final EmptyIterator EMPTY_ITERATOR = new EmptyIterator();

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {}

  @Override
  public boolean hasTop() {
    return false;
  }

  @Override
  public void next() throws IOException {
    // nothing should call this since hasTop always returns false
    throw new UnsupportedOperationException();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {}

  @Override
  public Key getTopKey() {
    // nothing should call this since hasTop always returns false
    throw new UnsupportedOperationException();
  }

  @Override
  public Value getTopValue() {
    // nothing should call this since hasTop always returns false
    throw new UnsupportedOperationException();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return EMPTY_ITERATOR;
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {}
}
