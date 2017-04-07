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
package org.apache.accumulo.tserver;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iterators.system.InterruptibleIterator;

class MemKeyConversionIterator extends WrappingIterator implements InterruptibleIterator {
  private MemKey currKey = null;
  private Value currVal = null;

  public MemKeyConversionIterator(SortedKeyValueIterator<Key,Value> source) {
    super();
    setSource(source);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new MemKeyConversionIterator(getSource().deepCopy(env));
  }

  @Override
  public Key getTopKey() {
    return currKey;
  }

  @Override
  public Value getTopValue() {
    return currVal;
  }

  private void getTopKeyVal() {
    Key k = super.getTopKey();
    Value v = super.getTopValue();
    if (k instanceof MemKey || k == null) {
      currKey = (MemKey) k;
      currVal = v;
      return;
    }

    MemValue mv = MemValue.decode(v);
    currVal = mv.value;
    currKey = new MemKey(k, mv.kvCount);
  }

  @Override
  public void next() throws IOException {
    super.next();
    if (hasTop())
      getTopKeyVal();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    super.seek(range, columnFamilies, inclusive);

    if (hasTop())
      getTopKeyVal();

    Key k = range.getStartKey();
    if (k instanceof MemKey && hasTop()) {
      while (hasTop() && currKey.compareTo(k) < 0)
        next();
    }
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    ((InterruptibleIterator) getSource()).setInterruptFlag(flag);
  }

}
