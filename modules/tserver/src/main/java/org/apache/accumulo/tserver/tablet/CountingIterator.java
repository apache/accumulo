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
package org.apache.accumulo.tserver.tablet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

public class CountingIterator extends WrappingIterator {

  private long count;
  private final ArrayList<CountingIterator> deepCopies;
  private final AtomicLong entriesRead;

  @Override
  public CountingIterator deepCopy(IteratorEnvironment env) {
    return new CountingIterator(this, env);
  }

  private CountingIterator(CountingIterator other, IteratorEnvironment env) {
    setSource(other.getSource().deepCopy(env));
    count = 0;
    this.deepCopies = other.deepCopies;
    this.entriesRead = other.entriesRead;
    deepCopies.add(this);
  }

  public CountingIterator(SortedKeyValueIterator<Key,Value> source, AtomicLong entriesRead) {
    deepCopies = new ArrayList<>();
    this.setSource(source);
    count = 0;
    this.entriesRead = entriesRead;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void next() throws IOException {
    super.next();
    count++;
    if (count % 1024 == 0) {
      entriesRead.addAndGet(1024);
    }
  }

  public long getCount() {
    long sum = 0;
    for (CountingIterator dc : deepCopies) {
      sum += dc.count;
    }

    return count + sum;
  }
}
