/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.fate.util.UtilWaitThread;

/**
 * An iterator that is useful for testing, it sleeps for 3 seconds on each call.
 *
 * config -t ci -s table.iterator.minc.sleepy=21,org.apache.accumulo.core.iterators.SleepyIterator
 *
 */
public class SleepyIterator implements SortedKeyValueIterator<Key,Value> {

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Key getTopKey() {
    UtilWaitThread.sleep(3000);
    return null;
  }

  @Override
  public Value getTopValue() {
    UtilWaitThread.sleep(3000);
    return null;
  }

  @Override
  public boolean hasTop() {
    UtilWaitThread.sleep(3000);
    return false;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    UtilWaitThread.sleep(3000);
  }

  @Override
  public void next() throws IOException {
    UtilWaitThread.sleep(3000);
    throw new UnsupportedOperationException();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    UtilWaitThread.sleep(3000);
  }

}
