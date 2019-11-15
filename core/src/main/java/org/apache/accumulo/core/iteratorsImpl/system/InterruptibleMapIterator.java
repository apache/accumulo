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
package org.apache.accumulo.core.iteratorsImpl.system;

import java.io.IOException;
import java.util.Collection;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.SortedMapIterator;

/**
 * A SortedMapIterator but is made Interruptible for system use. This behavior was moved here so
 * that SortedMapIterator could be treated as public API.
 */
public class InterruptibleMapIterator extends SortedMapIterator implements InterruptibleIterator {

  private AtomicBoolean interruptFlag;
  private int interruptCheckCount = 0;

  public InterruptibleMapIterator(SortedMap<Key,Value> map, AtomicBoolean interruptFlag) {
    super(map);
    this.interruptFlag = interruptFlag;
  }

  @Override
  public void next() throws IOException {
    if (interruptFlag != null && interruptCheckCount++ % 100 == 0 && interruptFlag.get())
      throw new IterationInterruptedException();

    super.next();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    if (interruptFlag != null && interruptFlag.get())
      throw new IterationInterruptedException();

    super.seek(range, columnFamilies, inclusive);
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    this.interruptFlag = flag;
  }
}
