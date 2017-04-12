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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.InterruptibleIterator;

class PartialMutationSkippingIterator extends SkippingIterator implements InterruptibleIterator {

  private int kvCount;

  public PartialMutationSkippingIterator(SortedKeyValueIterator<Key,Value> source, int maxKVCount) {
    setSource(source);
    this.kvCount = maxKVCount;
  }

  @Override
  protected void consume() throws IOException {
    while (getSource().hasTop() && ((MemKey) getSource().getTopKey()).getKVCount() > kvCount)
      getSource().next();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new PartialMutationSkippingIterator(getSource().deepCopy(env), kvCount);
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    ((InterruptibleIterator) getSource()).setInterruptFlag(flag);
  }

}
