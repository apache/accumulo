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
package org.apache.accumulo.core.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.accumulo.core.spi.util.SharedBatchWriterQueue;

/**
 * SharedBatchWriterQueue backed by an ArrayBlockingQueue. Calls to
 * {@code #add(org.apache.accumulo.coordinator.SharedBatchWriterQueue.Work} will block if the
 * backing queue is full and will wait until the queued Work item has been flushed to the underlying
 * table.
 */
public class SharedBatchWriterBlockingQueue implements SharedBatchWriterQueue {

  private ArrayBlockingQueue<Work> queue = null;

  @Override
  public void init(InitParameters params) {
    queue = new ArrayBlockingQueue<Work>(params.getQueueSize());
  }

  @Override
  public void add(Work work) throws InterruptedException {
    queue.put(work);
    work.getFuture().join();
  }

  @Override
  public Work remove() throws InterruptedException {
    return queue.take();
  }

  @Override
  public void removeAll(Collection<Work> work) {
    queue.drainTo(work);
  }

  @Override
  public Iterator<Work> iterator() {
    return queue.iterator();
  }

}
