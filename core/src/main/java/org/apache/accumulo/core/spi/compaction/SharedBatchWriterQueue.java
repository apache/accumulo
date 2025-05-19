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
package org.apache.accumulo.core.spi.compaction;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import org.apache.accumulo.core.data.Mutation;

/**
 * Queue of mutations used by the SharedBatchWriter
 */
public interface SharedBatchWriterQueue {

  class Work {
    private final String id;
    private final Mutation mutation;
    private final CompletableFuture<Void> future;

    public Work(String id, Mutation mutation) {
      this.id = id;
      this.mutation = mutation;
      this.future = new CompletableFuture<>();
    }

    public Mutation getMutation() {
      return mutation;
    }

    public String getId() {
      return id;
    }

    public CompletableFuture<Void> getFuture() {
      return future;
    }

  }

  /**
   * Initialization parameters
   *
   * @since 2.1.4
   */
  interface InitParameters {
    int getQueueSize();
  }

  /**
   * Initialize the SharedBatchWriterQueue
   *
   * @param params initialization parameters
   * @since 2.1.4
   */
  void init(InitParameters params);

  /**
   * Add the {@code Work} item to the queue
   *
   * @param work work to be added to the queue
   * @throws InterruptedException interrupted while waiting
   * @since 2.1.4
   */
  void add(Work work) throws InterruptedException;

  /**
   * Remove a {@code Work} item from the queue
   *
   * @return work item
   * @throws InterruptedException interrupted while waiting for item
   * @since 2.1.4
   */
  Work remove() throws InterruptedException;

  /**
   * Remove all {@code Work} items from the queue and into the supplied Collection
   *
   * @param work destination for the removed items
   * @throws InterruptedException interrupted while filling destination
   * @since 2.1.4
   */
  void removeAll(Collection<Work> work) throws InterruptedException;

  /**
   * Return an Iterator over the {@code Work} elements in the queue
   *
   * @return iterator
   * @since 2.1.4
   */
  Iterator<Work> iterator();

}
