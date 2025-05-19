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
package org.apache.accumulo.coordinator;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.spi.compaction.SharedBatchWriterQueue;
import org.apache.accumulo.core.spi.compaction.SharedBatchWriterQueue.Work;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This class supports the use case of many threads writing mutations to a table. Instead of each
 * thread creating their own batch writer, each thread can add mutations to the queue that this
 * shared batch writer reads from. This is more efficient than creating a batch writer to add a
 * single mutation to a table as the batch writer would make 3 RPCs to write the single mutation.
 * Using this class results in much less thread creation and RPCs.
 */
public class SharedBatchWriter {
  private static final Logger log = LoggerFactory.getLogger(SharedBatchWriter.class);
  private final Character prefix;

  private final SharedBatchWriterQueue queue;
  private final String table;
  private final ServerContext context;

  public SharedBatchWriter(String table, Character prefix, ServerContext context, SharedBatchWriterQueue queue) {
    Objects.requireNonNull(table, "Missing table");
    Objects.requireNonNull(context, "Missing context");
    Objects.requireNonNull(queue, "Missing queue");
    this.table = table;
    this.prefix = prefix;
    this.context = context;
    this.queue = queue;
    var thread = Threads.createCriticalThread(
        "shared batch writer for " + table + " prefix:" + prefix, this::processMutations);
    thread.start();
  }

  private void processMutations() {
    Timer timer = Timer.startNew();
    while (true) {
      ArrayList<Work> batch = new ArrayList<>();
      try {
        batch.add(queue.remove());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }

      try (var writer = context.createBatchWriter(table)) {
        try {
          queue.removeAll(batch);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
        timer.restart();
        for (var work : batch) {
          writer.addMutation(work.getMutation());
        }
        writer.flush();
        log.trace("Wrote {} mutations in {}ms for prefix {}", batch.size(),
            timer.elapsed(TimeUnit.MILLISECONDS), prefix);
        batch.forEach(work -> work.getFuture().complete(null));
      } catch (TableNotFoundException | MutationsRejectedException e) {
        log.debug("Failed to process {} mutations in {}ms for prefix {}", batch.size(),
            timer.elapsed(TimeUnit.MILLISECONDS), prefix, e);
        batch.forEach(work -> work.getFuture().completeExceptionally(e));
      }
    }
  }
}
