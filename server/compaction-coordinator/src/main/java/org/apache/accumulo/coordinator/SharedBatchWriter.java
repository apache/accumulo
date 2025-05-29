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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This class supports the use case of many threads writing a single mutation to a table. It avoids
 * each thread creating its own batch writer which creates threads and makes 3 RPCs to write the
 * single mutation. Using this class results in much less thread creation and RPCs.
 */
public class SharedBatchWriter {
  private static final Logger log = LoggerFactory.getLogger(SharedBatchWriter.class);
  private final Character prefix;

  private static class Work {
    private final Mutation mutation;
    private final CompletableFuture<Void> future;

    private Work(Mutation mutation) {
      this.mutation = mutation;
      this.future = new CompletableFuture<>();
    }
  }

  private final BlockingQueue<Work> mutations;
  private final String table;
  private final ServerContext context;

  public SharedBatchWriter(String table, Character prefix, ServerContext context, int queueSize) {
    Preconditions.checkArgument(queueSize > 0, "illegal queue size %s", queueSize);
    this.table = table;
    this.prefix = prefix;
    this.context = context;
    this.mutations = new ArrayBlockingQueue<>(queueSize);
    var thread = Threads.createCriticalThread(
        "shared batch writer for " + table + " prefix:" + prefix, this::processMutations);
    thread.start();
  }

  public void write(Mutation m) {
    try {
      var work = new Work(m);
      mutations.put(work);
      work.future.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private void processMutations() {
    Timer timer = Timer.startNew();
    while (true) {
      ArrayList<Work> batch = new ArrayList<>();
      try {
        batch.add(mutations.take());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }

      try (var writer = context.createBatchWriter(table)) {
        mutations.drainTo(batch);
        timer.restart();
        for (var work : batch) {
          writer.addMutation(work.mutation);
        }
        writer.flush();
        log.trace("Wrote {} mutations in {}ms for prefix {}", batch.size(),
            timer.elapsed(TimeUnit.MILLISECONDS), prefix);
        batch.forEach(work -> work.future.complete(null));
      } catch (TableNotFoundException | MutationsRejectedException e) {
        log.debug("Failed to process {} mutations in {}ms for prefix {}", batch.size(),
            timer.elapsed(TimeUnit.MILLISECONDS), prefix, e);
        batch.forEach(work -> work.future.completeExceptionally(e));
      }
    }
  }
}
