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
package org.apache.accumulo.server.metadata;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;

import com.google.common.annotations.VisibleForTesting;

public class AsyncConditionalTabletsMutatorImpl implements Ample.AsyncConditionalTabletsMutator {
  private final Consumer<Ample.ConditionalResult> resultsConsumer;
  private final ExecutorService executor;
  private Future<Map<KeyExtent,Ample.ConditionalResult>> backgroundProcessing = null;
  private ConditionalTabletsMutatorImpl bufferingMutator;
  private final ServerContext context;
  private long mutatedTablets = 0;
  public static final int BATCH_SIZE = 1000;
  private final Function<DataLevel,String> tableMapper;

  @VisibleForTesting
  public AsyncConditionalTabletsMutatorImpl(ServerContext context,
      Function<DataLevel,String> tableMapper, Consumer<Ample.ConditionalResult> resultsConsumer) {
    this.resultsConsumer = Objects.requireNonNull(resultsConsumer);
    this.context = context;
    this.bufferingMutator = new ConditionalTabletsMutatorImpl(context, tableMapper);
    var creatorId = Thread.currentThread().getId();
    this.executor = Executors.newSingleThreadExecutor(runnable -> Threads.createThread(
        "Async conditional tablets mutator background thread, created by : #" + creatorId,
        runnable));
    this.tableMapper = Objects.requireNonNull(tableMapper);

  }

  @Override
  public Ample.OperationRequirements mutateTablet(KeyExtent extent) {
    if (mutatedTablets > BATCH_SIZE) {
      if (backgroundProcessing != null) {
        // a previous batch of mutations was submitted for processing so wait on it.
        try {
          backgroundProcessing.get().values().forEach(resultsConsumer);
        } catch (InterruptedException | ExecutionException e) {
          throw new IllegalStateException(e);
        }
      }

      // Spin up processing of the mutations submitted so far in a background thread. Must copy the
      // reference for the background thread because a new one is about to be created.
      var bufferingMutatorRef = bufferingMutator;
      backgroundProcessing = executor.submit(() -> {
        var result = bufferingMutatorRef.process();
        bufferingMutatorRef.close();
        return result;
      });

      bufferingMutator = new ConditionalTabletsMutatorImpl(context, tableMapper);
      mutatedTablets = 0;
    }
    mutatedTablets++;
    return bufferingMutator.mutateTablet(extent);
  }

  @Override
  public void close() {
    if (backgroundProcessing != null) {
      // a previous batch of mutations was submitted for processing so wait on it.
      try {
        backgroundProcessing.get().values().forEach(resultsConsumer);
      } catch (InterruptedException | ExecutionException e) {
        throw new IllegalStateException(e);
      }
    }
    // process anything not processed so far
    bufferingMutator.process().values().forEach(resultsConsumer);
    bufferingMutator.close();
    executor.shutdownNow();
  }

}
