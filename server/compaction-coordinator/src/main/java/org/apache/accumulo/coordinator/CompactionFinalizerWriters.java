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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.spi.util.SharedBatchWriterQueue;
import org.apache.accumulo.core.spi.util.SharedBatchWriterQueue.InitParameters;
import org.apache.accumulo.core.spi.util.SharedBatchWriterQueue.Work;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.SharedBatchWriter;
import org.apache.accumulo.core.util.SharedBatchWriterBlockingQueue;
import org.apache.accumulo.server.ServerContext;

/**
 * Object that holds 16 SharedBatchWriters for writing ExternalCompactionFinalStates to the metadata
 * table for the CompactionFinalizer.
 */
public class CompactionFinalizerWriters {

  private final static int NUM_WRITERS = 16;

  private final ConcurrentHashMap<Character,
      Pair<SharedBatchWriter,SharedBatchWriterQueue>> writers =
          new ConcurrentHashMap<>(NUM_WRITERS);

  private final ServerContext context;
  private final int totalQueueSize;

  CompactionFinalizerWriters(ServerContext ctx) {
    Objects.requireNonNull(ctx, "ServerContext missing");
    context = ctx;
    totalQueueSize =
        context.getConfiguration().getCount(Property.COMPACTION_COORDINATOR_FINALIZER_QUEUE_SIZE);

    int perWriterQueueSize = totalQueueSize / NUM_WRITERS;

    for (char c : new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D',
        'E', 'F'}) {

      final SharedBatchWriterQueue queue = Property.createInstanceFromPropertyName(
          context.getConfiguration(), Property.COMPACTION_COORDINATOR_FINALIZER_WRITER_QUEUE,
          SharedBatchWriterQueue.class, new SharedBatchWriterBlockingQueue());
      queue.init(new InitParameters() {
        @Override
        public int getQueueSize() {
          return perWriterQueueSize;
        }

      });
      writers.computeIfAbsent(c,
          (i) -> new Pair<SharedBatchWriter,SharedBatchWriterQueue>(
              new SharedBatchWriter(Ample.DataLevel.USER.metaTable(), c, context, queue), queue))
          .getFirst();
    }
  }

  public int getQueueSize() {
    return totalQueueSize;
  }

  public SharedBatchWriterQueue getQueue(ExternalCompactionId ecid) {
    return writers.get(ecid.getFirstUUIDChar()).getSecond();
  }

  public Set<ExternalCompactionId> getQueuedIds() {
    final Set<ExternalCompactionId> ids = new HashSet<>(totalQueueSize);
    writers.forEach((k, v) -> {
      Iterator<Work> iter = v.getSecond().iterator();
      while (iter.hasNext()) {
        ids.add(ExternalCompactionId.of(iter.next().getId()));
      }
    });
    return ids;
  }

  public void write(ExternalCompactionFinalState ecfs) {
    try {
      final ExternalCompactionId ecid = ecfs.getExternalCompactionId();
      var work = new Work(ecid.canonical(), ecfs.toMutation());
      getQueue(ecid).add(work);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

}
