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
package org.apache.accumulo.manager.split;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.split.FindSplits;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Splitter {

  private static final Logger LOG = LoggerFactory.getLogger(Splitter.class);

  private final Manager manager;
  private final ThreadPoolExecutor splitExecutor;
  // tracks which tablets are queued in splitExecutor
  private final Map<Text,KeyExtent> queuedTablets = new ConcurrentHashMap<>();

  class SplitWorker implements Runnable {

    @Override
    public void run() {
      try {
        while (manager.stillManager()) {
          if (queuedTablets.isEmpty()) {
            sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            continue;
          }

          final Map<Text,KeyExtent> userSplits = new HashMap<>();
          final Map<Text,KeyExtent> metaSplits = new HashMap<>();

          // Go through all the queued up splits and partition
          // into the different store types to be submitted.
          queuedTablets.forEach((metaRow, extent) -> {
            switch (FateInstanceType.fromTableId((extent.tableId()))) {
              case USER:
                userSplits.put(metaRow, extent);
                break;
              case META:
                metaSplits.put(metaRow, extent);
                break;
              default:
                throw new IllegalStateException("Unexpected FateInstanceType");
            }
          });

          // see the user and then meta splits
          // The meta plits (zk) will be processed one at a time but there will not be
          // many of those splits. The user splits are processed as a batch.
          seedSplits(FateInstanceType.USER, userSplits);
          seedSplits(FateInstanceType.META, metaSplits);
        }
      } catch (Exception e) {
        LOG.error("Failed to split", e);
      }
    }
  }

  private void seedSplits(FateInstanceType instanceType, Map<Text,KeyExtent> splits) {
    if (!splits.isEmpty()) {
      try (var seeder = manager.fateClient(instanceType).beginSeeding()) {
        for (KeyExtent extent : splits.values()) {
          @SuppressWarnings("unused")
          var unused = seeder.attemptToSeedTransaction(Fate.FateOperation.SYSTEM_SPLIT,
              FateKey.forSplit(extent), new FindSplits(extent), true);
        }
      } finally {
        queuedTablets.keySet().removeAll(splits.keySet());
      }
    }
  }

  public Splitter(Manager manager) {
    this.manager = manager;
    ServerContext context = manager.getContext();

    this.splitExecutor = context.threadPools().getPoolBuilder("split_seeder").numCoreThreads(1)
        .numMaxThreads(1).withTimeOut(0L, TimeUnit.MILLISECONDS).enableThreadPoolMetrics().build();

  }

  public synchronized void start() {
    splitExecutor.execute(new SplitWorker());
  }

  public synchronized void stop() {
    splitExecutor.shutdownNow();
  }

  public void initiateSplit(KeyExtent extent) {
    // Want to avoid queuing the same tablet multiple times, it would not cause bugs but would waste
    // work. Use the metadata row to identify a tablet because the KeyExtent also includes the prev
    // end row which may change when splits happen. The metaRow is conceptually tableId+endRow and
    // that does not change for a split.
    Text metaRow = extent.toMetaRow();
    int qsize = queuedTablets.size();
    if (qsize >= 10_000 || queuedTablets.putIfAbsent(metaRow, extent) != null) {
      LOG.trace("Did not add {} to split queue {}", metaRow, qsize);
    }
  }
}
