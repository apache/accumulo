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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionFinalState.FinalState;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionFinalizer {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionFinalizer.class);

  protected final ServerContext context;
  private final ExecutorService ntfyExecutor;
  private final ExecutorService backgroundExecutor;
  private final BlockingQueue<ExternalCompactionFinalState> pendingNotifications;
  private final long tserverCheckInterval;

  protected CompactionFinalizer(ServerContext context, ScheduledThreadPoolExecutor schedExecutor) {
    this.context = context;
    this.pendingNotifications = new ArrayBlockingQueue<>(1000);

    tserverCheckInterval = this.context.getConfiguration()
        .getTimeInMillis(Property.COMPACTION_COORDINATOR_FINALIZER_COMPLETION_CHECK_INTERVAL);
    int max = this.context.getConfiguration()
        .getCount(Property.COMPACTION_COORDINATOR_FINALIZER_TSERVER_NOTIFIER_MAXTHREADS);

    this.ntfyExecutor = ThreadPools.getServerThreadPools().createThreadPool(3, max, 1,
        TimeUnit.MINUTES, "Compaction Finalizer Notifier", true);

    this.backgroundExecutor = ThreadPools.getServerThreadPools().createFixedThreadPool(1,
        "Compaction Finalizer Background Task", true);

    backgroundExecutor.execute(() -> {
      processPending();
    });

    ThreadPools.watchCriticalScheduledTask(schedExecutor.scheduleWithFixedDelay(
        this::notifyTservers, 0, tserverCheckInterval, TimeUnit.MILLISECONDS));
  }

  public void commitCompaction(ExternalCompactionId ecid, KeyExtent extent, long fileSize,
      long fileEntries) {

    var ecfs =
        new ExternalCompactionFinalState(ecid, extent, FinalState.FINISHED, fileSize, fileEntries);

    LOG.debug("Initiating commit for external compaction: {}", ecfs);

    // write metadata entry
    context.getAmple().putExternalCompactionFinalStates(List.of(ecfs));

    if (!pendingNotifications.offer(ecfs)) {
      LOG.debug("Queue full, notification to tablet server will happen later {}.", ecfs);
    } else {
      LOG.debug("Queued tserver notification for completed external compaction: {}", ecfs);
    }
  }

  public void failCompactions(Map<ExternalCompactionId,KeyExtent> compactionsToFail) {

    var finalStates = compactionsToFail.entrySet().stream().map(
        e -> new ExternalCompactionFinalState(e.getKey(), e.getValue(), FinalState.FAILED, 0L, 0L))
        .collect(Collectors.toList());

    context.getAmple().putExternalCompactionFinalStates(finalStates);

    finalStates.forEach(pendingNotifications::offer);
  }

  private void notifyTserver(Location loc, ExternalCompactionFinalState ecfs) {

    TabletClientService.Client client = null;
    try {
      client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, loc.getHostAndPort(), context);
      if (ecfs.getFinalState() == FinalState.FINISHED) {
        LOG.debug("Notifying tserver {} that compaction {} has finished.", loc, ecfs);
        client.compactionJobFinished(TraceUtil.traceInfo(), context.rpcCreds(),
            ecfs.getExternalCompactionId().canonical(), ecfs.getExtent().toThrift(),
            ecfs.getFileSize(), ecfs.getEntries());
      } else if (ecfs.getFinalState() == FinalState.FAILED) {
        LOG.debug("Notifying tserver {} that compaction {} has failed.", loc, ecfs);
        client.compactionJobFailed(TraceUtil.traceInfo(), context.rpcCreds(),
            ecfs.getExternalCompactionId().canonical(), ecfs.getExtent().toThrift());
      } else {
        throw new IllegalArgumentException(ecfs.getFinalState().name());
      }
    } catch (TException e) {
      LOG.warn("Failed to notify tserver {}", loc.getHostAndPort(), e);
    } finally {
      ThriftUtil.returnClient(client, context);
    }
  }

  private void processPending() {

    while (!Thread.interrupted()) {
      try {
        ArrayList<ExternalCompactionFinalState> batch = new ArrayList<>();
        batch.add(pendingNotifications.take());
        pendingNotifications.drainTo(batch);

        List<Future<?>> futures = new ArrayList<>();

        List<ExternalCompactionId> statusesToDelete = new ArrayList<>();

        Map<KeyExtent,TabletMetadata> tabletsMetadata;
        var extents = batch.stream().map(ExternalCompactionFinalState::getExtent).collect(toList());
        try (TabletsMetadata tablets = context.getAmple().readTablets().forTablets(extents)
            .fetch(ColumnType.LOCATION, ColumnType.PREV_ROW, ColumnType.ECOMP).build()) {
          tabletsMetadata = tablets.stream().collect(toMap(TabletMetadata::getExtent, identity()));
        }

        for (ExternalCompactionFinalState ecfs : batch) {

          TabletMetadata tabletMetadata = tabletsMetadata.get(ecfs.getExtent());

          if (tabletMetadata == null || !tabletMetadata.getExternalCompactions().keySet()
              .contains(ecfs.getExternalCompactionId())) {
            // there is not per tablet external compaction entry, so delete its final state marker
            // from metadata table
            LOG.debug(
                "Unable to find tablets external compaction entry, deleting completion entry {}",
                ecfs);
            statusesToDelete.add(ecfs.getExternalCompactionId());
          } else if (tabletMetadata.getLocation() != null
              && tabletMetadata.getLocation().getType() == LocationType.CURRENT) {
            futures
                .add(ntfyExecutor.submit(() -> notifyTserver(tabletMetadata.getLocation(), ecfs)));
          } else {
            LOG.trace(
                "External compaction {} is completed, but there is no location for tablet.  Unable to notify tablet, will try again later.",
                ecfs);
          }
        }

        if (!statusesToDelete.isEmpty()) {
          LOG.info(
              "Deleting unresolvable completed external compactions from metadata table, ids: {}",
              statusesToDelete);
          context.getAmple().deleteExternalCompactionFinalStates(statusesToDelete);
        }

        for (Future<?> future : futures) {
          try {
            future.get();
          } catch (ExecutionException e) {
            LOG.debug("Failed to notify tserver", e);
          }
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (RuntimeException e) {
        LOG.warn("Failed to process pending notifications", e);
      }
    }
  }

  private void notifyTservers() {
    try {
      Iterator<ExternalCompactionFinalState> finalStates =
          context.getAmple().getExternalCompactionFinalStates().iterator();
      while (finalStates.hasNext()) {
        ExternalCompactionFinalState state = finalStates.next();
        LOG.debug("Found external compaction in final state: {}, queueing for tserver notification",
            state);
        pendingNotifications.put(state);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (RuntimeException e) {
      LOG.warn("Failed to notify tservers", e);
    }
  }
}
