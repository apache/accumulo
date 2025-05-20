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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.COORDINATOR_FINALIZER_BACKGROUND_POOL;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.COORDINATOR_FINALIZER_NOTIFIER_POOL;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.Ample;
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
import org.apache.accumulo.core.util.Timer;
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
  private final ConcurrentHashMap<Character,SharedBatchWriter> writers =
      new ConcurrentHashMap<>(16);
  private final int queueSize;

  protected CompactionFinalizer(ServerContext context, ScheduledThreadPoolExecutor schedExecutor) {
    this.context = context;
    queueSize =
        context.getConfiguration().getCount(Property.COMPACTION_COORDINATOR_FINALIZER_QUEUE_SIZE);

    this.pendingNotifications = new ArrayBlockingQueue<>(queueSize);

    tserverCheckInterval = this.context.getConfiguration()
        .getTimeInMillis(Property.COMPACTION_COORDINATOR_FINALIZER_COMPLETION_CHECK_INTERVAL);
    int max = this.context.getConfiguration()
        .getCount(Property.COMPACTION_COORDINATOR_FINALIZER_TSERVER_NOTIFIER_MAXTHREADS);

    this.ntfyExecutor = ThreadPools.getServerThreadPools()
        .getPoolBuilder(COORDINATOR_FINALIZER_NOTIFIER_POOL).numCoreThreads(3).numMaxThreads(max)
        .withTimeOut(1L, MINUTES).enableThreadPoolMetrics().build();

    this.backgroundExecutor =
        ThreadPools.getServerThreadPools().getPoolBuilder(COORDINATOR_FINALIZER_BACKGROUND_POOL)
            .numCoreThreads(1).enableThreadPoolMetrics().build();

    backgroundExecutor.execute(() -> {
      processPending();
    });

    ThreadPools.watchCriticalScheduledTask(schedExecutor.scheduleWithFixedDelay(
        this::notifyTservers, 0, tserverCheckInterval, TimeUnit.MILLISECONDS));

  }

  private SharedBatchWriter getWriter(ExternalCompactionId ecid) {
    return writers.computeIfAbsent(ecid.getFirstUUIDChar(),
        (prefix) -> new SharedBatchWriter(Ample.DataLevel.USER.metaTable(), prefix, context,
            queueSize / 16));
  }

  public void commitCompaction(ExternalCompactionId ecid, KeyExtent extent, long fileSize,
      long fileEntries) {

    var ecfs =
        new ExternalCompactionFinalState(ecid, extent, FinalState.FINISHED, fileSize, fileEntries);

    SharedBatchWriter writer = getWriter(ecid);

    LOG.trace("Initiating commit for external compaction: {} {}", ecid, ecfs);

    // write metadata entry
    Timer timer = Timer.startNew();
    writer.write(ecfs.toMutation());
    LOG.trace("{} metadata compation state write completed in {}ms", ecid,
        timer.elapsed(TimeUnit.MILLISECONDS));

    if (!pendingNotifications.offer(ecfs)) {
      LOG.trace("Queue full, notification to tablet server will happen later {}.", ecid);
    } else {
      LOG.trace("Queued tserver notification for completed external compaction: {}", ecid);
    }
  }

  public void failCompactions(Map<ExternalCompactionId,KeyExtent> compactionsToFail) {
    if (compactionsToFail.size() == 1) {
      var e = compactionsToFail.entrySet().iterator().next();
      var ecfs =
          new ExternalCompactionFinalState(e.getKey(), e.getValue(), FinalState.FAILED, 0L, 0L);
      getWriter(e.getKey()).write(ecfs.toMutation());
    } else {
      try (BatchWriter writer = context.createBatchWriter(Ample.DataLevel.USER.metaTable())) {
        for (var e : compactionsToFail.entrySet()) {
          var ecfs =
              new ExternalCompactionFinalState(e.getKey(), e.getValue(), FinalState.FAILED, 0L, 0L);
          writer.addMutation(ecfs.toMutation());
        }
      } catch (MutationsRejectedException | TableNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    for (var e : compactionsToFail.entrySet()) {
      var ecfs =
          new ExternalCompactionFinalState(e.getKey(), e.getValue(), FinalState.FAILED, 0L, 0L);
      if (!pendingNotifications.offer(ecfs)) {
        break;
      }
    }
  }

  private void notifyTserver(Location loc, ExternalCompactionFinalState ecfs) {

    TabletClientService.Client client = null;
    Timer timer = Timer.startNew();
    try {
      client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, loc.getHostAndPort(), context);
      if (ecfs.getFinalState() == FinalState.FINISHED) {
        LOG.trace("Notifying tserver {} that compaction {} has finished.", loc, ecfs);
        client.compactionJobFinished(TraceUtil.traceInfo(), context.rpcCreds(),
            ecfs.getExternalCompactionId().canonical(), ecfs.getExtent().toThrift(),
            ecfs.getFileSize(), ecfs.getEntries());
      } else if (ecfs.getFinalState() == FinalState.FAILED) {
        LOG.trace("Notifying tserver {} that compaction {} with {} has failed.", loc,
            ecfs.getExternalCompactionId(), ecfs);
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
    LOG.trace("Tserver {} notification of {} {} took {}ms", loc, ecfs.getExternalCompactionId(),
        ecfs, timer.elapsed(TimeUnit.MILLISECONDS));
  }

  private void processPending() {

    while (!Thread.interrupted()) {
      try {
        ArrayList<ExternalCompactionFinalState> batch = new ArrayList<>();
        batch.add(pendingNotifications.take());
        pendingNotifications.drainTo(batch);

        LOG.trace("Processing pending of batch size {}", batch.size());

        List<Future<?>> futures = new ArrayList<>();

        List<ExternalCompactionId> statusesToDelete = new ArrayList<>();

        Map<KeyExtent,TabletMetadata> tabletsMetadata;
        var extents = batch.stream().map(ExternalCompactionFinalState::getExtent).collect(toList());
        Timer timer = Timer.startNew();
        try (TabletsMetadata tablets = context.getAmple().readTablets().forTablets(extents)
            .fetch(ColumnType.LOCATION, ColumnType.PREV_ROW, ColumnType.ECOMP).build()) {
          tabletsMetadata = tablets.stream().collect(toMap(TabletMetadata::getExtent, identity()));
        }
        LOG.trace("Metadata scan completed in {}ms for batch size {}, found {}",
            timer.elapsed(TimeUnit.MILLISECONDS), batch.size(), tabletsMetadata.size());

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
          timer.restart();
          context.getAmple().deleteExternalCompactionFinalStates(statusesToDelete);
          LOG.info(
              "Deleted unresolvable completed external compactions from metadata table, ids: {} in {}ms",
              statusesToDelete.size(), timer.elapsed(TimeUnit.MILLISECONDS));
          for (var ecid : statusesToDelete) {
            LOG.debug("Deleted unresolvable completed external compaction {}", ecid);
          }
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
    Timer timer = Timer.startNew();
    try (var finalStatesStream = context.getAmple().getExternalCompactionFinalStates()) {
      int count = 0;
      Iterator<ExternalCompactionFinalState> finalStates = finalStatesStream.iterator();
      while (finalStates.hasNext()) {
        ExternalCompactionFinalState state = finalStates.next();
        count++;
        LOG.trace("Found external compaction in final state: {}, queueing for tserver notification",
            state);
        pendingNotifications.put(state);
      }
      LOG.trace("Added {} final compaction states to notification queue in {}ms", count,
          timer.elapsed(TimeUnit.MILLISECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (RuntimeException e) {
      LOG.warn("Failed to notify tservers", e);
    }
  }
}
