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
package org.apache.accumulo.tserver.compactions;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.accumulo.core.util.compaction.CompactionServicesConfig.DEFAULT_SERVICE;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactionServices;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.apache.accumulo.core.util.compaction.CompactionServicesConfig;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.tserver.compactions.CompactionExecutor.CType;
import org.apache.accumulo.tserver.metrics.CompactionExecutorsMetrics;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class CompactionManager {

  private static final Logger log = LoggerFactory.getLogger(CompactionManager.class);

  private Iterable<Compactable> compactables;
  private volatile Map<CompactionServiceId,CompactionService> services;

  private LinkedBlockingQueue<Compactable> compactablesToCheck = new LinkedBlockingQueue<>();

  private long maxTimeBetweenChecks;

  private ServerContext context;

  private CompactionServicesConfig currentCfg;

  private long lastConfigCheckTime = System.nanoTime();

  private CompactionExecutorsMetrics ceMetrics;

  private String lastDeprecationWarning = "";

  private Map<CompactionExecutorId,ExternalCompactionExecutor> externalExecutors;

  private Map<ExternalCompactionId,ExtCompInfo> runningExternalCompactions;

  static class ExtCompInfo {
    final KeyExtent extent;
    final CompactionExecutorId executor;

    public ExtCompInfo(KeyExtent extent, CompactionExecutorId executor) {
      this.extent = extent;
      this.executor = executor;
    }
  }

  private void warnAboutDeprecation(String warning) {
    if (!warning.equals(lastDeprecationWarning)) {
      log.warn(warning);
      lastDeprecationWarning = warning;
    }
  }

  private void mainLoop() {
    long lastCheckAllTime = System.nanoTime();

    long increment = Math.max(1, maxTimeBetweenChecks / 10);

    var retryFactory = Retry.builder().infiniteRetries().retryAfter(increment, MILLISECONDS)
        .incrementBy(increment, MILLISECONDS).maxWait(maxTimeBetweenChecks, MILLISECONDS)
        .backOffFactor(1.07).logInterval(1, MINUTES).createFactory();
    var retry = retryFactory.createRetry();
    Compactable last = null;

    while (true) {
      try {
        long passed = NANOSECONDS.toMillis(System.nanoTime() - lastCheckAllTime);
        if (passed >= maxTimeBetweenChecks) {
          // take a snapshot of what is currently running
          HashSet<ExternalCompactionId> runningEcids =
              new HashSet<>(runningExternalCompactions.keySet());
          for (Compactable compactable : compactables) {
            last = compactable;
            submitCompaction(compactable);
            // remove anything from snapshot that tablets know are running
            compactable.getExternalCompactionIds(runningEcids::remove);
          }
          lastCheckAllTime = System.nanoTime();
          // anything left in the snapshot is unknown to any tablet and should be removed if it
          // still exists
          runningExternalCompactions.keySet().removeAll(runningEcids);
        } else {
          var compactable = compactablesToCheck.poll(maxTimeBetweenChecks - passed, MILLISECONDS);
          if (compactable != null) {
            last = compactable;
            submitCompaction(compactable);
          }
        }

        last = null;
        if (retry.hasRetried()) {
          retry = retryFactory.createRetry();
        }

        checkForConfigChanges(false);

      } catch (Exception e) {
        var extent = last == null ? null : last.getExtent();
        log.warn("Failed to compact {} ", extent, e);
        retry.useRetry();
        try {
          retry.waitForNextAttempt(log, "compaction initiation loop");
        } catch (InterruptedException e1) {
          log.debug("Retry interrupted", e1);
        }
      }
    }
  }

  /**
   * Get each configured service for the compactable tablet and submit for compaction
   */
  private void submitCompaction(Compactable compactable) {
    for (CompactionKind ctype : CompactionKind.values()) {
      var csid = compactable.getConfiguredService(ctype);
      var service = services.get(csid);
      if (service == null) {
        checkForConfigChanges(true);
        service = services.get(csid);
        if (service == null) {
          log.error(
              "Tablet {} returned non-existent compaction service {} for compaction type {}.  Check"
                  + " the table compaction dispatcher configuration. Attempting to fall back to "
                  + "{} service.",
              compactable.getExtent(), csid, ctype, DEFAULT_SERVICE);
          service = services.get(DEFAULT_SERVICE);
        }
      }

      if (service != null) {
        service.submitCompaction(ctype, compactable, compactablesToCheck::add);
      }
    }
  }

  public CompactionManager(Iterable<Compactable> compactables, ServerContext context,
      CompactionExecutorsMetrics ceMetrics) {
    this.compactables = compactables;

    this.currentCfg =
        new CompactionServicesConfig(context.getConfiguration(), this::warnAboutDeprecation);

    this.context = context;

    this.ceMetrics = ceMetrics;

    this.externalExecutors = new ConcurrentHashMap<>();

    this.runningExternalCompactions = new ConcurrentHashMap<>();

    Map<CompactionServiceId,CompactionService> tmpServices = new HashMap<>();

    currentCfg.getPlanners().forEach((serviceName, plannerClassName) -> {
      try {
        tmpServices.put(CompactionServiceId.of(serviceName),
            new CompactionService(serviceName, plannerClassName,
                currentCfg.getRateLimit(serviceName),
                currentCfg.getOptions().getOrDefault(serviceName, Map.of()), context, ceMetrics,
                this::getExternalExecutor));
      } catch (RuntimeException e) {
        log.error("Failed to create compaction service {} with planner:{} options:{}", serviceName,
            plannerClassName, currentCfg.getOptions().getOrDefault(serviceName, Map.of()), e);
      }
    });

    this.services = Map.copyOf(tmpServices);

    this.maxTimeBetweenChecks =
        context.getConfiguration().getTimeInMillis(Property.TSERV_MAJC_DELAY);

    ceMetrics.setExternalMetricsSupplier(this::getExternalMetrics);
  }

  public void compactableChanged(Compactable compactable) {
    compactablesToCheck.add(compactable);
  }

  private synchronized void checkForConfigChanges(boolean force) {
    try {
      final long secondsSinceLastCheck =
          NANOSECONDS.toSeconds(System.nanoTime() - lastConfigCheckTime);
      if (!force && (secondsSinceLastCheck < 1)) {
        return;
      }

      lastConfigCheckTime = System.nanoTime();

      var tmpCfg =
          new CompactionServicesConfig(context.getConfiguration(), this::warnAboutDeprecation);

      if (!currentCfg.equals(tmpCfg)) {
        Map<CompactionServiceId,CompactionService> tmpServices = new HashMap<>();

        tmpCfg.getPlanners().forEach((serviceName, plannerClassName) -> {

          try {
            var csid = CompactionServiceId.of(serviceName);
            var service = services.get(csid);
            if (service == null) {
              tmpServices.put(csid,
                  new CompactionService(serviceName, plannerClassName,
                      tmpCfg.getRateLimit(serviceName),
                      tmpCfg.getOptions().getOrDefault(serviceName, Map.of()), context, ceMetrics,
                      this::getExternalExecutor));
            } else {
              service.configurationChanged(plannerClassName, tmpCfg.getRateLimit(serviceName),
                  tmpCfg.getOptions().getOrDefault(serviceName, Map.of()));
              tmpServices.put(csid, service);
            }
          } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create or update compaction service "
                + serviceName + " with planner:" + plannerClassName + " options:"
                + tmpCfg.getOptions().getOrDefault(serviceName, Map.of()), e);
          }
        });

        var deletedServices =
            Sets.difference(currentCfg.getPlanners().keySet(), tmpCfg.getPlanners().keySet());

        for (String serviceName : deletedServices) {
          services.get(CompactionServiceId.of(serviceName)).stop();
        }

        this.services = Map.copyOf(tmpServices);

        HashSet<CompactionExecutorId> activeExternalExecs = new HashSet<>();
        services.values().forEach(cs -> cs.getExternalExecutorsInUse(activeExternalExecs::add));
        // clean up an external compactors that are no longer in use by any compaction service
        externalExecutors.keySet().retainAll(activeExternalExecs);

      }
    } catch (RuntimeException e) {
      log.error("Failed to reconfigure compaction services ", e);
    }
  }

  public void start() {
    log.debug("Started compaction manager");
    Threads.createThread("Compaction Manager", () -> mainLoop()).start();
  }

  public CompactionServices getServices() {
    var serviceIds = services.keySet();

    return new CompactionServices() {
      @Override
      public Set<CompactionServiceId> getIds() {
        return serviceIds;
      }
    };
  }

  public boolean isCompactionQueued(KeyExtent extent, Set<CompactionServiceId> servicesUsed) {
    return servicesUsed.stream().map(services::get).filter(Objects::nonNull)
        .anyMatch(compactionService -> compactionService.isCompactionQueued(extent));
  }

  public int getCompactionsRunning() {
    return services.values().stream().mapToInt(cs -> cs.getCompactionsRunning(CType.INTERNAL)).sum()
        + runningExternalCompactions.size();
  }

  public int getCompactionsQueued() {
    return services.values().stream().mapToInt(cs -> cs.getCompactionsQueued(CType.INTERNAL)).sum()
        + externalExecutors.values().stream()
            .mapToInt(ee -> ee.getCompactionsQueued(CType.EXTERNAL)).sum();
  }

  public ExternalCompactionJob reserveExternalCompaction(String queueName, long priority,
      String compactorId, ExternalCompactionId externalCompactionId) {
    log.debug("Attempting to reserve external compaction, queue:{} priority:{} compactor:{}",
        queueName, priority, compactorId);

    ExternalCompactionExecutor extCE = getExternalExecutor(queueName);
    var ecJob = extCE.reserveExternalCompaction(priority, compactorId, externalCompactionId);
    if (ecJob != null) {
      runningExternalCompactions.put(ecJob.getExternalCompactionId(),
          new ExtCompInfo(ecJob.getExtent(), extCE.getId()));
      log.debug("Reserved external compaction {}", ecJob.getExternalCompactionId());
    }
    return ecJob;
  }

  ExternalCompactionExecutor getExternalExecutor(CompactionExecutorId ceid) {
    return externalExecutors.computeIfAbsent(ceid, id -> new ExternalCompactionExecutor(id));
  }

  ExternalCompactionExecutor getExternalExecutor(String queueName) {
    return getExternalExecutor(CompactionExecutorIdImpl.externalId(queueName));
  }

  public void registerExternalCompaction(ExternalCompactionId ecid, KeyExtent extent,
      CompactionExecutorId ceid) {
    runningExternalCompactions.put(ecid, new ExtCompInfo(extent, ceid));
  }

  public void commitExternalCompaction(ExternalCompactionId extCompactionId,
      KeyExtent extentCompacted, Map<KeyExtent,Tablet> currentTablets, long fileSize,
      long entries) {
    var ecInfo = runningExternalCompactions.get(extCompactionId);
    if (ecInfo != null) {
      Preconditions.checkState(ecInfo.extent.equals(extentCompacted),
          "Unexpected extent seen on compaction commit %s %s", ecInfo.extent, extentCompacted);
      Tablet tablet = currentTablets.get(ecInfo.extent);
      if (tablet != null) {
        tablet.asCompactable().commitExternalCompaction(extCompactionId, fileSize, entries);
        compactablesToCheck.add(tablet.asCompactable());
      }
      runningExternalCompactions.remove(extCompactionId);
    }
  }

  public void externalCompactionFailed(ExternalCompactionId ecid, KeyExtent extentCompacted,
      Map<KeyExtent,Tablet> currentTablets) {
    var ecInfo = runningExternalCompactions.get(ecid);
    if (ecInfo != null) {
      Preconditions.checkState(ecInfo.extent.equals(extentCompacted),
          "Unexpected extent seen on compaction commit %s %s", ecInfo.extent, extentCompacted);
      Tablet tablet = currentTablets.get(ecInfo.extent);
      if (tablet != null) {
        tablet.asCompactable().externalCompactionFailed(ecid);
        compactablesToCheck.add(tablet.asCompactable());
      }
      runningExternalCompactions.remove(ecid);
    }
  }

  public List<TCompactionQueueSummary> getCompactionQueueSummaries() {
    return externalExecutors.values().stream().flatMap(ece -> ece.summarize())
        .collect(Collectors.toList());
  }

  public static class ExtCompMetric {
    public CompactionExecutorId ceid;
    public int running;
    public int queued;
  }

  public Collection<ExtCompMetric> getExternalMetrics() {
    Map<CompactionExecutorId,ExtCompMetric> metrics = new HashMap<>();

    externalExecutors.forEach((eeid, ece) -> {
      ExtCompMetric ecm = new ExtCompMetric();
      ecm.ceid = eeid;
      ecm.queued = ece.getCompactionsQueued(CType.EXTERNAL);
      metrics.put(eeid, ecm);
    });

    runningExternalCompactions.values().forEach(eci -> {
      var ecm = metrics.computeIfAbsent(eci.executor, id -> {
        var newEcm = new ExtCompMetric();
        newEcm.ceid = id;
        return newEcm;
      });

      ecm.running++;
    });

    return metrics.values();
  }

  public void compactableClosed(KeyExtent extent, Set<CompactionServiceId> servicesUsed,
      Set<ExternalCompactionId> ecids) {
    runningExternalCompactions.keySet().removeAll(ecids);
    servicesUsed.stream().map(services::get).filter(Objects::nonNull)
        .forEach(compService -> compService.compactableClosed(extent));
  }
}
