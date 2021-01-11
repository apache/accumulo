/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.compactions;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner.PlanningParameters;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.ExecutorManager;
import org.apache.accumulo.core.util.compaction.CompactionPlanImpl;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.core.util.ratelimit.SharedRateLimiterFactory;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.tserver.compactions.SubmittedJob.Status;
import org.apache.accumulo.tserver.metrics.CompactionExecutorsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class CompactionService {
  private CompactionPlanner planner;
  private Map<CompactionExecutorId,CompactionExecutor> executors;
  private final CompactionServiceId myId;
  private Map<KeyExtent,Collection<SubmittedJob>> submittedJobs = new ConcurrentHashMap<>();
  private ServerContext serverCtx;
  private String plannerClassName;
  private Map<String,String> plannerOpts;
  private CompactionExecutorsMetrics ceMetrics;
  private ExecutorService planningExecutor;
  private Map<CompactionKind,ConcurrentMap<KeyExtent,Compactable>> queuedForPlanning;

  private RateLimiter readLimiter;
  private RateLimiter writeLimiter;
  private AtomicLong rateLimit = new AtomicLong(0);

  private static final Logger log = LoggerFactory.getLogger(CompactionService.class);

  private class CpInitParams implements CompactionPlanner.InitParameters {

    private final Map<String,String> plannerOpts;
    private final Map<CompactionExecutorId,Integer> requestedExecutors;

    CpInitParams(Map<String,String> plannerOpts) {
      this.plannerOpts = plannerOpts;
      this.requestedExecutors = new HashMap<>();
    }

    @Override
    public ServiceEnvironment getServiceEnvironment() {
      return new ServiceEnvironmentImpl(serverCtx);
    }

    @Override
    public Map<String,String> getOptions() {
      return plannerOpts;
    }

    @Override
    public String getFullyQualifiedOption(String key) {
      return Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + myId + ".opts." + key;
    }

    @Override
    public ExecutorManager getExecutorManager() {
      return new ExecutorManager() {
        @Override
        public CompactionExecutorId createExecutor(String executorName, int threads) {
          Preconditions.checkArgument(threads > 0, "Positive number of threads required : %s",
              threads);
          var ceid = CompactionExecutorId.of(myId + "." + executorName);
          Preconditions.checkState(!requestedExecutors.containsKey(ceid));
          requestedExecutors.put(ceid, threads);
          return ceid;
        }
      };
    }

  }

  public CompactionService(String serviceName, String plannerClass, Long maxRate,
      Map<String,String> plannerOptions, ServerContext sctx, CompactionExecutorsMetrics ceMetrics) {

    Preconditions.checkArgument(maxRate >= 0);

    this.myId = CompactionServiceId.of(serviceName);
    this.serverCtx = sctx;
    this.plannerClassName = plannerClass;
    this.plannerOpts = plannerOptions;
    this.ceMetrics = ceMetrics;

    var initParams = new CpInitParams(plannerOpts);
    planner = createPlanner(plannerClass);
    planner.init(initParams);

    Map<CompactionExecutorId,CompactionExecutor> tmpExecutors = new HashMap<>();

    this.rateLimit.set(maxRate);

    this.readLimiter = SharedRateLimiterFactory.getInstance(this.serverCtx.getConfiguration())
        .create("CS_" + serviceName + "_read", () -> rateLimit.get());
    this.writeLimiter = SharedRateLimiterFactory.getInstance(this.serverCtx.getConfiguration())
        .create("CS_" + serviceName + "_write", () -> rateLimit.get());

    initParams.requestedExecutors.forEach((ceid, numThreads) -> {
      tmpExecutors.put(ceid,
          new CompactionExecutor(ceid, numThreads, ceMetrics, readLimiter, writeLimiter));
    });

    this.executors = Map.copyOf(tmpExecutors);

    this.planningExecutor =
        ThreadPools.createThreadPool(1, 1, 0L, TimeUnit.MILLISECONDS, "CompactionPlanner", false);

    this.queuedForPlanning = new EnumMap<>(CompactionKind.class);
    for (CompactionKind kind : CompactionKind.values()) {
      queuedForPlanning.put(kind, new ConcurrentHashMap<KeyExtent,Compactable>());
    }

    log.debug("Created new compaction service id:{} rate limit:{} planner:{} planner options:{}",
        myId, maxRate, plannerClass, plannerOptions);
  }

  private CompactionPlanner createPlanner(String plannerClass) {
    try {
      return ConfigurationTypeHelper.getClassInstance(null, plannerClass, CompactionPlanner.class);
    } catch (IOException | ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean reconcile(Set<CompactionJob> jobs, Collection<SubmittedJob> submitted) {
    for (SubmittedJob submittedJob : submitted) {
      // only read status once to avoid race conditions since multiple compares are done
      var status = submittedJob.getStatus();
      if (status == Status.QUEUED) {
        if (!jobs.remove(submittedJob.getJob())) {
          if (!submittedJob.cancel(Status.QUEUED)) {
            return false;
          }
        }
      } else if (status == Status.RUNNING) {
        for (CompactionJob job : jobs) {
          if (!Collections.disjoint(submittedJob.getJob().getFiles(), job.getFiles())) {
            return false;
          }
        }
      }
    }

    return true;
  }

  public void compact(CompactionKind kind, Compactable compactable,
      Consumer<Compactable> completionCallback) {
    Objects.requireNonNull(compactable);

    if (queuedForPlanning.get(kind).putIfAbsent(compactable.getExtent(), compactable) == null) {
      try {
        planningExecutor.execute(() -> {
          try {
            planCompaction(kind, compactable, completionCallback);
          } finally {
            queuedForPlanning.get(kind).remove(compactable.getExtent());
          }
        });
      } catch (RejectedExecutionException e) {
        queuedForPlanning.get(kind).remove(compactable.getExtent());
        throw e;
      }
    }
  }

  private void planCompaction(CompactionKind kind, Compactable compactable,
      Consumer<Compactable> completionCallback) {
    var files = compactable.getFiles(myId, kind);

    if (files.isEmpty() || files.get().candidates.isEmpty()) {
      log.trace("Compactable returned no files {} {} {}", compactable.getExtent(), kind, files);
      return;
    }

    PlanningParameters params = new PlanningParameters() {

      @Override
      public TableId getTableId() {
        return compactable.getTableId();
      }

      @Override
      public ServiceEnvironment getServiceEnvironment() {
        return new ServiceEnvironmentImpl(serverCtx);
      }

      @Override
      public double getRatio() {
        return compactable.getCompactionRatio();
      }

      @Override
      public CompactionKind getKind() {
        return kind;
      }

      @Override
      public Collection<CompactionJob> getRunningCompactions() {
        return files.get().compacting;
      }

      @Override
      public Collection<CompactableFile> getCandidates() {
        return files.get().candidates;
      }

      @Override
      public Collection<CompactableFile> getAll() {
        return files.get().allFiles;
      }

      @Override
      public Map<String,String> getExecutionHints() {
        if (kind == CompactionKind.USER)
          return files.get().executionHints;
        else
          return Map.of();
      }

      @Override
      public CompactionPlan.Builder createPlanBuilder() {
        return new CompactionPlanImpl.BuilderImpl(kind, files.get().allFiles,
            files.get().candidates);
      }
    };

    log.trace("Planning compactions {} {} {} {}", planner.getClass().getName(),
        compactable.getExtent(), kind, files);

    CompactionPlan plan;
    try {
      plan = planner.makePlan(params);
    } catch (RuntimeException e) {
      log.debug("Planner failed {} {} {} {}", planner.getClass().getName(), compactable.getExtent(),
          kind, files, e);
      throw e;
    }

    plan = convertPlan(plan, kind, files.get().allFiles, files.get().candidates);

    Set<CompactionJob> jobs = new HashSet<>(plan.getJobs());

    Collection<SubmittedJob> submitted =
        submittedJobs.getOrDefault(compactable.getExtent(), List.of());
    if (!submitted.isEmpty()) {
      submitted.removeIf(sj -> {
        // to avoid race conditions, only read status once and use local var for the two compares
        var status = sj.getStatus();
        return status != Status.QUEUED && status != Status.RUNNING;
      });
    }

    if (reconcile(jobs, submitted)) {
      for (CompactionJob job : jobs) {
        var sjob =
            executors.get(job.getExecutor()).submit(myId, job, compactable, completionCallback);
        // its important that the collection created in computeIfAbsent supports concurrency
        submittedJobs.computeIfAbsent(compactable.getExtent(), k -> new ConcurrentLinkedQueue<>())
            .add(sjob);
      }

      if (!jobs.isEmpty()) {
        log.trace("Submitted compaction plan {} id:{} files:{} plan:{}", compactable.getExtent(),
            myId, files, plan);
      }
    } else {
      log.trace("Did not submit compaction plan {} id:{} files:{} plan:{}", compactable.getExtent(),
          myId, files, plan);
    }
  }

  private CompactionPlan convertPlan(CompactionPlan plan, CompactionKind kind,
      Set<CompactableFile> allFiles, Set<CompactableFile> candidates) {

    if (plan.getClass().equals(CompactionPlanImpl.class))
      return plan;

    var builder = new CompactionPlanImpl.BuilderImpl(kind, allFiles, candidates);

    for (var job : plan.getJobs()) {
      Preconditions.checkArgument(job.getKind() == kind, "Unexpected compaction kind %s != %s",
          job.getKind(), kind);
      builder.addJob(job.getPriority(), job.getExecutor(), job.getFiles());
    }

    return builder.build();
  }

  public boolean isCompactionQueued(KeyExtent extent) {
    return submittedJobs.getOrDefault(extent, List.of()).stream()
        .anyMatch(job -> job.getStatus() == Status.QUEUED);
  }

  public void configurationChanged(String plannerClassName, Long maxRate,
      Map<String,String> plannerOptions) {
    Preconditions.checkArgument(maxRate >= 0);

    var old = this.rateLimit.getAndSet(maxRate);
    if (old != maxRate)
      log.debug("Updated compaction service id:{} rate limit:{}", myId, maxRate);

    if (this.plannerClassName.equals(plannerClassName) && this.plannerOpts.equals(plannerOptions))
      return;

    var initParams = new CpInitParams(plannerOptions);
    var tmpPlanner = createPlanner(plannerClassName);
    tmpPlanner.init(initParams);

    Map<CompactionExecutorId,CompactionExecutor> tmpExecutors = new HashMap<>();

    initParams.requestedExecutors.forEach((ceid, numThreads) -> {
      var executor = executors.get(ceid);
      if (executor == null) {
        executor = new CompactionExecutor(ceid, numThreads, ceMetrics, readLimiter, writeLimiter);
      } else {
        executor.setThreads(numThreads);
      }
      tmpExecutors.put(ceid, executor);
    });

    Sets.difference(executors.keySet(), tmpExecutors.keySet()).forEach(ceid -> {
      executors.get(ceid).stop();
    });

    this.plannerClassName = plannerClassName;
    this.plannerOpts = plannerOptions;
    this.executors = Map.copyOf(tmpExecutors);
    this.planner = tmpPlanner;

    log.debug("Updated compaction service id:{} planner:{} options:{}", myId, plannerClassName,
        plannerOptions);

  }

  public void stop() {
    executors.values().forEach(CompactionExecutor::stop);
  }

  int getCompactionsRunning() {
    return executors.values().stream().mapToInt(CompactionExecutor::getCompactionsRunning).sum();
  }

  int getCompactionsQueued() {
    return executors.values().stream().mapToInt(CompactionExecutor::getCompactionsQueued).sum();
  }
}
