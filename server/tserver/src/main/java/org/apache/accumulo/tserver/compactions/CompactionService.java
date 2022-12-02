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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
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
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlanImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlannerInitParams;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.core.util.ratelimit.SharedRateLimiterFactory;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.tserver.compactions.CompactionExecutor.CType;
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
  private ServerContext context;
  private String plannerClassName;
  private Map<String,String> plannerOpts;
  private CompactionExecutorsMetrics ceMetrics;
  private ExecutorService planningExecutor;
  private Map<CompactionKind,ConcurrentMap<KeyExtent,Compactable>> queuedForPlanning;

  private RateLimiter readLimiter;
  private RateLimiter writeLimiter;
  private AtomicLong rateLimit = new AtomicLong(0);
  private Function<CompactionExecutorId,ExternalCompactionExecutor> externExecutorSupplier;

  private static final Logger log = LoggerFactory.getLogger(CompactionService.class);

  public CompactionService(String serviceName, String plannerClass, Long maxRate,
      Map<String,String> plannerOptions, ServerContext context,
      CompactionExecutorsMetrics ceMetrics,
      Function<CompactionExecutorId,ExternalCompactionExecutor> externExecutorSupplier) {

    Preconditions.checkArgument(maxRate >= 0);

    this.myId = CompactionServiceId.of(serviceName);
    this.context = context;
    this.plannerClassName = plannerClass;
    this.plannerOpts = plannerOptions;
    this.ceMetrics = ceMetrics;
    this.externExecutorSupplier = externExecutorSupplier;

    var initParams =
        new CompactionPlannerInitParams(myId, plannerOpts, new ServiceEnvironmentImpl(context));
    planner = createPlanner(plannerClass);
    planner.init(initParams);

    Map<CompactionExecutorId,CompactionExecutor> tmpExecutors = new HashMap<>();

    this.rateLimit.set(maxRate);

    this.readLimiter = SharedRateLimiterFactory.getInstance(this.context.getConfiguration())
        .create("CS_" + serviceName + "_read", () -> rateLimit.get());
    this.writeLimiter = SharedRateLimiterFactory.getInstance(this.context.getConfiguration())
        .create("CS_" + serviceName + "_write", () -> rateLimit.get());

    initParams.getRequestedExecutors().forEach((ceid, numThreads) -> {
      tmpExecutors.put(ceid,
          new InternalCompactionExecutor(ceid, numThreads, ceMetrics, readLimiter, writeLimiter));
    });

    initParams.getRequestedExternalExecutors().forEach((ceid) -> {
      tmpExecutors.put(ceid, externExecutorSupplier.apply(ceid));
    });

    this.executors = Map.copyOf(tmpExecutors);

    this.planningExecutor = ThreadPools.getServerThreadPools().createThreadPool(1, 1, 0L,
        TimeUnit.MILLISECONDS, "CompactionPlanner", false);

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
        // Note the submitted jobs set may not contain external compactions that started on another
        // tserver. However, this is ok as the purpose of this check is to look for compaction jobs
        // that transitioned from QUEUED to RUNNING during planning. Any external compactions
        // started on another tserver will not make this transition during planning.
        for (CompactionJob job : jobs) {
          if (!Collections.disjoint(submittedJob.getJob().getFiles(), job.getFiles())) {
            return false;
          }
        }
      }
    }

    return true;
  }

  /**
   * Get compaction plan for the provided compactable tablet and possibly submit for compaction.
   * Plans get added to the planning queue before calling the planningExecutor to get the plan. If
   * no files are selected, return. Otherwise, submit the compaction job.
   */
  public void submitCompaction(CompactionKind kind, Compactable compactable,
      Consumer<Compactable> completionCallback) {
    Objects.requireNonNull(compactable);

    // add tablet to planning queue and use planningExecutor to get the plan
    if (queuedForPlanning.get(kind).putIfAbsent(compactable.getExtent(), compactable) == null) {
      try {
        planningExecutor.execute(() -> {
          try {
            Optional<Compactable.Files> files = compactable.getFiles(myId, kind);
            if (files.isEmpty() || files.get().candidates.isEmpty()) {
              log.trace("Compactable returned no files {} {}", compactable.getExtent(), kind);
            } else {
              CompactionPlan plan = getCompactionPlan(kind, files.get(), compactable);
              submitCompactionJob(plan, files.get(), compactable, completionCallback);
            }
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

  private class CpPlanParams implements PlanningParameters {
    private final CompactionKind kind;
    private final Compactable comp;
    private final Compactable.Files files;

    public CpPlanParams(CompactionKind kind, Compactable comp, Compactable.Files files) {
      this.kind = kind;
      this.comp = comp;
      this.files = files;
    }

    private final ServiceEnvironment senv = new ServiceEnvironmentImpl(context);

    @Override
    public TableId getTableId() {
      return comp.getTableId();
    }

    @Override
    public ServiceEnvironment getServiceEnvironment() {
      return senv;
    }

    @Override
    public double getRatio() {
      return comp.getCompactionRatio();
    }

    @Override
    public CompactionKind getKind() {
      return kind;
    }

    @Override
    public Collection<CompactionJob> getRunningCompactions() {
      return files.compacting;
    }

    @Override
    public Collection<CompactableFile> getCandidates() {
      return files.candidates;
    }

    @Override
    public Collection<CompactableFile> getAll() {
      return files.allFiles;
    }

    @Override
    public Map<String,String> getExecutionHints() {
      if (kind == CompactionKind.USER) {
        return files.executionHints;
      } else {
        return Map.of();
      }
    }

    @Override
    public CompactionPlan.Builder createPlanBuilder() {
      return new CompactionPlanImpl.BuilderImpl(kind, files.allFiles, files.candidates);
    }
  }

  private CompactionPlan getCompactionPlan(CompactionKind kind, Compactable.Files files,
      Compactable compactable) {
    PlanningParameters params = new CpPlanParams(kind, compactable, files);

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

    return convertPlan(plan, kind, files.allFiles, files.candidates);
  }

  private void submitCompactionJob(CompactionPlan plan, Compactable.Files files,
      Compactable compactable, Consumer<Compactable> completionCallback) {
    // log error if tablet is metadata and compaction is external
    var execIds = plan.getJobs().stream().map(cj -> (CompactionExecutorIdImpl) cj.getExecutor());
    if (compactable.getExtent().isMeta() && execIds.anyMatch(ceid -> ceid.isExternalId())) {
      log.error(
          "Compacting metadata tablets on external compactors is not supported, please change "
              + "config for compaction service ({}) and/or table ASAP.  {} is not compacting, "
              + "ignoring plan {}",
          myId, compactable.getExtent(), plan);
      return;
    }

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
        CompactionExecutor executor = executors.get(job.getExecutor());
        var submittedJob = executor.submit(myId, job, compactable, completionCallback);
        // its important that the collection created in computeIfAbsent supports concurrency
        submittedJobs.computeIfAbsent(compactable.getExtent(), k -> new ConcurrentLinkedQueue<>())
            .add(submittedJob);
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

    if (plan.getClass().equals(CompactionPlanImpl.class)) {
      return plan;
    }

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
    if (old != maxRate) {
      log.debug("Updated compaction service id:{} rate limit:{}", myId, maxRate);
    }

    if (this.plannerClassName.equals(plannerClassName) && this.plannerOpts.equals(plannerOptions)) {
      return;
    }

    var initParams =
        new CompactionPlannerInitParams(myId, plannerOptions, new ServiceEnvironmentImpl(context));
    var tmpPlanner = createPlanner(plannerClassName);
    tmpPlanner.init(initParams);

    Map<CompactionExecutorId,CompactionExecutor> tmpExecutors = new HashMap<>();

    initParams.getRequestedExecutors().forEach((ceid, numThreads) -> {
      InternalCompactionExecutor executor = (InternalCompactionExecutor) executors.get(ceid);
      if (executor == null) {
        executor =
            new InternalCompactionExecutor(ceid, numThreads, ceMetrics, readLimiter, writeLimiter);
      } else {
        executor.setThreads(numThreads);
      }
      tmpExecutors.put(ceid, executor);
    });

    initParams.getRequestedExternalExecutors().forEach(ceid -> {
      ExternalCompactionExecutor executor = (ExternalCompactionExecutor) executors.get(ceid);
      if (executor == null) {
        executor = externExecutorSupplier.apply(ceid);
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

  int getCompactionsRunning(CType ctype) {
    return executors.values().stream().mapToInt(ce -> ce.getCompactionsRunning(ctype)).sum();
  }

  int getCompactionsQueued(CType ctype) {
    return executors.values().stream().mapToInt(ce -> ce.getCompactionsQueued(ctype)).sum();
  }

  public void getExternalExecutorsInUse(Consumer<CompactionExecutorId> idConsumer) {
    executors.forEach((ceid, ce) -> {
      if (ce instanceof ExternalCompactionExecutor) {
        idConsumer.accept(ceid);
      }
    });
  }

  public void compactableClosed(KeyExtent extent) {
    executors.values().forEach(compExecutor -> compExecutor.compactableClosed(extent));
  }
}
