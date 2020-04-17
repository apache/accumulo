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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner.PlanningParameters;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.ExecutorManager;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.tserver.compactions.SubmittedJob.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class CompactionServiceImpl implements CompactionService {
  // TODO when user has a compaction strat configured at table, may want to have a planner that uses
  // this to select files.
  private final CompactionPlanner planner;
  private final Map<CompactionExecutorId,CompactionExecutor> executors;
  private final CompactionServiceId myId;
  private Map<KeyExtent,List<SubmittedJob>> submittedJobs = new ConcurrentHashMap<>();

  private static final Logger log = LoggerFactory.getLogger(CompactionServiceImpl.class);

  public CompactionServiceImpl(String serviceName, String plannerClass,
      Map<String,String> serviceOptions, ServerContext sctx) {

    this.myId = CompactionServiceId.of(serviceName);

    try {
      planner =
          ConfigurationTypeHelper.getClassInstance(null, plannerClass, CompactionPlanner.class);
    } catch (IOException | ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }

    Map<CompactionExecutorId,CompactionExecutor> tmpExecutors = new HashMap<>();

    planner.init(new CompactionPlanner.InitParameters() {

      @Override
      public ServiceEnvironment getServiceEnvironment() {
        return new ServiceEnvironmentImpl(sctx);
      }

      @Override
      public Map<String,String> getOptions() {
        return serviceOptions;
      }

      @Override
      public ExecutorManager getExecutorManager() {
        return new ExecutorManager() {
          @Override
          public CompactionExecutorId createExecutor(String executorName, int threads) {
            var ceid = CompactionExecutorId.of(serviceName + "." + executorName);
            Preconditions.checkState(!tmpExecutors.containsKey(ceid));
            tmpExecutors.put(ceid, new CompactionExecutor(ceid, threads));
            return ceid;
          }
        };
      }

      @Override
      public String getFullyQualifiedOption(String key) {
        return Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + serviceName + ".opts." + key;
      }
    });

    this.executors = Map.copyOf(tmpExecutors);

    log.debug("Created new compaction service id:{} executors:{}", myId, executors.keySet());
  }

  private boolean reconcile(Collection<CompactionJob> jobs, List<SubmittedJob> submitted) {
    for (SubmittedJob submittedJob : submitted) {
      if (submittedJob.getStatus() == Status.QUEUED) {
        // TODO this is O(M*N) unless set
        if (!jobs.remove(submittedJob.getJob())) {
          if (!submittedJob.cancel(Status.QUEUED)) {
            return false;
          }
        }
      } else if (submittedJob.getStatus() == Status.RUNNING) {
        for (CompactionJob job : jobs) {
          if (!Collections.disjoint(submittedJob.getJob().getFiles(), job.getFiles())) {
            return false;
          }
        }
      }
    }

    return true;
  }

  @Override
  public void compact(CompactionKind kind, Compactable compactable,
      Consumer<Compactable> completionCallback) {
    // TODO this could take a while... could run this in a thread pool
    var files = compactable.getFiles(myId, kind);
    if (files.isPresent() && !files.get().candidates.isEmpty()) {
      PlanningParameters params = new PlanningParameters() {

        @Override
        public TableId getTableId() {
          return compactable.getTableId();
        }

        @Override
        public ServiceEnvironment getServiceEnvironment() {
          // TODO Auto-generated method stub
          return null;
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
      };

      var plan = planner.makePlan(params);
      Set<CompactionJob> jobs = new HashSet<>(plan.getJobs());
      List<SubmittedJob> submitted = submittedJobs.getOrDefault(compactable.getExtent(), List.of());
      if (!submitted.isEmpty()) {
        // TODO only read status once
        submitted
            .removeIf(sj -> sj.getStatus() != Status.QUEUED && sj.getStatus() != Status.RUNNING);
      }

      if (reconcile(jobs, submitted)) {
        for (CompactionJob job : jobs) {
          var sjob =
              executors.get(job.getExecutor()).submit(myId, job, compactable, completionCallback);
          submittedJobs.computeIfAbsent(compactable.getExtent(), k -> new ArrayList<>()).add(sjob);
        }
      } else {
        log.debug("Did not submit plan for {}", compactable.getExtent());
      }
    }
  }
}
