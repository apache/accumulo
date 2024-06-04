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
package org.apache.accumulo.server.compaction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.logging.ConditionalLogger;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactionServices;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlanImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlannerInitParams;
import org.apache.accumulo.core.util.compaction.CompactionServicesConfig;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import com.github.benmanes.caffeine.cache.Cache;

public class CompactionJobGenerator {
  private static final Logger log = LoggerFactory.getLogger(CompactionJobGenerator.class);
  private static final Logger UNKNOWN_SERVICE_ERROR_LOG =
      new ConditionalLogger.EscalatingLogger(log, Duration.ofMinutes(5), 3000, Level.ERROR);
  private static final Logger PLANNING_INIT_ERROR_LOG =
      new ConditionalLogger.EscalatingLogger(log, Duration.ofMinutes(5), 3000, Level.ERROR);
  private static final Logger PLANNING_ERROR_LOG =
      new ConditionalLogger.EscalatingLogger(log, Duration.ofMinutes(5), 3000, Level.ERROR);

  private final CompactionServicesConfig servicesConfig;
  private final Map<CompactionServiceId,CompactionPlanner> planners = new HashMap<>();
  private final Cache<TableId,CompactionDispatcher> dispatchers;
  private final Set<CompactionServiceId> serviceIds;
  private final PluginEnvironment env;
  private final Map<FateId,Map<String,String>> allExecutionHints;
  private final SteadyTime steadyTime;

  public CompactionJobGenerator(PluginEnvironment env,
      Map<FateId,Map<String,String>> executionHints, SteadyTime steadyTime) {
    servicesConfig = new CompactionServicesConfig(env.getConfiguration());
    serviceIds = servicesConfig.getPlanners().keySet().stream().map(CompactionServiceId::of)
        .collect(Collectors.toUnmodifiableSet());

    dispatchers = Caches.getInstance().createNewBuilder(CacheName.COMPACTION_DISPATCHERS, false)
        .maximumSize(10).build();
    this.env = env;
    if (executionHints.isEmpty()) {
      this.allExecutionHints = executionHints;
    } else {
      this.allExecutionHints = new HashMap<>();
      // Make the maps that will be passed to plugins unmodifiable. Do this once, so it does not
      // need to be done for each tablet.
      executionHints.forEach((k, v) -> allExecutionHints.put(k,
          v.isEmpty() ? Map.of() : Collections.unmodifiableMap(v)));
    }

    this.steadyTime = steadyTime;
  }

  public Collection<CompactionJob> generateJobs(TabletMetadata tablet, Set<CompactionKind> kinds) {
    Collection<CompactionJob> systemJobs = Set.of();

    log.debug("Planning for {} {} {}", tablet.getExtent(), kinds, this.hashCode());

    if (kinds.contains(CompactionKind.SYSTEM)) {
      CompactionServiceId serviceId = dispatch(CompactionKind.SYSTEM, tablet, Map.of());
      systemJobs = planCompactions(serviceId, CompactionKind.SYSTEM, tablet, Map.of());
    }

    Collection<CompactionJob> userJobs = Set.of();

    if (kinds.contains(CompactionKind.USER) && tablet.getSelectedFiles() != null) {
      var hints = allExecutionHints.get(tablet.getSelectedFiles().getFateId());
      if (hints != null) {
        CompactionServiceId serviceId = dispatch(CompactionKind.USER, tablet, hints);
        userJobs = planCompactions(serviceId, CompactionKind.USER, tablet, hints);
      }
    }

    if (userJobs.isEmpty()) {
      return systemJobs;
    } else if (systemJobs.isEmpty()) {
      return userJobs;
    } else {
      var all = new ArrayList<CompactionJob>(systemJobs.size() + userJobs.size());
      all.addAll(systemJobs);
      all.addAll(userJobs);
      return all;
    }
  }

  private CompactionServiceId dispatch(CompactionKind kind, TabletMetadata tablet,
      Map<String,String> executionHints) {

    CompactionDispatcher dispatcher = dispatchers.get(tablet.getTableId(),
        tableId -> CompactionPluginUtils.createDispatcher((ServiceEnvironment) env, tableId));

    CompactionDispatcher.DispatchParameters dispatchParams =
        new CompactionDispatcher.DispatchParameters() {
          @Override
          public CompactionServices getCompactionServices() {
            return () -> serviceIds;
          }

          @Override
          public ServiceEnvironment getServiceEnv() {
            return (ServiceEnvironment) env;
          }

          @Override
          public CompactionKind getCompactionKind() {
            return kind;
          }

          @Override
          public Map<String,String> getExecutionHints() {
            return executionHints;
          }
        };

    return dispatcher.dispatch(dispatchParams).getService();
  }

  private Collection<CompactionJob> planCompactions(CompactionServiceId serviceId,
      CompactionKind kind, TabletMetadata tablet, Map<String,String> executionHints) {

    if (!servicesConfig.getPlanners().containsKey(serviceId.canonical())) {
      UNKNOWN_SERVICE_ERROR_LOG.trace(
          "Table {} returned non-existent compaction service {} for compaction type {}.  Check"
              + " the table compaction dispatcher configuration. No compactions will happen"
              + " until the configuration is fixed. This log message is temporarily suppressed.",
          tablet.getExtent().tableId(), serviceId, kind);
      return Set.of();
    }

    CompactionPlanner planner =
        planners.computeIfAbsent(serviceId, sid -> createPlanner(tablet.getTableId(), serviceId));

    // selecting indicator
    // selected files

    String ratioStr =
        env.getConfiguration(tablet.getTableId()).get(Property.TABLE_MAJC_RATIO.getKey());
    if (ratioStr == null) {
      ratioStr = Property.TABLE_MAJC_RATIO.getDefaultValue();
    }

    double ratio = Double.parseDouble(ratioStr);

    Set<CompactableFile> allFiles = tablet.getFilesMap().entrySet().stream()
        .map(entry -> new CompactableFileImpl(entry.getKey(), entry.getValue()))
        .collect(Collectors.toUnmodifiableSet());
    Set<CompactableFile> candidates;

    if (kind == CompactionKind.SYSTEM) {
      if (tablet.getExternalCompactions().isEmpty() && tablet.getSelectedFiles() == null) {
        candidates = allFiles;
      } else {
        var tmpFiles = new HashMap<>(tablet.getFilesMap());
        // remove any files that are in active compactions
        tablet.getExternalCompactions().values().stream().flatMap(ecm -> ecm.getJobFiles().stream())
            .forEach(tmpFiles::remove);
        // remove any files that are selected and the user compaction has completed
        // at least 1 job, otherwise we can keep the files
        var selectedFiles = tablet.getSelectedFiles();

        if (selectedFiles != null) {
          long selectedExpirationDuration =
              ConfigurationTypeHelper.getTimeInMillis(env.getConfiguration(tablet.getTableId())
                  .get(Property.TABLE_COMPACTION_SELECTION_EXPIRATION.getKey()));

          // If jobs are completed, or selected time has not expired, the remove
          // from the candidate list otherwise we can cancel the selection
          if (selectedFiles.getCompletedJobs() > 0
              || (steadyTime.minus(selectedFiles.getSelectedTime()).toMillis()
                  < selectedExpirationDuration)) {
            tmpFiles.keySet().removeAll(selectedFiles.getFiles());
          }
        }
        candidates = tmpFiles.entrySet().stream()
            .map(entry -> new CompactableFileImpl(entry.getKey(), entry.getValue()))
            .collect(Collectors.toUnmodifiableSet());
      }
    } else if (kind == CompactionKind.USER) {
      var selectedFiles = new HashSet<>(tablet.getSelectedFiles().getFiles());
      tablet.getExternalCompactions().values().stream().flatMap(ecm -> ecm.getJobFiles().stream())
          .forEach(selectedFiles::remove);
      candidates = selectedFiles.stream()
          .map(file -> new CompactableFileImpl(file, tablet.getFilesMap().get(file)))
          .collect(Collectors.toUnmodifiableSet());
    } else {
      throw new UnsupportedOperationException();
    }

    if (candidates.isEmpty()) {
      // there are not candidate files for compaction, so no reason to call the planner
      return Set.of();
    }

    CompactionPlanner.PlanningParameters params = new CompactionPlanner.PlanningParameters() {
      @Override
      public TableId getTableId() {
        return tablet.getTableId();
      }

      @Override
      public ServiceEnvironment getServiceEnvironment() {
        return (ServiceEnvironment) env;
      }

      @Override
      public CompactionKind getKind() {
        return kind;
      }

      @Override
      public double getRatio() {
        return ratio;
      }

      @Override
      public Collection<CompactableFile> getAll() {
        return allFiles;
      }

      @Override
      public Collection<CompactableFile> getCandidates() {
        return candidates;
      }

      @Override
      public Collection<CompactionJob> getRunningCompactions() {
        var allFiles2 = tablet.getFilesMap();
        return tablet.getExternalCompactions().values().stream().map(ecMeta -> {
          Collection<CompactableFile> files = ecMeta.getJobFiles().stream()
              .map(f -> new CompactableFileImpl(f, allFiles2.get(f))).collect(Collectors.toList());
          CompactionJob job = new CompactionJobImpl(ecMeta.getPriority(),
              ecMeta.getCompactionGroupId(), files, ecMeta.getKind(), Optional.empty());
          return job;
        }).collect(Collectors.toUnmodifiableList());
      }

      @Override
      public Map<String,String> getExecutionHints() {
        return executionHints;
      }

      @Override
      public CompactionPlan.Builder createPlanBuilder() {
        return new CompactionPlanImpl.BuilderImpl(kind, allFiles, candidates);
      }
    };
    return planCompactions(planner, params, serviceId);
  }

  private CompactionPlanner createPlanner(TableId tableId, CompactionServiceId serviceId) {

    CompactionPlanner planner;
    String plannerClassName = null;
    Map<String,String> options = null;
    try {
      plannerClassName = servicesConfig.getPlanners().get(serviceId.canonical());
      options = servicesConfig.getOptions().get(serviceId.canonical());
      planner = env.instantiate(tableId, plannerClassName, CompactionPlanner.class);
      CompactionPlannerInitParams initParameters = new CompactionPlannerInitParams(serviceId,
          servicesConfig.getPlannerPrefix(serviceId.canonical()),
          servicesConfig.getOptions().get(serviceId.canonical()), (ServiceEnvironment) env);
      planner.init(initParameters);
    } catch (Exception e) {
      PLANNING_INIT_ERROR_LOG.trace(
          "Failed to create compaction planner for service:{} tableId:{} using class:{} options:{}.  Compaction "
              + "service will not start any new compactions until its configuration is fixed. This log message is "
              + "temporarily suppressed.",
          serviceId, tableId, plannerClassName, options, e);
      planner = new ProvisionalCompactionPlanner(serviceId);
    }
    return planner;
  }

  private Collection<CompactionJob> planCompactions(CompactionPlanner planner,
      CompactionPlanner.PlanningParameters params, CompactionServiceId serviceId) {
    try {
      return planner.makePlan(params).getJobs();
    } catch (Exception e) {
      PLANNING_ERROR_LOG.trace(
          "Failed to plan compactions for service:{} kind:{} tableId:{} hints:{}.  Compaction service may not start any"
              + " new compactions until this issue is resolved. Duplicates of this log message are temporarily"
              + " suppressed.",
          serviceId, params.getKind(), params.getTableId(), params.getExecutionHints(), e);
      return Set.of();
    }
  }
}
