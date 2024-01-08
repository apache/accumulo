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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
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
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.core.util.cache.Caches.CacheName;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlanImpl;
import org.apache.accumulo.core.util.compaction.CompactionPlannerInitParams;
import org.apache.accumulo.core.util.compaction.CompactionServicesConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class CompactionJobGenerator {
  private static final Logger log = LoggerFactory.getLogger(CompactionJobGenerator.class);
  private final CompactionServicesConfig servicesConfig;
  private final Map<CompactionServiceId,CompactionPlanner> planners = new HashMap<>();
  private final Cache<TableId,CompactionDispatcher> dispatchers;
  private final Set<CompactionServiceId> serviceIds;
  private final PluginEnvironment env;
  private final Map<Long,Map<String,String>> allExecutionHints;
  private final Cache<Pair<TableId,CompactionServiceId>,Long> unknownCompactionServiceErrorCache;

  public CompactionJobGenerator(PluginEnvironment env,
      Map<Long,Map<String,String>> executionHints) {
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
    unknownCompactionServiceErrorCache =
        Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).build();
  }

  public Collection<CompactionJob> generateJobs(TabletMetadata tablet, Set<CompactionKind> kinds) {

    // ELASTICITY_TODO do not want user configured plugins to cause exceptions that prevents tablets
    // from being
    // assigned. So probably want to catch exceptions and log, but not too spammily OR some how
    // report something
    // back to the manager so it can log.

    Collection<CompactionJob> systemJobs = Set.of();

    if (kinds.contains(CompactionKind.SYSTEM)) {
      CompactionServiceId serviceId = dispatch(CompactionKind.SYSTEM, tablet, Map.of());
      systemJobs = planCompactions(serviceId, CompactionKind.SYSTEM, tablet, Map.of());
    }

    Collection<CompactionJob> userJobs = Set.of();

    if (kinds.contains(CompactionKind.USER) && tablet.getSelectedFiles() != null) {
      var hints = allExecutionHints.get(tablet.getSelectedFiles().getFateTxId());
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
      var cacheKey = new Pair<>(tablet.getTableId(), serviceId);
      var last = unknownCompactionServiceErrorCache.getIfPresent(cacheKey);
      if (last == null) {
        // have not logged an error recently for this, so lets log one
        log.error(
            "Tablet {} returned non-existent compaction service {} for compaction type {}.  Check"
                + " the table compaction dispatcher configuration. No compactions will happen"
                + " until the configuration is fixed. This log message is temporarily suppressed for the"
                + " entire table.",
            tablet.getExtent(), serviceId, kind);
        unknownCompactionServiceErrorCache.put(cacheKey, System.currentTimeMillis());
      }

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
        // remove any files that are selected
        if (tablet.getSelectedFiles() != null) {
          tmpFiles.keySet().removeAll(tablet.getSelectedFiles().getFiles());
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

    return planner.makePlan(params).getJobs();
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
      log.error(
          "Failed to create compaction planner for {} using class:{} options:{}.  Compaction service will not start any new compactions until its configuration is fixed.",
          serviceId, plannerClassName, options, e);
      planner = new ProvisionalCompactionPlanner(serviceId);
    }
    return planner;
  }
}
