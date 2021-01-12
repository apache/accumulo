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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactionServices;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.fate.util.Retry;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.tserver.metrics.CompactionExecutorsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class CompactionManager {

  private static final Logger log = LoggerFactory.getLogger(CompactionManager.class);

  private Iterable<Compactable> compactables;
  private volatile Map<CompactionServiceId,CompactionService> services;

  private LinkedBlockingQueue<Compactable> compactablesToCheck = new LinkedBlockingQueue<>();

  private long maxTimeBetweenChecks;

  private ServerContext ctx;

  private Config currentCfg;

  private long lastConfigCheckTime = System.nanoTime();

  private CompactionExecutorsMetrics ceMetrics;

  public static final CompactionServiceId DEFAULT_SERVICE = CompactionServiceId.of("default");

  private String lastDeprecationWarning = "";

  private class Config {
    Map<String,String> planners = new HashMap<>();
    Map<String,Long> rateLimits = new HashMap<>();
    Map<String,Map<String,String>> options = new HashMap<>();
    long defaultRateLimit = 0;

    @SuppressWarnings("removal")
    private long getDefaultThroughput(AccumuloConfiguration aconf) {
      if (aconf.isPropertySet(Property.TSERV_MAJC_THROUGHPUT, true)) {
        return aconf.getAsBytes(Property.TSERV_MAJC_THROUGHPUT);
      }

      return ConfigurationTypeHelper
          .getMemoryAsBytes(Property.TSERV_COMPACTION_SERVICE_DEFAULT_RATE_LIMIT.getDefaultValue());
    }

    @SuppressWarnings("removal")
    private Map<String,String> getConfiguration(AccumuloConfiguration aconf) {

      Map<String,String> configs =
          aconf.getAllPropertiesWithPrefix(Property.TSERV_COMPACTION_SERVICE_PREFIX);

      // check if deprecated properties for compaction executor are set
      if (aconf.isPropertySet(Property.TSERV_MAJC_MAXCONCURRENT, true)) {

        String defaultServicePrefix =
            Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + DEFAULT_SERVICE.canonical() + ".";

        // check if any properties for the default compaction service are set
        boolean defaultServicePropsSet = configs.keySet().stream()
            .filter(key -> key.startsWith(defaultServicePrefix)).map(Property::getPropertyByKey)
            .anyMatch(prop -> prop == null || aconf.isPropertySet(prop, true));

        if (defaultServicePropsSet) {

          String warning = String.format(
              "The deprecated property %s was set. Properties with the prefix %s "
                  + "were also set, which replace the deprecated properties. The deprecated "
                  + "property was therefore ignored.",
              Property.TSERV_MAJC_MAXCONCURRENT.getKey(), defaultServicePrefix);

          if (!warning.equals(lastDeprecationWarning)) {
            log.warn(warning);
            lastDeprecationWarning = warning;
          }
        } else {
          String numThreads = aconf.get(Property.TSERV_MAJC_MAXCONCURRENT);

          // Its possible a user has configured the others compaction services, but not the default
          // service. In this case want to produce a config with the default service configs
          // overridden using deprecated configs.

          HashMap<String,String> configsCopy = new HashMap<>(configs);

          Map<String,String> defaultServiceConfigs =
              Map.of(defaultServicePrefix + "planner", DefaultCompactionPlanner.class.getName(),
                  defaultServicePrefix + "planner.opts.executors",
                  "[{'name':'deprecated','numThreads':" + numThreads + "}]");

          configsCopy.putAll(defaultServiceConfigs);

          String warning = String.format(
              "The deprecated property %s was set. Properties with the prefix %s "
                  + "were not set, these should replace the deprecated properties. The old "
                  + "properties were automatically mapped to the new properties in process "
                  + "creating : %s.",
              Property.TSERV_MAJC_MAXCONCURRENT.getKey(), defaultServicePrefix,
              defaultServiceConfigs);

          if (!warning.equals(lastDeprecationWarning)) {
            log.warn(warning);
            lastDeprecationWarning = warning;
          }

          configs = Map.copyOf(configsCopy);
        }
      }

      return configs;

    }

    Config(AccumuloConfiguration aconf) {
      Map<String,String> configs = getConfiguration(aconf);

      configs.forEach((prop, val) -> {

        var suffix = prop.substring(Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey().length());
        String[] tokens = suffix.split("\\.");
        if (tokens.length == 4 && tokens[1].equals("planner") && tokens[2].equals("opts")) {
          options.computeIfAbsent(tokens[0], k -> new HashMap<>()).put(tokens[3], val);
        } else if (tokens.length == 2 && tokens[1].equals("planner")) {
          planners.put(tokens[0], val);
        } else if (tokens.length == 3 && tokens[1].equals("rate") && tokens[2].equals("limit")) {
          var eprop = Property.getPropertyByKey(prop);
          if (eprop == null || aconf.isPropertySet(eprop, true)
              || !isDeprecatedThroughputSet(aconf)) {
            rateLimits.put(tokens[0], ConfigurationTypeHelper.getFixedMemoryAsBytes(val));
          }
        } else {
          throw new IllegalArgumentException("Malformed compaction service property " + prop);
        }
      });

      defaultRateLimit = getDefaultThroughput(aconf);

      var diff = Sets.difference(options.keySet(), planners.keySet());

      if (!diff.isEmpty()) {
        throw new IllegalArgumentException(
            "Incomplete compaction service definitions, missing planner class " + diff);
      }

    }

    @SuppressWarnings("removal")
    private boolean isDeprecatedThroughputSet(AccumuloConfiguration aconf) {
      return aconf.isPropertySet(Property.TSERV_MAJC_THROUGHPUT, true);
    }

    public long getRateLimit(String serviceName) {
      return rateLimits.getOrDefault(serviceName, defaultRateLimit);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Config) {
        var oc = (Config) o;
        return planners.equals(oc.planners) && options.equals(oc.options)
            && rateLimits.equals(oc.rateLimits);
      }

      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(planners, options, rateLimits);
    }
  }

  private void mainLoop() {
    long lastCheckAllTime = System.nanoTime();

    long increment = Math.max(1, maxTimeBetweenChecks / 10);

    var retryFactory = Retry.builder().infiniteRetries()
        .retryAfter(increment, TimeUnit.MILLISECONDS).incrementBy(increment, TimeUnit.MILLISECONDS)
        .maxWait(maxTimeBetweenChecks, TimeUnit.MILLISECONDS).backOffFactor(1.07)
        .logInterval(1, TimeUnit.MINUTES).createFactory();
    var retry = retryFactory.createRetry();
    Compactable last = null;

    while (true) {
      try {
        long passed = TimeUnit.MILLISECONDS.convert(System.nanoTime() - lastCheckAllTime,
            TimeUnit.NANOSECONDS);
        if (passed >= maxTimeBetweenChecks) {
          for (Compactable compactable : compactables) {
            last = compactable;
            compact(compactable);
          }
          lastCheckAllTime = System.nanoTime();
        } else {
          var compactable =
              compactablesToCheck.poll(maxTimeBetweenChecks - passed, TimeUnit.MILLISECONDS);
          if (compactable != null) {
            last = compactable;
            compact(compactable);
          }
        }

        last = null;
        if (retry.hasRetried())
          retry = retryFactory.createRetry();

        checkForConfigChanges();

      } catch (Exception e) {
        var extent = last == null ? null : last.getExtent();
        log.warn("Failed to compact {} ", extent, e);
        retry.useRetry();
        try {
          retry.waitForNextAttempt();
        } catch (InterruptedException e1) {
          log.debug("Retry interrupted", e1);
        }
      }
    }
  }

  private void compact(Compactable compactable) {
    for (CompactionKind ctype : CompactionKind.values()) {
      var csid = compactable.getConfiguredService(ctype);
      var service = services.get(csid);
      if (service == null) {
        log.error(
            "Tablet {} returned non existant compaction service {} for compaction type {}.  Check"
                + " the table compaction dispatcher configuration. Attempting to fall back to "
                + "{} service.",
            compactable.getExtent(), csid, ctype, DEFAULT_SERVICE);

        service = services.get(DEFAULT_SERVICE);
      }

      if (service != null) {
        service.compact(ctype, compactable, compactablesToCheck::add);
      }
    }
  }

  public CompactionManager(Iterable<Compactable> compactables, ServerContext ctx,
      CompactionExecutorsMetrics ceMetrics) {
    this.compactables = compactables;

    this.currentCfg = new Config(ctx.getConfiguration());

    this.ctx = ctx;

    this.ceMetrics = ceMetrics;

    Map<CompactionServiceId,CompactionService> tmpServices = new HashMap<>();

    currentCfg.planners.forEach((serviceName, plannerClassName) -> {
      try {
        tmpServices.put(CompactionServiceId.of(serviceName),
            new CompactionService(serviceName, plannerClassName,
                currentCfg.getRateLimit(serviceName),
                currentCfg.options.getOrDefault(serviceName, Map.of()), ctx, ceMetrics));
      } catch (RuntimeException e) {
        log.error("Failed to create compaction service {} with planner:{} options:{}", serviceName,
            plannerClassName, currentCfg.options.getOrDefault(serviceName, Map.of()));
      }
    });

    this.services = Map.copyOf(tmpServices);

    this.maxTimeBetweenChecks = ctx.getConfiguration().getTimeInMillis(Property.TSERV_MAJC_DELAY);
  }

  public void compactableChanged(Compactable compactable) {
    compactablesToCheck.add(compactable);
  }

  private void checkForConfigChanges() {
    try {
      if (TimeUnit.SECONDS.convert(System.nanoTime() - lastConfigCheckTime, TimeUnit.NANOSECONDS)
          < 1)
        return;

      lastConfigCheckTime = System.nanoTime();

      var tmpCfg = new Config(ctx.getConfiguration());

      if (!currentCfg.equals(tmpCfg)) {
        Map<CompactionServiceId,CompactionService> tmpServices = new HashMap<>();

        tmpCfg.planners.forEach((serviceName, plannerClassName) -> {

          try {
            var csid = CompactionServiceId.of(serviceName);
            var service = services.get(csid);
            if (service == null) {
              tmpServices.put(csid,
                  new CompactionService(serviceName, plannerClassName,
                      tmpCfg.getRateLimit(serviceName),
                      tmpCfg.options.getOrDefault(serviceName, Map.of()), ctx, ceMetrics));
            } else {
              service.configurationChanged(plannerClassName, tmpCfg.getRateLimit(serviceName),
                  tmpCfg.options.getOrDefault(serviceName, Map.of()));
              tmpServices.put(csid, service);
            }
          } catch (RuntimeException e) {
            throw new RuntimeException("Failed to create or update compaction service "
                + serviceName + " with planner:" + plannerClassName + " options:"
                + tmpCfg.options.getOrDefault(serviceName, Map.of()), e);
          }
        });

        var deletedServices =
            Sets.difference(currentCfg.planners.keySet(), tmpCfg.planners.keySet());

        for (String serviceName : deletedServices) {
          services.get(CompactionServiceId.of(serviceName)).stop();
        }

        this.services = Map.copyOf(tmpServices);
      }
    } catch (RuntimeException e) {
      log.error("Failed to reconfigure compaction services ", e);
    }
  }

  public void start() {
    log.debug("Started compaction manager");
    new NamingThreadFactory("Compaction Manager").newThread(() -> mainLoop()).start();
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
    return services.values().stream().mapToInt(CompactionService::getCompactionsRunning).sum();
  }

  public int getCompactionsQueued() {
    return services.values().stream().mapToInt(CompactionService::getCompactionsQueued).sum();
  }
}
