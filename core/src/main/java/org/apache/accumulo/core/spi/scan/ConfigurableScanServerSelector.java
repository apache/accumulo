```java
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
package org.apache.accumulo.core.spi.scan;

import static org.apache.accumulo.core.spi.scan.RendezvousHasher.Mode.SERVER;
import static org.apache.accumulo.core.util.LazySingletons.GSON;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * The default Accumulo selector for scan servers. This dispatcher will :
 *
 * <ul>
 * <li>Hash each tablet to a per attempt configurable number of scan servers and then randomly
 * choose one of those scan servers. Using hashing allows different client to select the same scan
 * servers for a given tablet.</li>
 * <li>Use a per attempt configurable busy timeout.</li>
 * </ul>
 *
 * <p>
 * This class accepts a single configuration that has a json value. To configure this class set
 * {@code scan.server.selector.opts.profiles=<json>} in the accumulo client configuration along with
 * the config for the class. The following is the default configuration value.
 * </p>
 * <p>
 * {@value ConfigurableScanServerSelector#PROFILES_DEFAULT}
 * </p>
 *
 * The json is structured as a list of profiles, with each profile having the following fields.
 *
 * <ul>
 * <li><b>isDefault : </b> A boolean that specifies whether this is the default profile. One and
 * only one profile must set this to true.</li>
 * <li><b>maxBusyTimeout : </b> The maximum busy timeout to use. The busy timeout from the last
 * attempt configuration grows exponentially up to this max.</li>
 * <li><b>scanTypeActivations : </b> A list of scan types that will activate this profile. Scan
 * types are specified by setting {@code scan_type=<scan_type>} as execution on the scanner. See
 * {@link org.apache.accumulo.core.client.ScannerBase#setExecutionHints(Map)}</li>
 * <li><b>group : </b> Scan servers can be started with an optional group. If specified, this option
 * will limit the scan servers used to those that were started with this group name. If not
 * specified, the set of scan servers that did not specify a group will be used. Grouping scan
 * servers supports at least two use cases. First groups can be used to dedicate resources for
 * certain scans. Second groups can be used to have different hardware/VM types for scans, for
 * example could have some scans use expensive high memory VMs and others use cheaper burstable
 * VMs.</li>
 * <li><b>timeToWaitForScanServers : </b> When there are no scans servers, this setting determines
 * how long to wait for scan servers to become available before falling back to tablet servers.
 * Falling back to tablet servers may cause tablets to be loaded that are not currently loaded. When
 * this setting is given a wait time and there are no scan servers, it will wait for scan servers to
 * be available. This setting avoids loading tablets on tablet servers when scans servers are
 * temporarily unavailable which could be caused by normal cluster activity. You can specify the
 * wait time using different units to precisely control the wait duration. The supported units are:
 * <ul>
 * <li>"d" for days</li>
 * <li>"h" for hours</li>
 * <li>"m" for minutes</li>
 * <li>"s" for seconds</li>
 * <li>"ms" for milliseconds</li>
 * </ul>
 * If duration is not specified this setting defaults to 100 years, which for all practical purposes
 * will never fall back to tablet servers. Waiting for scan servers is done via
 * {@link org.apache.accumulo.core.spi.scan.ScanServerSelector.SelectorParameters#waitUntil(Supplier, Duration, String)}.
 * To immediately fall back to tablet servers when no scan servers are present set this to
 * zero.</li>
 * <li><b>attemptPlans : </b> A list of configuration to use for each scan attempt. Each list object
 * has the following fields:
 * <ul>
 * <li><b>servers : </b> The number of servers to randomly choose from for this attempt.</li>
 * <li><b>busyTimeout : </b> The busy timeout to use for this attempt.</li>
 * <li><b>salt : </b> An optional string to append when hashing the tablet. When this is set
 * differently for attempts it has the potential to cause the set of servers chosen from to be
 * disjoint. When not set or the same, the servers between attempts will be subsets.</li>
 * </ul>
 * </li>
 * </ul>
 *
 * <p>
 * Below is an example configuration with two profiles, one is the default and the other is used
 * when the scan execution hint {@code scan_type=slow} is set.
 * </p>
 *
 * <pre>
 *    [
 *     {
 *       "isDefault":true,
 *       "maxBusyTimeout":"5m",
 *       "busyTimeoutMultiplier":4,
 *       "attemptPlans":[
 *         {"servers":"3", "busyTimeout":"33ms"},
 *         {"servers":"100%", "busyTimeout":"100ms"}
 *       ]
 *     },
 *     {
 *       "scanTypeActivations":["slow"],
 *       "maxBusyTimeout":"20m",
 *       "busyTimeoutMultiplier":8,
 *       "group":"lowcost",
 *       "timeToWaitForScanServers": "120s",
 *       "attemptPlans":[
 *         {"servers":"1", "busyTimeout":"10s"},
 *         {"servers":"3", "busyTimeout":"30s","salt":"42"},
 *         {"servers":"9", "busyTimeout":"60s","salt":"84"}
 *       ]
 *     }
 *    ]
 * </pre>
 *
 * <p>
 * For the default profile in the example it will start off by choosing randomly from 3 scan servers
 * based on a hash of the tablet with no salt. For the first attempt it will use a busy timeout of
 * 33 milliseconds. If the first attempt returns with busy, then it will randomly choose from 100%
 * or all servers for the second attempt and use a busy timeout of 100ms. For subsequent attempts it
 * will keep choosing from all servers and start multiplying the busy timeout by 4 until the max
 * busy timeout of 4 minutes is reached.
 * </p>
 *
 * <p>
 * For the profile activated by {@code scan_type=slow} it starts off by choosing randomly from 1
 * scan server based on a hash of the tablet with no salt and a busy timeout of 10s. The second
 * attempt will choose from 3 scan servers based on a hash of the tablet plus the salt
 * {@literal 42}. Without the salt, the single scan servers from the first attempt would always be
 * included in the set of 3. With the salt the single scan server from the first attempt may not be
 * included. The third attempt will choose a scan server from 9 using the salt {@literal 84} and a
 * busy timeout of 60s. The different salt means the set of servers that attempts 2 and 3 choose
 * from may be disjoint. Attempt 4 and greater will continue to choose from 9 servers and will
 * multiply the busy timeout by 8 from the 60s timeout until it reaches 20 minutes.
 * </p>
 */
@SuppressFBWarnings(value = "UWF_UNWRITTEN_FIELD", justification = "Fields set by GSON")
public class ConfigurableScanServerSelector implements ScanServerSelector {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurableScanServerSelector.class);

  static final String PROFILES_OPT = "profiles";

  static final String PROFILES_DEFAULT =
      "[{\"isDefault\":true,\"maxBusyTimeout\":\"4m\",\"busyTimeoutMultiplier\":4,\"attemptPlans\":[{\"servers\":\"3\",\"busyTimeout\":\"33ms\"},{\"servers\":\"100%\",\"busyTimeout\":\"100ms\"}]}]";

  private List<Profile> profiles;

  private Map<String, ResourceGroupId> serverToGroup;
  private Map<ResourceGroupId, Set<String>> groupToServers;

  @Override
  public void init(InitParams params) {
    var serverSet = params.getScanServers();
    serverToGroup = new HashMap<>();
    groupToServers = new HashMap<>();
    var ungrouped = new HashSet<String>();

    serverSet.forEach(server -> serverToGroup.put(server, null));
    groupToServers.put(null, ungrouped);

    var configured = Preconditions.checkNotNull(
        params.getServiceEnv().getConfiguration().get(PROFILES_OPT, PROFILES_DEFAULT),
        "profiles config is null");
    profiles = GSON.fromJson(configured, new TypeToken<List<Profile>>() {}.getType());

    var serverGroupConfig = params.getServiceEnv().getConfiguration().get("server.groups");

    if (serverGroupConfig != null) {
      var serverGroups =
          GSON.fromJson(serverGroupConfig, new TypeToken<Map<String, String>>() {}.getType());

      serverGroups.forEach((server, group) -> {
        if (!groupToServers.containsKey(group)) {
          groupToServers.put(group, new HashSet<>());
        }

        serverToGroup.put(server, group);
        groupToServers.get(group).add(server);
        ungrouped.remove(server);
      });
    }

    serverSet.forEach(server -> {
      var group = serverToGroup.get(server);
      if (group == null) {
        ungrouped.add(server);
      }
    });
  }

  private Profile selectProfile(SelectorParameters params) {
    var executionHints = params.getExecutionHints();

    if (executionHints != null && executionHints.containsKey("scan_type")) {
      String scanType = executionHints.get("scan_type");
      Optional<Profile> optProfile = profiles.stream()
          .filter(p -> p.scanTypeActivations != null
              && p.scanTypeActivations.contains(scanType))
          .findFirst();

      if (optProfile.isPresent()) {
        return optProfile.get();
      }
    }

    return profiles.stream().filter(p -> p.isDefault).findFirst()
        .orElseThrow(() -> new IllegalStateException("No default profile found"));
  }

  @Override
  public ScanServerSelections selectServers(SelectorParameters params) {
    Profile profile = selectProfile(params);

    var groupName = profile.group;
    Set<String> serversToUse = groupToServers.get(groupName);

    if (serversToUse == null || serversToUse.isEmpty()) {
      // Check if we should wait for servers to become available
      var timeToWait = profile.timeToWaitForScanServers;
      if (timeToWait != null && timeToWait.compareTo(Duration.ZERO) > 0) {
        var groupNameForMsg = groupName == null ? "ungrouped" : groupName;
        if (params.waitUntil(() -> {
          Set<String> servers = groupToServers.get(groupName);
          return servers != null && !servers.isEmpty();
        }, timeToWait, "Waiting for scan servers in group " + groupNameForMsg)) {
          serversToUse = groupToServers.get(groupName);
        }
      }

      if (serversToUse == null || serversToUse.isEmpty()) {
        return new ScanServerSelections() {
          @Override
          public String getScanServer(TabletId tabletId) {
            return null;
          }

          @Override
          public Duration getDelay() {
            return Duration.ZERO;
          }
        };
      }
    }

    Set<String> finalServersToUse = serversToUse;
    int maxAttempts = params.getTablets().stream()
        .mapToInt(tablet -> params.getAttempts(tablet).size()).max().orElse(0);

    Duration busyTO = Duration.ofMillis(profile.getBusyTimeout(maxAttempts));

    int maxErrorAttempts =
        params.getTablets().stream()
            .mapToInt(tablet -> (int) params.getAttempts(tablet).stream()
                .filter(a -> a.getResult() == ScanServerAttempt.Result.ERROR).count())
            .max().orElse(0);
    Duration delay = maxErrorAttempts == 0 ? Duration.ZERO
        : Duration.ofMillis(Math.min(5000L, 100L * (1L << Math.min(maxErrorAttempts - 1, 30))));

    LOG.trace("Returning servers to use: {}", finalServersToUse);
    return new ScanServerSelections() {
      @Override
      public String getScanServer(TabletId tabletId) {
        var server = RendezvousHasher.selectServer(tabletId, "", SERVER,
            profile.attemptPlans.get(Math.min(maxAttempts - 1, profile.attemptPlans.size() - 1)),
            finalServersToUse);
        return server;
      }

      @Override
      public Duration getDelay() {
        return delay;
      }

      @Override
      public Duration getBusyTimeout() {
        return busyTO;
      }
    };
  }

  static class Profile {

    boolean isDefault = false;
    List<String> scanTypeActivations;
    String group;
    String maxBusyTimeout = "5m";
    int busyTimeoutMultiplier = 4;
    Duration timeToWaitForScanServers;
    List<AttemptPlan> attemptPlans = new ArrayList<>();

    Duration getBusyTimeout(int attemptNum) {
      if (attemptNum <= 0) {
        throw new IllegalArgumentException("Attempt number must be positive");
      }

      if (attemptNum <= attemptPlans.size()) {
        return attemptPlans.get(attemptNum - 1).getBusyTimeout();
      }

      var lastPlan = attemptPlans.get(attemptPlans.size() - 1);
      var lastTO = lastPlan.getBusyTimeout();
      long multiplier = 1L;
      for (int i = attemptPlans.size(); i < attemptNum; i++) {
        multiplier *= busyTimeoutMultiplier;
      }

      long maxTO = ConfigurationTypeHelper.getTimeInMillis(maxBusyTimeout);
      long newTO = Math.min(lastTO.toMillis() * multiplier, maxTO);
      return Duration.ofMillis(newTO);
    }
  }

  static class AttemptPlan {
    String servers;
    Duration busyTimeout;
    String salt;

    Duration getBusyTimeout() {
      return busyTimeout;
    }

    String getSalt() {
      return salt == null ? "" : salt;
    }

    int getNumServers(int totalServers) {
      if (servers.endsWith("%")) {
        var percent = Integer.parseInt(servers.substring(0, servers.length() - 1));
        return Math.max(1, (totalServers * percent) / 100);
      } else {
        return Integer.parseInt(servers);
      }
    }
  }
}
```