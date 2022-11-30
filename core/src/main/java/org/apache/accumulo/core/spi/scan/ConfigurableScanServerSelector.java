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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.lang.reflect.Type;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.data.TabletId;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
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
 * example could have some scans use expensive high memory VMs and others use cheaper burstable VMs.
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
 * For the profile activated by {@code scan_type=slow} it start off by choosing randomly from 1 scan
 * server based on a hash of the tablet with no salt and a busy timeout of 10s. The second attempt
 * will choose from 3 scan servers based on a hash of the tablet plus the salt {@literal 42}.
 * Without the salt, the single scan servers from the first attempt would always be included in the
 * set of 3. With the salt the single scan server from the first attempt may not be included. The
 * third attempt will choose a scan server from 9 using the salt {@literal 84} and a busy timeout of
 * 60s. The different salt means the set of servers that attempts 2 and 3 choose from may be
 * disjoint. Attempt 4 and greater will continue to choose from the same 9 servers as attempt 3 and
 * will keep increasing the busy timeout by multiplying 8 until the maximum of 20 minutes is
 * reached. For this profile it will choose from scan servers in the group {@literal lowcost}.
 * </p>
 */
public class ConfigurableScanServerSelector implements ScanServerSelector {

  private static final SecureRandom RANDOM = new SecureRandom();
  public static final String PROFILES_DEFAULT = "[{'isDefault':true,'maxBusyTimeout':'5m',"
      + "'busyTimeoutMultiplier':8, 'scanTypeActivations':[], "
      + "'attemptPlans':[{'servers':'3', 'busyTimeout':'33ms', 'salt':'one'},"
      + "{'servers':'13', 'busyTimeout':'33ms', 'salt':'two'},"
      + "{'servers':'100%', 'busyTimeout':'33ms'}]}]";

  private Supplier<Map<String,List<String>>> orderedScanServersSupplier;

  private Map<String,Profile> profiles;
  private Profile defaultProfile;

  private static final Set<String> OPT_NAMES = Set.of("profiles");

  @SuppressFBWarnings(value = {"NP_UNWRITTEN_FIELD", "UWF_UNWRITTEN_FIELD"},
      justification = "Object deserialized by GSON")
  private static class AttemptPlan {
    String servers;
    String busyTimeout;
    String salt = "";

    transient double serversRatio;
    transient int parsedServers;
    transient boolean isServersPercent;
    transient boolean parsed = false;
    transient long parsedBusyTimeout;

    void parse() {
      if (parsed) {
        return;
      }

      if (servers.endsWith("%")) {
        // TODO check < 100
        serversRatio = Double.parseDouble(servers.substring(0, servers.length() - 1)) / 100.0;
        if (serversRatio < 0 || serversRatio > 1) {
          throw new IllegalArgumentException("Bad servers percentage : " + servers);
        }
        isServersPercent = true;
      } else {
        parsedServers = Integer.parseInt(servers);
        if (parsedServers <= 0) {
          throw new IllegalArgumentException("Server must be positive : " + servers);
        }
        isServersPercent = false;
      }

      parsedBusyTimeout = ConfigurationTypeHelper.getTimeInMillis(busyTimeout);

      parsed = true;
    }

    int getNumServers(int totalServers) {
      parse();
      if (isServersPercent) {
        return Math.max(1, (int) Math.round(serversRatio * totalServers));
      } else {
        return Math.min(totalServers, parsedServers);
      }
    }

    long getBusyTimeout() {
      parse();
      return parsedBusyTimeout;
    }
  }

  @SuppressFBWarnings(value = {"NP_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD", "UWF_UNWRITTEN_FIELD"},
      justification = "Object deserialized by GSON")
  private static class Profile {
    public List<AttemptPlan> attemptPlans;
    List<String> scanTypeActivations;
    boolean isDefault = false;
    int busyTimeoutMultiplier;
    String maxBusyTimeout;
    String group = ScanServerSelector.DEFAULT_SCAN_SERVER_GROUP_NAME;

    transient boolean parsed = false;
    transient long parsedMaxBusyTimeout;

    int getNumServers(int attempt, int totalServers) {
      int index = Math.min(attempt, attemptPlans.size() - 1);
      return attemptPlans.get(index).getNumServers(totalServers);
    }

    void parse() {
      if (parsed) {
        return;
      }
      parsedMaxBusyTimeout = ConfigurationTypeHelper.getTimeInMillis(maxBusyTimeout);
      parsed = true;
    }

    long getBusyTimeout(int attempt) {
      int index = Math.min(attempt, attemptPlans.size() - 1);
      long busyTimeout = attemptPlans.get(index).getBusyTimeout();
      if (attempt >= attemptPlans.size()) {
        parse();
        busyTimeout = (long) (busyTimeout
            * Math.pow(busyTimeoutMultiplier, attempt - attemptPlans.size() + 1));
        busyTimeout = Math.min(busyTimeout, parsedMaxBusyTimeout);
      }

      return busyTimeout;
    }

    public String getSalt(int attempts) {
      int index = Math.min(attempts, attemptPlans.size() - 1);
      return attemptPlans.get(index).salt;
    }
  }

  private void parseProfiles(Map<String,String> options) {
    Type listType = new TypeToken<ArrayList<Profile>>() {}.getType();
    Gson gson = new Gson();
    List<Profile> profList =
        gson.fromJson(options.getOrDefault("profiles", PROFILES_DEFAULT), listType);

    profiles = new HashMap<>();
    defaultProfile = null;

    for (Profile prof : profList) {
      if (prof.scanTypeActivations != null) {
        for (String scanType : prof.scanTypeActivations) {
          if (profiles.put(scanType, prof) != null) {
            throw new IllegalArgumentException(
                "Scan type activation seen in multiple profiles : " + scanType);
          }
        }
      }
      if (prof.isDefault) {
        if (defaultProfile != null) {
          throw new IllegalArgumentException("Multiple default profiles seen");
        }

        defaultProfile = prof;
      }

      if (defaultProfile == null) {
        throw new IllegalArgumentException("No default profile specified");
      }
    }
  }

  @Override
  public void init(ScanServerSelector.InitParameters params) {
    // avoid constantly resorting the scan servers, just do it periodically in case they change
    orderedScanServersSupplier = Suppliers.memoizeWithExpiration(() -> {
      Collection<ScanServerInfo> scanServers = params.getScanServers().get();
      Map<String,List<String>> groupedServers = new HashMap<>();
      scanServers.forEach(sserver -> groupedServers
          .computeIfAbsent(sserver.getGroup(), k -> new ArrayList<>()).add(sserver.getAddress()));
      groupedServers.values().forEach(ssAddrs -> Collections.sort(ssAddrs));
      return groupedServers;
    }, 100, TimeUnit.MILLISECONDS);

    var opts = params.getOptions();

    var diff = Sets.difference(opts.keySet(), OPT_NAMES);

    Preconditions.checkArgument(diff.isEmpty(), "Unknown options %s", diff);

    parseProfiles(params.getOptions());
  }

  @Override
  public ScanServerSelections selectServers(ScanServerSelector.SelectorParameters params) {

    String scanType = params.getHints().get("scan_type");

    Profile profile = null;

    if (scanType != null) {
      profile = profiles.getOrDefault(scanType, defaultProfile);
    } else {
      profile = defaultProfile;
    }

    // only get this once and use it for the entire method so that the method uses a consistent
    // snapshot
    List<String> orderedScanServers =
        orderedScanServersSupplier.get().getOrDefault(profile.group, List.of());

    if (orderedScanServers.isEmpty()) {
      return new ScanServerSelections() {
        @Override
        public String getScanServer(TabletId tabletId) {
          return null;
        }

        @Override
        public Duration getDelay() {
          return Duration.ZERO;
        }

        @Override
        public Duration getBusyTimeout() {
          return Duration.ZERO;
        }
      };
    }

    Map<TabletId,String> serversToUse = new HashMap<>();

    // get the max number of busy attempts, treat errors as busy attempts
    int attempts = params.getTablets().stream()
        .mapToInt(tablet -> params.getAttempts(tablet).size()).max().orElse(0);

    int numServers = profile.getNumServers(attempts, orderedScanServers.size());

    for (TabletId tablet : params.getTablets()) {

      String serverToUse = null;

      var hashCode = hashTablet(tablet, profile.getSalt(attempts));

      int serverIndex =
          (Math.abs(hashCode.asInt()) + RANDOM.nextInt(numServers)) % orderedScanServers.size();

      serverToUse = orderedScanServers.get(serverIndex);

      serversToUse.put(tablet, serverToUse);
    }

    Duration busyTO = Duration.ofMillis(profile.getBusyTimeout(attempts));

    return new ScanServerSelections() {
      @Override
      public String getScanServer(TabletId tabletId) {
        return serversToUse.get(tabletId);
      }

      @Override
      public Duration getDelay() {
        return Duration.ZERO;
      }

      @Override
      public Duration getBusyTimeout() {
        return busyTO;
      }
    };
  }

  private HashCode hashTablet(TabletId tablet, String salt) {
    var hasher = Hashing.murmur3_128().newHasher();

    if (tablet.getEndRow() != null) {
      hasher.putBytes(tablet.getEndRow().getBytes(), 0, tablet.getEndRow().getLength());
    } else {
      hasher.putByte((byte) 5);
    }

    if (tablet.getPrevEndRow() != null) {
      hasher.putBytes(tablet.getPrevEndRow().getBytes(), 0, tablet.getPrevEndRow().getLength());
    } else {
      hasher.putByte((byte) 7);
    }

    hasher.putString(tablet.getTable().canonical(), UTF_8);

    if (salt != null && !salt.isEmpty()) {
      hasher.putString(salt, UTF_8);
    }

    return hasher.hash();
  }
}
