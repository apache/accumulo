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

import static org.apache.accumulo.core.spi.scan.RendezvousHasher.Mode.HOST;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.util.HostAndPort;

/**
 * Extension of the {@link ConfigurableScanServerSelector} that can be used when there are multiple
 * ScanServers running on the same host and for some reason, like using a shared off-heap cache,
 * sending scans for the same tablet to the same host may provide a better experience.
 *
 * <p>
 * This implementation will use rendezvous hashing to map a tablet to one or more hosts. Then it will
 * randomly pick one of those hosts and then randomly pick a scan server on that host. Scan servers
 * that have not had previous failures are chosen first.
 *
 * <p>
 * This implementation uses the same configuration as {@link ConfigurableScanServerSelector} but
 * interprets {@code attemptPlans} differently in two ways. First for the server count it will use
 * that to determine how many hosts to choose from. Second it will only consider an attempt plan as
 * failed when servers on all host have failed.
 *
 * <pre>
 *       "attemptPlans":[
 *           {"servers":"1", "busyTimeout":"10s"},
 *           {"servers":"3", "busyTimeout":"30s"},
 *        ]
 * </pre>
 *
 * <p>
 * For example given the above attempt plan configuration, the following sequence of events could
 * happen a tablet.
 *
 * <ol>
 * <li>host32 is chosen and it has 2 severs [host32:1000,host32:1001]. host32:1001 is randomly
 * chosen.</li>
 * <li>Scan on host32:1001 has a busy timeout.</li>
 * <li>host32 is chosen again because not all servers have failed. For the server, host32:1000 is
 * chosen because host32:1001 failed.
 * <li>Scan on host32:1000 has a busy timeout.</li>
 * <li>Because all servers on host32 have failed, move to the next attemptPlan and choose from three
 * host. host32,host40, and host09 are candidates and host09 is randomly picked, then a server from
 * host09:1000 is randomly picked
 * <li>The scan on host09:1000 succeeds.
 * </ol>
 *
 * <p>
 * This behavior will randomly spread scans against a single tablet across all servers on a host.
 * For example 100 clients are scanning 1 tablet that maps to 1 host with 5 servers, then those
 * scans will evenly spread across the 5 servers on the host.
 *
 */
public class ConfigurableScanServerHostSelector extends ConfigurableScanServerSelector {

  /**
   * @return map of previous failure keyed on host name with a set of servers per host
   */
  Map<String,Set<String>> computeFailuresByHost(TabletId tablet, SelectorParameters params) {
    var attempts = params.getAttempts(tablet);
    if (attempts.isEmpty()) {
      return Map.of();
    }

    Map<String,Set<String>> previousFailures = new HashMap<>();
    for (var attempt : attempts) {
      var hp = HostAndPort.fromString(attempt.getServer());
      previousFailures.computeIfAbsent(hp.getHost(), h -> new HashSet<>()).add(attempt.getServer());
    }

    return previousFailures;
  }

  List<String> removeFailedHost(String group, Map<String,Set<String>> prevFailures,
      List<String> rendezvousHosts, ScanServersSnapshot serversSnapshot) {
    if (prevFailures.isEmpty()) {
      return rendezvousHosts;
    }

    // filter out hosts where all servers failed
    List<String> availableHost = new ArrayList<>(rendezvousHosts.size());
    for (String host : rendezvousHosts) {
      var hostsFailures = prevFailures.getOrDefault(host, Set.of());
      if (hostsFailures.isEmpty()) {
        availableHost.add(host);
      } else {
        var hostsServers = serversSnapshot.getServersForHost(group, host);
        if (!hostsFailures.containsAll(hostsServers)) {
          // this host has some servers that did not fail
          availableHost.add(host);
        }
      }
    }

    return availableHost;
  }

  private List<String> removeFailedServers(Set<String> failedServers,
      List<String> scanServersOnHost) {
    if (failedServers.isEmpty()) {
      return scanServersOnHost;
    }
    return scanServersOnHost.stream().filter(s -> !failedServers.contains(s))
        .collect(Collectors.toList());
  }

  /**
   * Finds the set of scan servers to use for a given attempt. If all servers have previously failed
   * for all host this will return an empty list.
   *
   */
  private List<String> getServersForHostAttempt(int hostAttempt, TabletId tablet, Profile profile,
      RendezvousHasher rhasher, Map<String,Set<String>> prevFailures) {
    final var snapshot = rhasher.getSnapshot();
    final int numHostToUse =
        profile.getNumServers(hostAttempt, snapshot.getHostsForGroup(profile.group).size());
    List<String> rendezvousHosts =
        rhasher.rendezvous(HOST, profile.group, tablet, profile.getSalt(hostAttempt), numHostToUse);
    rendezvousHosts = removeFailedHost(profile.group, prevFailures, rendezvousHosts, snapshot);
    if (rendezvousHosts.isEmpty()) {
      return List.of();
    }
    var hostToUse = rendezvousHosts.get(RANDOM.nextInt(rendezvousHosts.size()));
    List<String> hostServers = snapshot.getServersForHost(profile.group, hostToUse);
    return removeFailedServers(prevFailures.getOrDefault(hostToUse, Set.of()), hostServers);
  }

  @Override
  int selectServers(SelectorParameters params, Profile profile, RendezvousHasher rhasher,
      Map<TabletId,String> serversToUse) {

    int maxHostAttempt = 0;

    for (TabletId tablet : params.getTablets()) {
      Map<String,Set<String>> prevFailures = computeFailuresByHost(tablet, params);

      for (int hostAttempt = 0; hostAttempt < profile.getAttemptPlans().size(); hostAttempt++) {
        maxHostAttempt = Math.max(hostAttempt, maxHostAttempt);
        List<String> scanServers =
            getServersForHostAttempt(hostAttempt, tablet, profile, rhasher, prevFailures);
        if (!scanServers.isEmpty()) {
          String serverToUse = scanServers.get(RANDOM.nextInt(scanServers.size()));
          serversToUse.put(tablet, serverToUse);
          break;
        }
      }

      if (!serversToUse.containsKey(tablet) && !prevFailures.isEmpty()) {
        // assuming no servers were found because all servers have previously failed, so in the
        // case were all servers have previous failed ignore previous failures and try any server
        List<String> scanServers = getServersForHostAttempt(profile.getAttemptPlans().size() - 1,
            tablet, profile, rhasher, Map.of());
        if (!scanServers.isEmpty()) {
          String serverToUse = scanServers.get(RANDOM.nextInt(scanServers.size()));
          serversToUse.put(tablet, serverToUse);
        }
      }
    }

    return maxHostAttempt;
  }
}
