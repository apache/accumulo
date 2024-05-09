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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.TabletId;

import com.google.common.hash.HashCode;
import com.google.common.net.HostAndPort;

/**
 * Extension of the {@code ConfigurableScanServerSelector} that can be used when there are multiple
 * ScanServers running on the same host and for some reason, like using a shared off-heap cache,
 * sending scans for the same tablet to the same host may provide a better experience.
 *
 * This implementation will initially hash a Tablet to a ScanServer. If the ScanServer is unable to
 * execute the scan, this implementation will try to send the scan to a ScanServer on the same host.
 * If there are no more ScanServers to try on that host, then it will fall back to trying a
 * different host and the process repeats.
 *
 */
public class ConfigurableScanServerHostSelector extends ConfigurableScanServerSelector {

  private static final class PriorHostServersComparator implements Comparator<PriorHostServers> {

    @Override
    public int compare(PriorHostServers o1, PriorHostServers o2) {
      return Integer.compare(o1.getPriorServers().size(), o2.getPriorServers().size());
    }

  }

  private static final class PriorHostServers {
    private final String priorHost;
    private final List<String> priorServers = new ArrayList<>();

    public PriorHostServers(String priorHost) {
      this.priorHost = priorHost;
    }

    public String getPriorHost() {
      return priorHost;
    }

    public List<String> getPriorServers() {
      return priorServers;
    }
  }

  @Override
  protected int selectServers(SelectorParameters params, Profile profile,
      List<String> orderedScanServers, Map<TabletId,String> serversToUse) {

    // orderedScanServers is the set of ScanServers addresses (host:port)
    // for the resource group designated for the profile being used for
    // this scan. We want to group these scan servers by hostname and
    // hash the tablet to the hostname, then randomly pick one of the
    // scan servers in that group.

    final Map<String,List<String>> scanServerHosts = new HashMap<>();
    for (final String address : orderedScanServers) {
      final HostAndPort hp = HostAndPort.fromString(address);
      scanServerHosts.computeIfAbsent(hp.getHost(), (k) -> {
        return new ArrayList<String>();
      }).add(address);
    }
    final List<String> hostIndex = new ArrayList<>(scanServerHosts.keySet());

    final int numberOfPreviousAttempts = params.getTablets().stream()
        .mapToInt(tablet -> params.getAttempts(tablet).size()).max().orElse(0);

    final int numServersToUseInAttemptPlan =
        profile.getNumServers(numberOfPreviousAttempts, orderedScanServers.size());

    for (TabletId tablet : params.getTablets()) {

      boolean scanServerFound = false;
      if (numberOfPreviousAttempts > 0) {
        // Sort the prior attempts by the number of scan servers tried in the list
        // for each host. In theory the server at the top of the list either has
        // scan servers remaining on that host, or has tried them all.
        final Map<String,PriorHostServers> priorServers = new HashMap<>(numberOfPreviousAttempts);
        params.getAttempts(tablet).forEach(ssa -> {
          final String priorServerAddress = ssa.getServer();
          final HostAndPort priorHP = HostAndPort.fromString(priorServerAddress);
          priorServers.computeIfAbsent(priorHP.getHost(), (k) -> {
            return new PriorHostServers(priorHP.getHost());
          }).getPriorServers().add(priorServerAddress);
        });
        final List<PriorHostServers> priors = new ArrayList<>(priorServers.values());
        // sort after populating
        Collections.sort(priors, new PriorHostServersComparator());

        for (PriorHostServers phs : priors) {
          final Set<String> scanServersOnPriorHost =
              new HashSet<>(scanServerHosts.get(phs.getPriorHost()));
          scanServersOnPriorHost.removeAll(phs.getPriorServers());
          if (scanServersOnPriorHost.size() > 0) {
            serversToUse.put(tablet, scanServersOnPriorHost.iterator().next());
            scanServerFound = true;
            break;
          }
        }
        // If we get here, then we were unable to find a host with a ScanServer that
        // we did not try. Remove the hosts from the hostIndex.
        for (PriorHostServers phs : priors) {
          hostIndex.remove(phs.getPriorHost());
        }
      }

      if (!scanServerFound) {
        if (hostIndex.size() == 0) {
          // We tried all servers
          serversToUse.put(tablet, null);
        } else {
          final HashCode hashCode = hashTablet(tablet, profile.getSalt(numberOfPreviousAttempts));
          final int serverIndex =
              (Math.abs(hashCode.asInt()) + RANDOM.nextInt(numServersToUseInAttemptPlan))
                  % hostIndex.size();
          final String hostToUse = hostIndex.get(serverIndex);
          final List<String> scanServersOnHost = scanServerHosts.get(hostToUse);
          serversToUse.put(tablet, scanServersOnHost.get(0));
        }
      }

    }
    return numberOfPreviousAttempts;

  }

}
