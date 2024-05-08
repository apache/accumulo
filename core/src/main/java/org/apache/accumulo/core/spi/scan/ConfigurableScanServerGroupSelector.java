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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.TabletId;

import com.google.common.hash.HashCode;

public class ConfigurableScanServerGroupSelector extends ConfigurableScanServerSelector {

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
  protected int hashTabletsToServers(SelectorParameters params, Profile profile,
      List<String> orderedScanServers, Map<TabletId,String> serversToUse) {

    // orderedScanServers is the set of ScanServers addresses (host:port)
    // for the resource group designated for the profile being used for
    // this scan. We want to group these scan servers by hostname and
    // hash the tablet to the hostname, then randomly pick one of the
    // scan servers in that group.

    final Map<String,List<String>> scanServersByHost = new HashMap<>();
    for (String hostPort : orderedScanServers) {
      String host = hostPort.split(":")[0];
      scanServersByHost.putIfAbsent(host, new ArrayList<String>()).add(hostPort);
    }
    final List<String> hostIndex = new ArrayList<>(scanServersByHost.keySet());

    final int numberOfPreviousAttempts = params.getTablets().stream()
        .mapToInt(tablet -> params.getAttempts(tablet).size()).max().orElse(0);

    final int numServersToUseInAttemptPlan =
        profile.getNumServers(numberOfPreviousAttempts, orderedScanServers.size());

    for (TabletId tablet : params.getTablets()) {

      boolean scanServerFound = false;
      Collection<? extends ScanServerAttempt> priorAttempts = params.getAttempts(tablet);
      if (priorAttempts != null && priorAttempts.size() != 0) {
        // Sort the prior attempts by the number of scan servers tried in the list
        // for each host. In theory the server at the top of the list either has
        // scan servers remaining on that host, or has tried them all.
        Map<String,PriorHostServers> priorServers = new HashMap<>();
        priorAttempts.forEach(ssa -> {
          String priorServer = ssa.getServer();
          String priorHost = priorServer.split(":")[0];
          priorServers.putIfAbsent(priorHost, new PriorHostServers(priorHost)).getPriorServers()
              .add(priorServer);
        });
        List<PriorHostServers> priors = new ArrayList<>(priorServers.values());
        // sort after populating
        Collections.sort(priors, new PriorHostServersComparator());

        for (PriorHostServers phs : priors) {
          Set<String> scanServersOnPriorHost =
              new HashSet<>(scanServersByHost.get(phs.getPriorHost()));
          scanServersOnPriorHost.removeAll(phs.getPriorServers());
          if (scanServersOnPriorHost.size() > 0) {
            serversToUse.put(tablet, scanServersOnPriorHost.iterator().next());
            scanServerFound = true;
            break;
          }
        }
      }

      if (!scanServerFound) {
        // No prior attempts. Hash the tablet to a host, then pick the first scan server on
        // that host
        final HashCode hashCode = hashTablet(tablet, profile.getSalt(numberOfPreviousAttempts));
        final int serverIndex =
            (Math.abs(hashCode.asInt()) + RANDOM.nextInt(numServersToUseInAttemptPlan))
                % scanServersByHost.size();
        final String hostToUse = hostIndex.get(serverIndex);
        final List<String> scanServersOnHost = scanServersByHost.get(hostToUse);
        serversToUse.put(tablet, scanServersOnHost.get(0));
      }

    }
    return numberOfPreviousAttempts;

  }

}
