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
package org.apache.accumulo.test;

import static org.apache.accumulo.harness.AccumuloITBase.random;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.spi.balancer.BalancerEnvironment;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TableStatistics;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.spi.balancer.data.TabletStatistics;
import org.apache.accumulo.core.spi.balancer.util.ThrottledBalancerProblemReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A chaotic load balancer used for testing. It constantly shuffles tablets, preventing them from
 * resting in a single location for very long. This is not designed for performance, do not use on
 * production systems. I'm calling it the LokiLoadBalancer.
 *
 * <p>
 * Will balance randomly, maintaining distribution
 */
public class ChaoticLoadBalancer implements TabletBalancer {
  private static final Logger log = LoggerFactory.getLogger(ChaoticLoadBalancer.class);

  protected BalancerEnvironment environment;

  public ChaoticLoadBalancer() {}

  @Override
  public void init(BalancerEnvironment balancerEnvironment) {
    this.environment = balancerEnvironment;
  }

  @Override
  public void getAssignments(AssignmentParameters params) {
    long total = params.unassignedTablets().size();
    long avg = (long) Math.ceil(((double) total) / params.currentStatus().size());
    Map<TabletServerId,Long> toAssign = new HashMap<>();
    List<TabletServerId> tServerArray = new ArrayList<>();
    for (Entry<TabletServerId,TServerStatus> e : params.currentStatus().entrySet()) {
      long numTablets = 0;
      for (TableStatistics ti : e.getValue().getTableMap().values()) {
        numTablets += ti.getTabletCount();
      }
      if (numTablets <= avg) {
        tServerArray.add(e.getKey());
        toAssign.put(e.getKey(), avg - numTablets);
      }
    }

    if (tServerArray.isEmpty()) {
      // No tservers to assign to
      return;
    }

    for (TabletId tabletId : params.unassignedTablets().keySet()) {
      int index = random.nextInt(tServerArray.size());
      TabletServerId dest = tServerArray.get(index);
      params.addAssignment(tabletId, dest);
      long remaining = toAssign.get(dest) - 1;
      if (remaining == 0) {
        tServerArray.remove(index);
        toAssign.remove(dest);
      } else {
        toAssign.put(dest, remaining);
      }
    }
  }

  private final ThrottledBalancerProblemReporter problemReporter =
      new ThrottledBalancerProblemReporter(getClass());
  private final ThrottledBalancerProblemReporter.OutstandingMigrationsProblem outstandingMigrationsProblem =
      problemReporter.createOutstandingMigrationsProblem();

  @Override
  public long balance(BalanceParameters params) {
    Map<TabletServerId,Long> numTablets = new HashMap<>();
    List<TabletServerId> underCapacityTServer = new ArrayList<>();

    if (!params.currentMigrations().isEmpty()) {
      outstandingMigrationsProblem.setMigrations(params.currentMigrations());
      problemReporter.reportProblem(outstandingMigrationsProblem);
      return 100;
    }
    problemReporter.clearProblemReportTimes();

    boolean moveMetadata = random.nextInt(4) == 0;
    long totalTablets = 0;
    for (Entry<TabletServerId,TServerStatus> e : params.currentStatus().entrySet()) {
      long tabletCount = 0;
      for (TableStatistics ti : e.getValue().getTableMap().values()) {
        tabletCount += ti.getTabletCount();
      }
      numTablets.put(e.getKey(), tabletCount);
      underCapacityTServer.add(e.getKey());
      totalTablets += tabletCount;
    }
    // totalTablets is fuzzy due to asynchronicity of the stats
    // *1.2 to handle fuzziness, and prevent locking for 'perfect' balancing scenarios
    long avg = (long) Math.ceil(((double) totalTablets) / params.currentStatus().size() * 1.2);

    for (Entry<TabletServerId,TServerStatus> e : params.currentStatus().entrySet()) {
      for (String tableId : e.getValue().getTableMap().keySet()) {
        TableId id = TableId.of(tableId);
        if (!moveMetadata && MetadataTable.ID.equals(id)) {
          continue;
        }
        try {
          for (TabletStatistics ts : getOnlineTabletsForTable(e.getKey(), id)) {
            int index = random.nextInt(underCapacityTServer.size());
            TabletServerId dest = underCapacityTServer.get(index);
            if (dest.equals(e.getKey())) {
              continue;
            }
            params.migrationsOut().add(new TabletMigration(ts.getTabletId(), e.getKey(), dest));
            if (numTablets.put(dest, numTablets.get(dest) + 1) > avg) {
              underCapacityTServer.remove(index);
            }
            if (numTablets.put(e.getKey(), numTablets.get(e.getKey()) - 1) <= avg
                && !underCapacityTServer.contains(e.getKey())) {
              underCapacityTServer.add(e.getKey());
            }

            // We can get some craziness with only 1 tserver, so lets make sure there's always an
            // option!
            if (underCapacityTServer.isEmpty()) {
              underCapacityTServer.addAll(numTablets.keySet());
            }
          }
        } catch (AccumuloSecurityException e1) {
          // Shouldn't happen, but carry on if it does
          log.debug(
              "Encountered AccumuloSecurityException.  This should not happen.  Carrying on anyway.",
              e1);
        } catch (AccumuloException e1) {
          // Shouldn't happen, but carry on if it does
          log.debug("Encountered AccumuloException.  This should not happen.  Carrying on anyway.",
              e1);
        }
      }
    }

    return 100;
  }

  protected List<TabletStatistics> getOnlineTabletsForTable(TabletServerId tabletServerId,
      TableId tableId) throws AccumuloException, AccumuloSecurityException {
    return environment.listOnlineTabletsForTable(tabletServerId, tableId);
  }

}
