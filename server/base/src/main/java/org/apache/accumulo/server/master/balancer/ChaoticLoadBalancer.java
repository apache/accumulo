/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.master.balancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletMigration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A chaotic load balancer used for testing. It constantly shuffles tablets, preventing them from resting in a single location for very long. This is not
 * designed for performance, do not use on production systems. I'm calling it the LokiLoadBalancer.
 *
 * <p>
 * Will balance randomly, maintaining distribution
 */
public class ChaoticLoadBalancer extends TabletBalancer {
  private static final Logger log = LoggerFactory.getLogger(ChaoticLoadBalancer.class);

  @SuppressWarnings("unused")
  private final String tableName;

  public ChaoticLoadBalancer() {
    this.tableName = null;
  }

  // Required constructor
  public ChaoticLoadBalancer(String tableName) {
    this.tableName = tableName;
  }

  Random r = new Random();

  @Override
  public void getAssignments(SortedMap<TServerInstance,TabletServerStatus> current, Map<KeyExtent,TServerInstance> unassigned,
      Map<KeyExtent,TServerInstance> assignments) {
    long total = assignments.size() + unassigned.size();
    long avg = (long) Math.ceil(((double) total) / current.size());
    Map<TServerInstance,Long> toAssign = new HashMap<>();
    List<TServerInstance> tServerArray = new ArrayList<>();
    for (Entry<TServerInstance,TabletServerStatus> e : current.entrySet()) {
      long numTablets = 0;
      for (TableInfo ti : e.getValue().getTableMap().values()) {
        numTablets += ti.tablets;
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

    for (KeyExtent ke : unassigned.keySet()) {
      int index = r.nextInt(tServerArray.size());
      TServerInstance dest = tServerArray.get(index);
      assignments.put(ke, dest);
      long remaining = toAssign.get(dest).longValue() - 1;
      if (remaining == 0) {
        tServerArray.remove(index);
        toAssign.remove(dest);
      } else {
        toAssign.put(dest, remaining);
      }
    }
  }

  protected final OutstandingMigrations outstandingMigrations = new OutstandingMigrations(log);

  @Override
  public long balance(SortedMap<TServerInstance,TabletServerStatus> current, Set<KeyExtent> migrations, List<TabletMigration> migrationsOut) {
    Map<TServerInstance,Long> numTablets = new HashMap<>();
    List<TServerInstance> underCapacityTServer = new ArrayList<>();

    if (!migrations.isEmpty()) {
      outstandingMigrations.migrations = migrations;
      constraintNotMet(outstandingMigrations);
      return 100;
    }
    resetBalancerErrors();

    boolean moveMetadata = r.nextInt(4) == 0;
    long totalTablets = 0;
    for (Entry<TServerInstance,TabletServerStatus> e : current.entrySet()) {
      long tabletCount = 0;
      for (TableInfo ti : e.getValue().getTableMap().values()) {
        tabletCount += ti.tablets;
      }
      numTablets.put(e.getKey(), tabletCount);
      underCapacityTServer.add(e.getKey());
      totalTablets += tabletCount;
    }
    // totalTablets is fuzzy due to asynchronicity of the stats
    // *1.2 to handle fuzziness, and prevent locking for 'perfect' balancing scenarios
    long avg = (long) Math.ceil(((double) totalTablets) / current.size() * 1.2);

    for (Entry<TServerInstance,TabletServerStatus> e : current.entrySet()) {
      for (String tableId : e.getValue().getTableMap().keySet()) {
        Table.ID id = Table.ID.of(tableId);
        if (!moveMetadata && MetadataTable.ID.equals(id))
          continue;
        try {
          for (TabletStats ts : getOnlineTabletsForTable(e.getKey(), id)) {
            KeyExtent ke = new KeyExtent(ts.extent);
            int index = r.nextInt(underCapacityTServer.size());
            TServerInstance dest = underCapacityTServer.get(index);
            if (dest.equals(e.getKey()))
              continue;
            migrationsOut.add(new TabletMigration(ke, e.getKey(), dest));
            if (numTablets.put(dest, numTablets.get(dest) + 1) > avg)
              underCapacityTServer.remove(index);
            if (numTablets.put(e.getKey(), numTablets.get(e.getKey()) - 1) <= avg && !underCapacityTServer.contains(e.getKey()))
              underCapacityTServer.add(e.getKey());

            // We can get some craziness with only 1 tserver, so lets make sure there's always an option!
            if (underCapacityTServer.isEmpty())
              underCapacityTServer.addAll(numTablets.keySet());
          }
        } catch (ThriftSecurityException e1) {
          // Shouldn't happen, but carry on if it does
          log.debug("Encountered ThriftSecurityException.  This should not happen.  Carrying on anyway.", e1);
        } catch (TException e1) {
          // Shouldn't happen, but carry on if it does
          log.debug("Encountered TException.  This should not happen.  Carrying on anyway.", e1);
        }
      }
    }

    return 100;
  }

}
