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
package org.apache.accumulo.core.metadata;

import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum TabletState {
  UNASSIGNED, ASSIGNED, HOSTED, ASSIGNED_TO_DEAD_SERVER, SUSPENDED, NEEDS_REASSIGNMENT;

  private static Logger log = LoggerFactory.getLogger(TabletState.class);

  public static TabletState compute(TabletMetadata tm, Set<TServerInstance> liveTServers) {
    return compute(tm, liveTServers, null, null);
  }

  public static TabletState compute(TabletMetadata tm, Set<TServerInstance> liveTServers,
      TabletBalancer balancer, Map<TabletServerId,String> tserverGroups) {
    TabletMetadata.Location current = null;
    TabletMetadata.Location future = null;
    if (tm.hasCurrent()) {
      current = tm.getLocation();
    } else {
      future = tm.getLocation();
    }
    if (future != null) {
      return liveTServers.contains(future.getServerInstance()) ? TabletState.ASSIGNED
          : TabletState.ASSIGNED_TO_DEAD_SERVER;
    } else if (current != null) {
      if (liveTServers.contains(current.getServerInstance())) {
        if (balancer != null) {
          var tsii = new TabletServerIdImpl(current.getServerInstance());
          var resourceGroup = tserverGroups.get(tsii);

          if (resourceGroup != null) {
            var reassign = balancer.needsReassignment(new TabletBalancer.CurrentAssignment() {
              @Override
              public TabletId getTablet() {
                return new TabletIdImpl(tm.getExtent());
              }

              @Override
              public TabletServerId getTabletServer() {
                return tsii;
              }

              @Override
              public String getResourceGroup() {
                return resourceGroup;
              }
            });

            if (reassign) {
              return TabletState.NEEDS_REASSIGNMENT;
            }
          } else {
            // A tablet server should always have a resource group, however there is a race
            // conditions where the resource group map was read before a tablet server came into
            // existence. Another possible cause for an absent resource group is a bug in accumulo.
            // In either case do not call the balancer for now with the assumption that the resource
            // group will be available later. Log a message in case it is a bug.
            log.trace(
                "Could not find resource group for tserver {}, so did not consult balancer.  Assuming this is a temporary race condition.",
                current.getServerInstance());
          }
        }
        return TabletState.HOSTED;
      } else {
        return TabletState.ASSIGNED_TO_DEAD_SERVER;
      }
    } else if (tm.getSuspend() != null) {
      return TabletState.SUSPENDED;
    } else {
      return TabletState.UNASSIGNED;
    }
  }
}
