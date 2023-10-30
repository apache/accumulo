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
package org.apache.accumulo.server.manager.state;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.tablet.thrift.TUnloadTabletGoal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public enum TabletGoalState {

  HOSTED(TUnloadTabletGoal.UNKNOWN),
  UNASSIGNED(TUnloadTabletGoal.UNASSIGNED),
  SUSPENDED(TUnloadTabletGoal.SUSPENDED);

  private final TUnloadTabletGoal unloadGoal;

  TabletGoalState(TUnloadTabletGoal unloadGoal) {
    this.unloadGoal = unloadGoal;
  }

  /** The purpose of unloading this tablet. */
  public TUnloadTabletGoal howUnload() {
    return unloadGoal;
  }

  private static final Logger log = LoggerFactory.getLogger(TabletGoalState.class);

  public static TabletGoalState compute(TabletMetadata tm, TabletState currentState,
      TabletBalancer balancer, TabletManagementParameters params) {
    Preconditions.checkArgument(Ample.DataLevel.of(tm.getTableId()) == params.getLevel(),
        "Tablet %s not in expected level %s", tm.getExtent(), params.getLevel());

    // Always follow through with assignments
    if (currentState == TabletState.ASSIGNED) {
      return HOSTED;
    }

    KeyExtent extent = tm.getExtent();
    // Shutting down?
    TabletGoalState state = getSystemGoalState(tm, params);

    if (state == TabletGoalState.HOSTED) {
      if (!params.isParentLevelUpgraded()) {
        // The place where this tablet stores its metadata was not upgraded, so do not assign this
        // tablet yet.
        return TabletGoalState.UNASSIGNED;
      }

      if (tm.getOperationId() != null) {
        return TabletGoalState.UNASSIGNED;
      }

      if (!params.isTableOnline(tm.getTableId())) {
        return TabletGoalState.UNASSIGNED;
      }

      switch (tm.getHostingGoal()) {
        case NEVER:
          return TabletGoalState.UNASSIGNED;
        case ONDEMAND:
          if (!tm.getHostingRequested()) {
            return TabletGoalState.UNASSIGNED;
          }
      }

      TServerInstance dest = params.getMigrations().get(extent);
      if (dest != null && tm.hasCurrent() && !dest.equals(tm.getLocation().getServerInstance())) {
        return TabletGoalState.UNASSIGNED;
      }

      if (currentState == TabletState.HOSTED && balancer != null) {
        // see if the balancer thinks this tablet needs to be unassigned

        Preconditions.checkArgument(
            tm.getLocation().getType() == TabletMetadata.LocationType.CURRENT,
            "Expected current tablet location %s %s", tm.getExtent(), tm.getLocation());
        var tsii = new TabletServerIdImpl(tm.getLocation().getServerInstance());

        var resourceGroup = params.getResourceGroup(tm.getLocation().getServerInstance());

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
            return UNASSIGNED;
          }
        } else {
          // A tablet server should always have a resource group, however there is a race
          // conditions where the resource group map was read before a tablet server came into
          // existence. Another possible cause for an absent resource group is a bug in accumulo.
          // In either case do not call the balancer for now with the assumption that the resource
          // group will be available later. Log a message in case it is a bug.
          log.trace(
              "Could not find resource group for tserver {}, so did not consult balancer.  Assuming this is a temporary race condition.",
              tm.getLocation().getServerInstance());
        }
      }

      if (tm.hasCurrent()
          && params.getServersToShutdown().contains(tm.getLocation().getServerInstance())) {
        return TabletGoalState.SUSPENDED;
      }
    }
    return state;
  }

  private static TabletGoalState getSystemGoalState(TabletMetadata tm,
      TabletManagementParameters params) {
    switch (params.getManagerState()) {
      case NORMAL:
        return TabletGoalState.HOSTED;
      case HAVE_LOCK: // fall-through intended
      case INITIAL: // fall-through intended
      case SAFE_MODE:
        if (tm.getExtent().isMeta()) {
          return TabletGoalState.HOSTED;
        }
        return TabletGoalState.UNASSIGNED;
      case UNLOAD_METADATA_TABLETS:
        if (tm.getExtent().isRootTablet()) {
          return TabletGoalState.HOSTED;
        }
        return TabletGoalState.UNASSIGNED;
      case UNLOAD_ROOT_TABLET:
        return TabletGoalState.UNASSIGNED;
      case STOP:
        return TabletGoalState.UNASSIGNED;
      default:
        throw new IllegalStateException("Unknown Manager State");
    }
  }
}
