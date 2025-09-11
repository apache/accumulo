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

import java.util.function.Supplier;

import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.tablet.thrift.TUnloadTabletGoal;
import org.apache.accumulo.server.fs.VolumeUtil;
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
      return trace(HOSTED, tm, "tablet is in assigned state");
    }

    // Shutting down?
    TabletGoalState systemGoalState = getSystemGoalState(tm, params);

    if (systemGoalState == TabletGoalState.HOSTED) {
      // All the code in this block should look for reasons to return something other than HOSTED.

      if (!params.isParentLevelUpgraded()) {
        // The place where this tablet stores its metadata was not upgraded, so do not assign this
        // tablet yet.
        return trace(UNASSIGNED, tm, "parent level not upgraded");
      }

      // When an operation id is set tablets need to be unassigned unless there are still wals. When
      // there are wals the tablet needs to be hosted to recover data in them. However, deleting
      // tablets do not need to recover wals.
      if (tm.getOperationId() != null && (tm.getLogs().isEmpty()
          || tm.getOperationId().getType() == TabletOperationType.DELETING)) {
        return trace(UNASSIGNED, tm, () -> "operation id " + tm.getOperationId() + " is set");
      }

      if (!params.isTableOnline(tm.getTableId())) {
        return trace(UNASSIGNED, tm, "table is not online");
      }

      // Only want to override the HOSTED goal for tablet availability if there are no walog
      // present. If a tablet has walogs, then it should be hosted for recovery no matter what the
      // current tablet availability settings are. When tablet availability is ONDEMAND or UNHOSTED,
      // then this tablet will eventually become unhosted after recovery occurs. This could cause a
      // little bit of churn on the cluster w/r/t balancing, but it's necessary.
      if (tm.getLogs().isEmpty()) {
        switch (tm.getTabletAvailability()) {
          case UNHOSTED:
            return trace(UNASSIGNED, tm, "tablet availability is UNHOSTED");
          case ONDEMAND:
            if (!tm.getHostingRequested()) {
              return trace(UNASSIGNED, tm,
                  "tablet availability is ONDEMAND and no hosting requested");
            }
            break;
          default:
            break;
        }
      }

      TServerInstance dest = tm.getMigration();
      if (dest != null && tm.hasCurrent() && !dest.equals(tm.getLocation().getServerInstance())) {
        return trace(UNASSIGNED, tm, () -> "tablet has a migration to " + dest);
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
            public ResourceGroupId getResourceGroup() {
              return resourceGroup;
            }
          });

          if (reassign) {
            return trace(UNASSIGNED, tm, "the balancer requested reassignment");
          }
        } else {
          log.warn("Could not find resource group for tserver {}, did not consult balancer to"
              + " check if tablet {} needs to be re-assigned. This tablet will be rechecked"
              + " soon. If this condition is not transient, then it could indicate a bug so"
              + " please report it.", tm.getLocation().getServerInstance(), tm.getExtent());
        }
      }

      if (params.getVolumeReplacements().size() > 0
          && VolumeUtil.needsVolumeReplacement(params.getVolumeReplacements(), tm)) {
        return trace(UNASSIGNED, tm, "tablet has volumes needing replacement");
      }

      if (tm.hasCurrent()
          && params.getServersToShutdown().contains(tm.getLocation().getServerInstance())) {
        if (params.canSuspendTablets()) {
          return trace(SUSPENDED, tm,
              () -> "tablet is assigned to " + tm.getLocation() + " that is being shutdown");
        } else {
          return trace(UNASSIGNED, tm,
              () -> "tablet is assigned to " + tm.getLocation() + " that is being shutdown");
        }
      }
    }
    return trace(systemGoalState, tm, "it's the system goal state");
  }

  private static TabletGoalState getSystemGoalState(TabletMetadata tm,
      TabletManagementParameters params) {
      return switch (params.getManagerState()) {
          case NORMAL -> HOSTED; // fall-through intended
          // fall-through intended
          case HAVE_LOCK, INITIAL, SAFE_MODE -> {
              if (tm.getExtent().isMeta()) {
                  yield HOSTED;
              }
              yield TabletGoalState.UNASSIGNED;
          }
          case UNLOAD_METADATA_TABLETS -> {
              if (tm.getExtent().isRootTablet()) {
                  yield HOSTED;
              }
              yield UNASSIGNED;
          }
          case UNLOAD_ROOT_TABLET, STOP -> UNASSIGNED;
          default -> throw new IllegalStateException("Unknown Manager State");
      };
  }

  private static TabletGoalState trace(TabletGoalState tabletGoalState, TabletMetadata tm,
      String reason) {
    if (log.isTraceEnabled()) {
      // The prev row column for the table may not have been fetched so can not call tm.getExtent()
      log.trace("Computed goal state of {} for {} because {}", tabletGoalState,
          TabletsSection.encodeRow(tm.getTableId(), tm.getEndRow()), reason);
    }
    return tabletGoalState;
  }

  private static TabletGoalState trace(TabletGoalState tabletGoalState, TabletMetadata tm,
      Supplier<String> reason) {
    if (log.isTraceEnabled()) {
      log.trace("Computed goal state of {} for {} because {}", tabletGoalState,
          TabletsSection.encodeRow(tm.getTableId(), tm.getEndRow()), reason.get());
    }
    return tabletGoalState;
  }
}
