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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.util.ManagerMetadataUtil;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

public abstract class AbstractTabletStateStore implements TabletStateStore {

  private final ClientContext context;
  private final Ample ample;

  protected AbstractTabletStateStore(ClientContext context) {
    this.context = context;
    this.ample = context.getAmple();
  }

  @Override
  public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    try (var tabletsMutator = ample.conditionallyMutateTablets()) {
      for (Assignment assignment : assignments) {
        var conditionalMutator = tabletsMutator.mutateTablet(assignment.tablet)
            .requireAbsentOperation()
            .requireLocation(TabletMetadata.Location.future(assignment.server))
            .putLocation(TabletMetadata.Location.current(assignment.server))
            .deleteLocation(TabletMetadata.Location.future(assignment.server)).deleteSuspension();

        ManagerMetadataUtil.updateLastForAssignmentMode(context, conditionalMutator,
            assignment.server, assignment.lastLocation);

        conditionalMutator.submit(tabletMetadata -> {
          Preconditions.checkArgument(tabletMetadata.getExtent().equals(assignment.tablet));
          // see if we are the current location, if so then the unknown mutation actually
          // succeeded
          return tabletMetadata.getLocation() != null && tabletMetadata.getLocation()
              .equals(TabletMetadata.Location.current(assignment.server));
        });
      }

      if (tabletsMutator.process().values().stream()
          .anyMatch(result -> result.getStatus() != ConditionalWriter.Status.ACCEPTED)) {
        // TODO should this look at why?
        throw new DistributedStoreException(
            "failed to set tablet location, conditional mutation failed");
      }
    } catch (RuntimeException ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public void setFutureLocations(Collection<Assignment> assignments)
      throws DistributedStoreException {
    try (var tabletsMutator = ample.conditionallyMutateTablets()) {
      for (Assignment assignment : assignments) {
        tabletsMutator.mutateTablet(assignment.tablet).requireAbsentOperation()
            .requireAbsentLocation().deleteSuspension()
            .putLocation(TabletMetadata.Location.future(assignment.server))
            .submit(tabletMetadata -> {
              Preconditions.checkArgument(tabletMetadata.getExtent().equals(assignment.tablet));
              // see if we are the future location, if so then the unknown mutation actually
              // succeeded
              return tabletMetadata.getLocation() != null && tabletMetadata.getLocation()
                  .equals(TabletMetadata.Location.future(assignment.server));
            });
      }

      var results = tabletsMutator.process();

      if (results.values().stream()
          .anyMatch(result -> result.getStatus() != ConditionalWriter.Status.ACCEPTED)) {
        var statuses = results.values().stream().map(Ample.ConditionalResult::getStatus)
            .collect(Collectors.toSet());
        throw new DistributedStoreException(
            "failed to set tablet location, conditional mutation failed. " + statuses);
      }

    } catch (RuntimeException ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public void unassign(Collection<TabletMetadata> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers) throws DistributedStoreException {
    unassign(tablets, logsForDeadServers, -1);
  }

  @Override
  public void suspend(Collection<TabletMetadata> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers, long suspensionTimestamp)
      throws DistributedStoreException {
    unassign(tablets, logsForDeadServers, suspensionTimestamp);
  }

  protected abstract void processSuspension(Ample.ConditionalTabletMutator tabletMutator,
      TabletMetadata tm, long suspensionTimestamp);

  private void unassign(Collection<TabletMetadata> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers, long suspensionTimestamp)
      throws DistributedStoreException {
    try (var tabletsMutator = ample.conditionallyMutateTablets()) {
      for (TabletMetadata tm : tablets) {
        var tabletMutator = tabletsMutator.mutateTablet(tm.getExtent()).requireAbsentOperation();

        if (tm.hasCurrent()) {
          tabletMutator.requireLocation(tm.getLocation());

          ManagerMetadataUtil.updateLastForAssignmentMode(context, tabletMutator,
              tm.getLocation().getServerInstance(), tm.getLast());
          tabletMutator.deleteLocation(tm.getLocation());
          if (logsForDeadServers != null) {
            List<Path> logs = logsForDeadServers.get(tm.getLocation().getServerInstance());
            if (logs != null) {
              for (Path log : logs) {
                LogEntry entry = new LogEntry(tm.getExtent(), 0, log.toString());
                tabletMutator.putWal(entry);
              }
            }
          }
        }

        if (tm.getLocation() != null && tm.getLocation().getType() != null
            && tm.getLocation().getType().equals(LocationType.FUTURE)) {
          tabletMutator.requireLocation(tm.getLocation());
          tabletMutator.deleteLocation(tm.getLocation());
        }

        processSuspension(tabletMutator, tm, suspensionTimestamp);

        tabletMutator.submit(tabletMetadata -> {
          // The status of the conditional update is unknown, so check and see if things are ok
          return tabletMetadata.getLocation() == null;
        });
      }

      Map<KeyExtent,Ample.ConditionalResult> results = tabletsMutator.process();

      if (results.values().stream().anyMatch(conditionalResult -> conditionalResult.getStatus()
          != ConditionalWriter.Status.ACCEPTED)) {
        throw new DistributedStoreException("Some unassignments did not satisfy conditions.");
      }

    } catch (RuntimeException ex) {
      throw new DistributedStoreException(ex);
    }
  }
}
