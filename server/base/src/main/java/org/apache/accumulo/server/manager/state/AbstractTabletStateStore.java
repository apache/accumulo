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
import java.util.Map.Entry;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public abstract class AbstractTabletStateStore implements TabletStateStore {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTabletStateStore.class);

  private final Ample ample;

  protected AbstractTabletStateStore(ServerContext context) {
    this.ample = context.getAmple();
  }

  @Override
  public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    try (var tabletsMutator = ample.conditionallyMutateTablets()) {
      for (Assignment assignment : assignments) {
        var conditionalMutator = tabletsMutator.mutateTablet(assignment.tablet)
            .requireLocation(TabletMetadata.Location.future(assignment.server))
            .putLocation(TabletMetadata.Location.current(assignment.server))
            .deleteLocation(TabletMetadata.Location.future(assignment.server)).deleteSuspension();

        updateLastLocation(conditionalMutator, assignment.server, assignment.lastLocation);

        conditionalMutator.submit(tabletMetadata -> {
          Preconditions.checkArgument(tabletMetadata.getExtent().equals(assignment.tablet));
          return tabletMetadata.getLocation() != null && tabletMetadata.getLocation()
              .equals(TabletMetadata.Location.current(assignment.server));
        });
      }

      if (tabletsMutator.process().values().stream()
          .anyMatch(result -> result.getStatus() != Status.ACCEPTED)) {
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
              return tabletMetadata.getLocation() != null && tabletMetadata.getLocation()
                  .equals(TabletMetadata.Location.future(assignment.server));
            });
      }

      Map<KeyExtent,ConditionalResult> results = tabletsMutator.process();

      for (Entry<KeyExtent,ConditionalResult> entry : results.entrySet()) {
        if (entry.getValue().getStatus() != Status.ACCEPTED) {
          LOG.debug("Likely concurrent FATE operation prevented setting future location for {}, "
              + "Manager will retry soon.", entry.getKey());
        }
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
        if (tm.getLocation() == null) {
          continue;
        }

        var tabletMutator =
            tabletsMutator.mutateTablet(tm.getExtent()).requireLocation(tm.getLocation());

        if (tm.hasCurrent()) {

          updateLastLocation(tabletMutator, tm.getLocation().getServerInstance(), tm.getLast());
          tabletMutator.deleteLocation(tm.getLocation());
          if (logsForDeadServers != null) {
            List<Path> logs = logsForDeadServers.get(tm.getLocation().getServerInstance());
            if (logs != null) {
              for (Path log : logs) {
                LogEntry entry = LogEntry.fromPath(log.toString());
                tabletMutator.putWal(entry);
              }
            }
          }
        }

        if (tm.getLocation() != null && tm.getLocation().getType() != null
            && tm.getLocation().getType().equals(LocationType.FUTURE)) {
          tabletMutator.deleteLocation(tm.getLocation());
        }

        processSuspension(tabletMutator, tm, suspensionTimestamp);

        tabletMutator.submit(tabletMetadata -> tabletMetadata.getLocation() == null);
      }

      Map<KeyExtent,Ample.ConditionalResult> results = tabletsMutator.process();

      if (results.values().stream()
          .anyMatch(conditionalResult -> conditionalResult.getStatus() != Status.ACCEPTED)) {
        throw new DistributedStoreException("Some unassignments did not satisfy conditions.");
      }

    } catch (RuntimeException ex) {
      throw new DistributedStoreException(ex);
    }
  }

  protected static void updateLastLocation(Ample.TabletUpdates<?> tabletMutator,
      TServerInstance location, Location lastLocation) {
    Preconditions.checkArgument(
        lastLocation == null || lastLocation.getType() == TabletMetadata.LocationType.LAST);
    Location newLocation = Location.last(location);
    if (lastLocation != null) {
      if (!lastLocation.equals(newLocation)) {
        tabletMutator.deleteLocation(lastLocation);
        tabletMutator.putLocation(newLocation);
      }
    } else {
      tabletMutator.putLocation(newLocation);
    }
  }

}
