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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ReadConsistency;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZooTabletStateStore implements TabletStateStore {

  private static final Logger log = LoggerFactory.getLogger(ZooTabletStateStore.class);
  private final Ample ample;

  ZooTabletStateStore(Ample ample) {
    this.ample = ample;
  }

  @Override
  public ClosableIterator<TabletLocationState> iterator() {

    return new ClosableIterator<>() {
      boolean finished = false;

      @Override
      public boolean hasNext() {
        return !finished;
      }

      @Override
      public TabletLocationState next() {
        finished = true;
        try {

          TabletMetadata rootMeta = ample.readTablet(RootTable.EXTENT, ReadConsistency.EVENTUAL);

          TServerInstance currentSession = null;
          TServerInstance futureSession = null;
          TServerInstance lastSession = null;

          Location loc = rootMeta.getLocation();

          if (loc != null && loc.getType() == LocationType.FUTURE) {
            futureSession = loc;
          }

          if (rootMeta.getLast() != null) {
            lastSession = rootMeta.getLast();
          }

          if (loc != null && loc.getType() == LocationType.CURRENT) {
            currentSession = loc;
          }

          List<Collection<String>> logs = new ArrayList<>();
          rootMeta.getLogs().forEach(logEntry -> {
            logs.add(Collections.singleton(logEntry.filename));
            log.debug("root tablet log {}", logEntry.filename);
          });

          return new TabletLocationState(RootTable.EXTENT, futureSession, currentSession,
              lastSession, null, logs, false);
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public void setFutureLocations(Collection<Assignment> assignments)
      throws DistributedStoreException {
    if (assignments.size() != 1) {
      throw new IllegalArgumentException("There is only one root tablet");
    }
    Assignment assignment = assignments.iterator().next();
    if (assignment.tablet.compareTo(RootTable.EXTENT) != 0) {
      throw new IllegalArgumentException("You can only store the root tablet location");
    }

    TabletMutator tabletMutator = ample.mutateTablet(assignment.tablet);
    tabletMutator.putLocation(assignment.server, LocationType.FUTURE);
    tabletMutator.mutate();
  }

  @Override
  public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    if (assignments.size() != 1) {
      throw new IllegalArgumentException("There is only one root tablet");
    }
    Assignment assignment = assignments.iterator().next();
    if (assignment.tablet.compareTo(RootTable.EXTENT) != 0) {
      throw new IllegalArgumentException("You can only store the root tablet location");
    }

    TabletMutator tabletMutator = ample.mutateTablet(assignment.tablet);
    tabletMutator.putLocation(assignment.server, LocationType.CURRENT);
    tabletMutator.deleteLocation(assignment.server, LocationType.FUTURE);

    tabletMutator.mutate();
  }

  @Override
  public void unassign(Collection<TabletLocationState> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers) throws DistributedStoreException {
    if (tablets.size() != 1) {
      throw new IllegalArgumentException("There is only one root tablet");
    }
    TabletLocationState tls = tablets.iterator().next();
    if (tls.extent.compareTo(RootTable.EXTENT) != 0) {
      throw new IllegalArgumentException("You can only store the root tablet location");
    }

    TabletMutator tabletMutator = ample.mutateTablet(tls.extent);

    tabletMutator.deleteLocation(tls.futureOrCurrent(), LocationType.FUTURE);
    tabletMutator.deleteLocation(tls.futureOrCurrent(), LocationType.CURRENT);
    if (logsForDeadServers != null) {
      List<Path> logs = logsForDeadServers.get(tls.futureOrCurrent());
      if (logs != null) {
        for (Path entry : logs) {
          LogEntry logEntry =
              new LogEntry(RootTable.EXTENT, System.currentTimeMillis(), entry.toString());
          tabletMutator.putWal(logEntry);
        }
      }
    }

    tabletMutator.mutate();

    log.debug("unassign root tablet location");
  }

  @Override
  public void suspend(Collection<TabletLocationState> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers, long suspensionTimestamp)
      throws DistributedStoreException {
    // No support for suspending root tablet.
    unassign(tablets, logsForDeadServers);
  }

  @Override
  public void unsuspend(Collection<TabletLocationState> tablets) {
    // no support for suspending root tablet.
  }

  @Override
  public String name() {
    return "Root Table";
  }
}
