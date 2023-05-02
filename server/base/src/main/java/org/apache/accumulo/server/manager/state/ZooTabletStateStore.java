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

import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ReadConsistency;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZooTabletStateStore extends AbstractTabletStateStore implements TabletStateStore {

  private static final Logger log = LoggerFactory.getLogger(ZooTabletStateStore.class);
  private final Ample ample;
  private final ClientContext context;

  ZooTabletStateStore(ClientContext context) {
    super(context);
    this.context = context;
    this.ample = context.getAmple();
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

          Location currentSession = null;
          Location futureSession = null;
          Location lastSession = null;

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
              lastSession, null, logs, false, TabletHostingGoal.ALWAYS, false);
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

  private static void validateAssignments(Collection<Assignment> assignments) {
    if (assignments.size() != 1) {
      throw new IllegalArgumentException("There is only one root tablet");
    }
    Assignment assignment = assignments.iterator().next();
    if (assignment.tablet.compareTo(RootTable.EXTENT) != 0) {
      throw new IllegalArgumentException("You can only store the root tablet location");
    }
  }

  @Override
  public void setFutureLocations(Collection<Assignment> assignments)
      throws DistributedStoreException {
    validateAssignments(assignments);
    super.setFutureLocations(assignments);
  }

  @Override
  public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    validateAssignments(assignments);
    super.setLocations(assignments);
  }

  private static void validateTablets(Collection<TabletLocationState> tablets) {
    if (tablets.size() != 1) {
      throw new IllegalArgumentException("There is only one root tablet");
    }
    TabletLocationState tls = tablets.iterator().next();
    if (tls.extent.compareTo(RootTable.EXTENT) != 0) {
      throw new IllegalArgumentException("You can only store the root tablet location");
    }
  }

  @Override
  public void unassign(Collection<TabletLocationState> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers) throws DistributedStoreException {
    validateTablets(tablets);

    super.unassign(tablets, logsForDeadServers);

    log.debug("unassign root tablet location");
  }

  @Override
  public void suspend(Collection<TabletLocationState> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers, long suspensionTimestamp)
      throws DistributedStoreException {
    validateTablets(tablets);
    super.suspend(tablets, logsForDeadServers, suspensionTimestamp);
  }

  @Override
  protected void processSuspension(Ample.ConditionalTabletMutator tabletMutator,
      TabletLocationState tls, long suspensionTimestamp) {
    // No support for suspending root tablet, so this is a NOOP
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
