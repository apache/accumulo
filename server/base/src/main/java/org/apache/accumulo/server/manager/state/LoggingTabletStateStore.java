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
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.fs.Path;

/**
 * Wraps a tablet state store and logs important events.
 */
class LoggingTabletStateStore implements TabletStateStore {

  private TabletStateStore wrapped;

  LoggingTabletStateStore(TabletStateStore tss) {
    this.wrapped = tss;
  }

  @Override
  public String name() {
    return wrapped.name();
  }

  @Override
  public ClosableIterator<TabletLocationState> iterator() {
    return wrapped.iterator();
  }

  @Override
  public void setFutureLocations(Collection<Assignment> assignments)
      throws DistributedStoreException {
    wrapped.setFutureLocations(assignments);
    assignments.forEach(assignment -> TabletLogger.assigned(assignment.tablet, assignment.server));
  }

  @Override
  public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    wrapped.setLocations(assignments);
    assignments.forEach(assignment -> TabletLogger.loaded(assignment.tablet, assignment.server));
  }

  @Override
  public void unassign(Collection<TabletLocationState> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers) throws DistributedStoreException {
    wrapped.unassign(tablets, logsForDeadServers);

    if (logsForDeadServers == null) {
      logsForDeadServers = Map.of();
    }

    for (TabletLocationState tls : tablets) {
      TabletLogger.unassigned(tls.extent, logsForDeadServers.size());
    }
  }

  @Override
  public void suspend(Collection<TabletLocationState> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers, long suspensionTimestamp)
      throws DistributedStoreException {
    wrapped.suspend(tablets, logsForDeadServers, suspensionTimestamp);

    if (logsForDeadServers == null) {
      logsForDeadServers = Map.of();
    }

    for (TabletLocationState tls : tablets) {
      var location = tls.getLocation();
      HostAndPort server = null;
      if (location != null) {
        server = location.getHostAndPort();
      }
      TabletLogger.suspended(tls.extent, server, suspensionTimestamp, TimeUnit.MILLISECONDS,
          logsForDeadServers.size());
    }
  }

  @Override
  public void unsuspend(Collection<TabletLocationState> tablets) throws DistributedStoreException {
    wrapped.unsuspend(tablets);
    for (TabletLocationState tls : tablets) {
      TabletLogger.unsuspended(tls.extent);
    }
  }
}
