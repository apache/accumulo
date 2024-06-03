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
import java.util.Set;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.hadoop.fs.Path;

import com.google.common.net.HostAndPort;

/**
 * Wraps a tablet state store and logs important events.
 */
class LoggingTabletStateStore implements TabletStateStore {

  private TabletStateStore wrapped;

  LoggingTabletStateStore(TabletStateStore tss) {
    this.wrapped = tss;
  }

  @Override
  public DataLevel getLevel() {
    return wrapped.getLevel();
  }

  @Override
  public String name() {
    return wrapped.name();
  }

  @Override
  public ClosableIterator<TabletManagement> iterator(List<Range> ranges,
      TabletManagementParameters parameters) {
    return wrapped.iterator(ranges, parameters);
  }

  @Override
  public Set<KeyExtent> setFutureLocations(Collection<Assignment> assignments)
      throws DistributedStoreException {
    var failures = wrapped.setFutureLocations(assignments);
    assignments.forEach(assignment -> {
      if (!failures.contains(assignment.tablet)) {
        TabletLogger.assigned(assignment.tablet, assignment.server);
      }
    });
    return failures;
  }

  @Override
  public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    wrapped.setLocations(assignments);
    assignments.forEach(assignment -> TabletLogger.loaded(assignment.tablet, assignment.server));
  }

  @Override
  public void unassign(Collection<TabletMetadata> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers) throws DistributedStoreException {
    wrapped.unassign(tablets, logsForDeadServers);

    if (logsForDeadServers == null) {
      logsForDeadServers = Map.of();
    }

    for (TabletMetadata tm : tablets) {
      TabletLogger.unassigned(tm.getExtent(), logsForDeadServers.size());
    }
  }

  @Override
  public void suspend(Collection<TabletMetadata> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers, SteadyTime suspensionTimestamp)
      throws DistributedStoreException {
    wrapped.suspend(tablets, logsForDeadServers, suspensionTimestamp);

    if (logsForDeadServers == null) {
      logsForDeadServers = Map.of();
    }

    for (TabletMetadata tm : tablets) {
      var location = tm.getLocation();
      HostAndPort server = null;
      if (location != null) {
        server = location.getHostAndPort();
      }
      TabletLogger.suspended(tm.getExtent(), server, suspensionTimestamp,
          logsForDeadServers.size());
    }
  }

  @Override
  public void unsuspend(Collection<TabletMetadata> tablets) throws DistributedStoreException {
    wrapped.unsuspend(tablets);
    for (TabletMetadata tm : tablets) {
      TabletLogger.unsuspended(tm.getExtent());
    }
  }
}
