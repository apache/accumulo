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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.Path;

/**
 * Interface for storing information about tablet assignments. There are three implementations:
 */
public interface TabletStateStore extends Iterable<TabletManagement> {

  /**
   * Identifying name for this tablet state store.
   */
  String name();

  /**
   * Scan the information about the tablets covered by this store
   */
  @Override
  ClosableIterator<TabletManagement> iterator();

  /**
   * Store the assigned locations in the data store.
   */
  void setFutureLocations(Collection<Assignment> assignments) throws DistributedStoreException;

  /**
   * Tablet servers will update the data store with the location when they bring the tablet online
   */
  void setLocations(Collection<Assignment> assignments) throws DistributedStoreException;

  /**
   * Mark the tablets as having no known or future location.
   *
   * @param tablets the tablets' current information
   * @param logsForDeadServers a cache of logs in use by servers when they died
   */
  void unassign(Collection<TabletMetadata> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers) throws DistributedStoreException;

  /**
   * Mark tablets as having no known or future location, but desiring to be returned to their
   * previous tserver.
   */
  void suspend(Collection<TabletMetadata> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers, long suspensionTimestamp)
      throws DistributedStoreException;

  /**
   * Remove a suspension marker for a collection of tablets, moving them to being simply unassigned.
   */
  void unsuspend(Collection<TabletMetadata> tablets) throws DistributedStoreException;

  public static void unassign(ServerContext context, TabletMetadata tm,
      Map<TServerInstance,List<Path>> logsForDeadServers) throws DistributedStoreException {
    getStoreForTablet(tm.getExtent(), context).unassign(Collections.singletonList(tm),
        logsForDeadServers);
  }

  public static void suspend(ServerContext context, TabletMetadata tm,
      Map<TServerInstance,List<Path>> logsForDeadServers, long suspensionTimestamp)
      throws DistributedStoreException {
    getStoreForTablet(tm.getExtent(), context).suspend(Collections.singletonList(tm),
        logsForDeadServers, suspensionTimestamp);
  }

  public static void setLocation(ServerContext context, Assignment assignment)
      throws DistributedStoreException {
    getStoreForTablet(assignment.tablet, context)
        .setLocations(Collections.singletonList(assignment));
  }

  static TabletStateStore getStoreForTablet(KeyExtent extent, ServerContext context) {
    return getStoreForLevel(DataLevel.of(extent.tableId()), context);
  }

  public static TabletStateStore getStoreForLevel(DataLevel level, ServerContext context) {
    return getStoreForLevel(level, context, null);
  }

  public static TabletStateStore getStoreForLevel(DataLevel level, ServerContext context,
      CurrentState state) {

    TabletStateStore tss;
    switch (level) {
      case ROOT:
        tss = new ZooTabletStateStore(context);
        break;
      case METADATA:
        tss = new RootTabletStateStore(context, state);
        break;
      case USER:
        tss = new MetaDataStateStore(context, state);
        break;
      default:
        throw new IllegalArgumentException("Unknown level " + level);
    }

    return new LoggingTabletStateStore(tss);
  }
}
