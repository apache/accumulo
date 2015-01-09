/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.master.state;

import java.util.Collection;
import java.util.Collections;

/**
 * Interface for storing information about tablet assignments. There are three implementations:
 *
 * ZooTabletStateStore: information about the root tablet is stored in ZooKeeper MetaDataStateStore: information about the other tablets are stored in the
 * metadata table
 *
 */
public abstract class TabletStateStore implements Iterable<TabletLocationState> {

  /**
   * Identifying name for this tablet state store.
   */
  abstract public String name();

  /**
   * Scan the information about the tablets covered by this store
   */
  @Override
  abstract public ClosableIterator<TabletLocationState> iterator();

  /**
   * Store the assigned locations in the data store.
   */
  abstract public void setFutureLocations(Collection<Assignment> assignments) throws DistributedStoreException;

  /**
   * Tablet servers will update the data store with the location when they bring the tablet online
   */
  abstract public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException;

  /**
   * Mark the tablets as having no known or future location.
   *
   * @param tablets
   *          the tablets' current information
   */
  abstract public void unassign(Collection<TabletLocationState> tablets) throws DistributedStoreException;

  public static void unassign(TabletLocationState tls) throws DistributedStoreException {
    TabletStateStore store;
    if (tls.extent.isRootTablet()) {
      store = new ZooTabletStateStore();
    } else if (tls.extent.isMeta()) {
      store = new RootTabletStateStore();
    } else {
      store = new MetaDataStateStore();
    }
    store.unassign(Collections.singletonList(tls));
  }

  public static void setLocation(Assignment assignment) throws DistributedStoreException {
    TabletStateStore store;
    if (assignment.tablet.isRootTablet()) {
      store = new ZooTabletStateStore();
    } else if (assignment.tablet.isMeta()) {
      store = new RootTabletStateStore();
    } else {
      store = new MetaDataStateStore();
    }
    store.setLocations(Collections.singletonList(assignment));
  }

}
