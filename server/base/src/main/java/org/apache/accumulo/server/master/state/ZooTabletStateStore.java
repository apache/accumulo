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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooTabletStateStore extends TabletStateStore {

  private static final Logger log = LoggerFactory.getLogger(ZooTabletStateStore.class);
  final private DistributedStore store;

  public ZooTabletStateStore(DistributedStore store) {
    this.store = store;
  }

  public ZooTabletStateStore() throws DistributedStoreException {
    try {
      store = new ZooStore();
    } catch (IOException ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public ClosableIterator<TabletLocationState> iterator() {
    return new ClosableIterator<TabletLocationState>() {
      boolean finished = false;

      @Override
      public boolean hasNext() {
        return !finished;
      }

      @Override
      public TabletLocationState next() {
        finished = true;
        try {
          byte[] future = store.get(RootTable.ZROOT_TABLET_FUTURE_LOCATION);
          byte[] current = store.get(RootTable.ZROOT_TABLET_LOCATION);
          byte[] last = store.get(RootTable.ZROOT_TABLET_LAST_LOCATION);

          TServerInstance currentSession = null;
          TServerInstance futureSession = null;
          TServerInstance lastSession = null;

          if (future != null)
            futureSession = parse(future);

          if (last != null)
            lastSession = parse(last);

          if (current != null) {
            currentSession = parse(current);
            futureSession = null;
          }
          List<Collection<String>> logs = new ArrayList<>();
          for (String entry : store.getChildren(RootTable.ZROOT_TABLET_WALOGS)) {
            byte[] logInfo = store.get(RootTable.ZROOT_TABLET_WALOGS + "/" + entry);
            if (logInfo != null) {
              LogEntry logEntry = LogEntry.fromBytes(logInfo);
              logs.add(Collections.singleton(logEntry.filename));
              log.debug("root tablet log {}", logEntry.filename);
            }
          }
          TabletLocationState result = new TabletLocationState(RootTable.EXTENT, futureSession, currentSession, lastSession, null, logs, false);
          log.debug("Returning root tablet state: {}", result);
          return result;
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }

      @Override
      public void remove() {
        throw new NotImplementedException();
      }

      @Override
      public void close() {}
    };
  }

  protected TServerInstance parse(byte[] current) {
    String str = new String(current, UTF_8);
    String[] parts = str.split("[|]", 2);
    HostAndPort address = HostAndPort.fromString(parts[0]);
    if (parts.length > 1 && parts[1] != null && parts[1].length() > 0) {
      return new TServerInstance(address, parts[1]);
    } else {
      // a 1.2 location specification: DO NOT WANT
      return null;
    }
  }

  @Override
  public void setFutureLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    if (assignments.size() != 1)
      throw new IllegalArgumentException("There is only one root tablet");
    Assignment assignment = assignments.iterator().next();
    if (assignment.tablet.compareTo(RootTable.EXTENT) != 0)
      throw new IllegalArgumentException("You can only store the root tablet location");
    String value = assignment.server.getLocation() + "|" + assignment.server.getSession();
    Iterator<TabletLocationState> currentIter = iterator();
    TabletLocationState current = currentIter.next();
    if (current.current != null) {
      throw new DistributedStoreException("Trying to set the root tablet location: it is already set to " + current.current);
    }
    store.put(RootTable.ZROOT_TABLET_FUTURE_LOCATION, value.getBytes(UTF_8));
  }

  @Override
  public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    if (assignments.size() != 1)
      throw new IllegalArgumentException("There is only one root tablet");
    Assignment assignment = assignments.iterator().next();
    if (assignment.tablet.compareTo(RootTable.EXTENT) != 0)
      throw new IllegalArgumentException("You can only store the root tablet location");
    String value = assignment.server.getLocation() + "|" + assignment.server.getSession();
    Iterator<TabletLocationState> currentIter = iterator();
    TabletLocationState current = currentIter.next();
    if (current.current != null) {
      throw new DistributedStoreException("Trying to set the root tablet location: it is already set to " + current.current);
    }
    if (!current.future.equals(assignment.server)) {
      throw new DistributedStoreException("Root tablet is already assigned to " + current.future);
    }
    store.put(RootTable.ZROOT_TABLET_LOCATION, value.getBytes(UTF_8));
    store.put(RootTable.ZROOT_TABLET_LAST_LOCATION, value.getBytes(UTF_8));
    // Make the following unnecessary by making the entire update atomic
    store.remove(RootTable.ZROOT_TABLET_FUTURE_LOCATION);
    log.debug("Put down root tablet location");
  }

  @Override
  public void unassign(Collection<TabletLocationState> tablets, Map<TServerInstance,List<Path>> logsForDeadServers) throws DistributedStoreException {
    if (tablets.size() != 1)
      throw new IllegalArgumentException("There is only one root tablet");
    TabletLocationState tls = tablets.iterator().next();
    if (tls.extent.compareTo(RootTable.EXTENT) != 0)
      throw new IllegalArgumentException("You can only store the root tablet location");
    if (logsForDeadServers != null) {
      List<Path> logs = logsForDeadServers.get(tls.futureOrCurrent());
      if (logs != null) {
        for (Path entry : logs) {
          LogEntry logEntry = new LogEntry(RootTable.EXTENT, System.currentTimeMillis(), tls.futureOrCurrent().getLocation().toString(), entry.toString());
          byte[] value;
          try {
            value = logEntry.toBytes();
          } catch (IOException ex) {
            throw new DistributedStoreException(ex);
          }
          store.put(RootTable.ZROOT_TABLET_WALOGS + "/" + logEntry.getUniqueID(), value);
        }
      }
    }
    store.remove(RootTable.ZROOT_TABLET_LOCATION);
    store.remove(RootTable.ZROOT_TABLET_FUTURE_LOCATION);
    log.debug("unassign root tablet location");
  }

  @Override
  public void suspend(Collection<TabletLocationState> tablets, Map<TServerInstance,List<Path>> logsForDeadServers, long suspensionTimestamp)
      throws DistributedStoreException {
    // No support for suspending root tablet.
    unassign(tablets, logsForDeadServers);
  }

  @Override
  public void unsuspend(Collection<TabletLocationState> tablets) throws DistributedStoreException {
    // no support for suspending root tablet.
  }

  @Override
  public String name() {
    return "Root Table";
  }
}
