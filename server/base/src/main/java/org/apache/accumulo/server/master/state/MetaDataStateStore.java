/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.master.state;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.hadoop.fs.Path;

class MetaDataStateStore implements TabletStateStore {

  protected final ClientContext context;
  protected final CurrentState state;
  private final String targetTableName;
  private final Ample ample;

  protected MetaDataStateStore(ClientContext context, CurrentState state, String targetTableName) {
    this.context = context;
    this.state = state;
    this.ample = context.getAmple();
    this.targetTableName = targetTableName;
  }

  MetaDataStateStore(ClientContext context, CurrentState state) {
    this(context, state, MetadataTable.NAME);
  }

  @Override
  public ClosableIterator<TabletLocationState> iterator() {
    return new MetaDataTableScanner(context, TabletsSection.getRange(), state, targetTableName);
  }

  public void setLocation(Assignment assignment, TServerInstance prevLastLoc)
      throws DistributedStoreException {
    try {
      TabletMutator tabletMutator = ample.mutateTablet(assignment.tablet);
      tabletMutator.putLocation(assignment.server, LocationType.CURRENT);
      tabletMutator.putLocation(assignment.server, LocationType.LAST);
      tabletMutator.deleteLocation(assignment.server, LocationType.FUTURE);

      // remove the old location
      if (prevLastLoc != null && !prevLastLoc.equals(assignment.server)) {
        tabletMutator.deleteLocation(prevLastLoc, LocationType.LAST);
      }

      tabletMutator.mutate();
    } catch (Exception ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public void setFutureLocation(Assignment assignment) throws DistributedStoreException {
    try {
      TabletMutator tabletMutator = ample.mutateTablet(assignment.tablet);
      tabletMutator.deleteSuspension();
      tabletMutator.putLocation(assignment.server, LocationType.FUTURE);
      tabletMutator.mutate();

    } catch (Exception ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public void unassign(Collection<TabletLocationState> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers) throws DistributedStoreException {
    unassign(tablets, logsForDeadServers, -1);
  }

  @Override
  public void suspend(Collection<TabletLocationState> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers, long suspensionTimestamp)
      throws DistributedStoreException {
    unassign(tablets, logsForDeadServers, suspensionTimestamp);
  }

  private void unassign(Collection<TabletLocationState> tablets,
      Map<TServerInstance,List<Path>> logsForDeadServers, long suspensionTimestamp)
      throws DistributedStoreException {
    try (var tabletsMutator = ample.mutateTablets()) {
      for (TabletLocationState tls : tablets) {
        TabletMutator tabletMutator = tabletsMutator.mutateTablet(tls.extent);
        if (tls.current != null) {
          tabletMutator.deleteLocation(tls.current, LocationType.CURRENT);
          if (logsForDeadServers != null) {
            List<Path> logs = logsForDeadServers.get(tls.current);
            if (logs != null) {
              for (Path log : logs) {
                LogEntry entry =
                    new LogEntry(tls.extent, 0, tls.current.hostPort(), log.toString());
                tabletMutator.putWal(entry);
              }
            }
          }
          if (suspensionTimestamp >= 0) {
            tabletMutator.putSuspension(tls.current, suspensionTimestamp);
          }
        }
        if (tls.suspend != null && suspensionTimestamp < 0) {
          tabletMutator.deleteSuspension();
        }
        if (tls.future != null) {
          tabletMutator.deleteLocation(tls.future, LocationType.FUTURE);
        }
        tabletMutator.mutate();
      }
    } catch (Exception ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public void unsuspend(Collection<TabletLocationState> tablets) throws DistributedStoreException {

    try (var tabletsMutator = ample.mutateTablets()) {
      for (TabletLocationState tls : tablets) {
        if (tls.suspend != null) {
          continue;
        }
        TabletMutator tabletMutator = tabletsMutator.mutateTablet(tls.extent);
        tabletMutator.deleteSuspension();
        tabletMutator.mutate();
      }
    } catch (Exception ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public String name() {
    return "Normal Tablets";
  }

}
