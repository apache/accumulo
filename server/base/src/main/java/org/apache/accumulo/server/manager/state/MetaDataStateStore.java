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

import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.util.ManagerMetadataUtil;
import org.apache.hadoop.fs.Path;

class MetaDataStateStore implements TabletStateStore {

  protected final ClientContext context;
  protected final CurrentState state;
  private final String targetTableName;
  private final Ample ample;
  private final DataLevel level;

  protected MetaDataStateStore(DataLevel level, ClientContext context, CurrentState state,
      String targetTableName) {
    this.level = level;
    this.context = context;
    this.state = state;
    this.ample = context.getAmple();
    this.targetTableName = targetTableName;
  }

  MetaDataStateStore(DataLevel level, ClientContext context, CurrentState state) {
    this(level, context, state, MetadataTable.NAME);
  }

  @Override
  public DataLevel getLevel() {
    return level;
  }

  @Override
  public ClosableIterator<TabletLocationState> iterator() {
    return new MetaDataTableScanner(context, TabletsSection.getRange(), state, targetTableName);
  }

  @Override
  public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    try (var tabletsMutator = ample.mutateTablets()) {
      for (Assignment assignment : assignments) {
        TabletMutator tabletMutator = tabletsMutator.mutateTablet(assignment.tablet);
        tabletMutator.putLocation(Location.current(assignment.server));
        ManagerMetadataUtil.updateLastForAssignmentMode(context, tabletMutator, assignment.server,
            assignment.lastLocation);
        tabletMutator.deleteLocation(Location.future(assignment.server));
        tabletMutator.deleteSuspension();
        tabletMutator.mutate();
      }
    } catch (RuntimeException ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public void setFutureLocations(Collection<Assignment> assignments)
      throws DistributedStoreException {
    try (var tabletsMutator = ample.mutateTablets()) {
      for (Assignment assignment : assignments) {
        tabletsMutator.mutateTablet(assignment.tablet).deleteSuspension()
            .putLocation(Location.future(assignment.server)).mutate();
      }
    } catch (RuntimeException ex) {
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
          ManagerMetadataUtil.updateLastForAssignmentMode(context, tabletMutator,
              tls.current.getServerInstance(), tls.last);
          tabletMutator.deleteLocation(tls.current);
          if (logsForDeadServers != null) {
            List<Path> logs = logsForDeadServers.get(tls.current.getServerInstance());
            if (logs != null) {
              for (Path log : logs) {
                LogEntry entry = new LogEntry(tls.extent, 0, log.toString());
                tabletMutator.putWal(entry);
              }
            }
          }
          if (suspensionTimestamp >= 0) {
            tabletMutator.putSuspension(tls.current.getServerInstance(), suspensionTimestamp);
          }
        }
        if (tls.suspend != null && suspensionTimestamp < 0) {
          tabletMutator.deleteSuspension();
        }
        if (tls.hasFuture()) {
          tabletMutator.deleteLocation(tls.future);
        }
        tabletMutator.mutate();
      }
    } catch (RuntimeException ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public void unsuspend(Collection<TabletLocationState> tablets) throws DistributedStoreException {
    try (var tabletsMutator = ample.mutateTablets()) {
      for (TabletLocationState tls : tablets) {
        if (tls.suspend != null) {
          tabletsMutator.mutateTablet(tls.extent).deleteSuspension().mutate();
        }
      }
    } catch (RuntimeException ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public String name() {
    return "Normal Tablets";
  }

}
