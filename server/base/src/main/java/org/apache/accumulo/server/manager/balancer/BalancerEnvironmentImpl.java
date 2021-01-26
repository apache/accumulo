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
package org.apache.accumulo.server.manager.balancer;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.manager.balancer.TabletStatisticsImpl;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.spi.balancer.BalancerEnvironment;
import org.apache.accumulo.core.spi.balancer.data.TabletMigration;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.spi.balancer.data.TabletStatistics;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BalancerEnvironmentImpl extends ServiceEnvironmentImpl implements BalancerEnvironment {
  private static final Logger log = LoggerFactory.getLogger(BalancerEnvironmentImpl.class);

  public BalancerEnvironmentImpl(ServerContext ctx) {
    super(ctx);
  }

  @Override
  public Map<String,TableId> getTableIdMap() {
    return Tables.getNameToIdMap(getContext());
  }

  @Override
  public boolean isTableOnline(TableId tableId) {
    return TableState.ONLINE.equals(Tables.getTableState(getContext(), tableId));
  }

  @Override
  public Map<TabletId,TabletServerId> listTabletLocations(TableId tableId) {
    Map<TabletId,TabletServerId> tablets = new LinkedHashMap<>();
    for (var tm : TabletsMetadata.builder().forTable(tableId).fetch(LOCATION, PREV_ROW)
        .build(getContext())) {
      TServerInstance inst = tm.getLocation();
      tablets.put(new TabletIdImpl(tm.getExtent()), new TabletServerIdImpl(inst.getHost(),
          inst.getHostAndPort().getPort(), inst.getSession()));
    }
    return tablets;
  }

  @Override
  public List<TabletStatistics> listOnlineTabletsForTable(TabletServerId tabletServerId,
      TableId tableId) throws AccumuloException, AccumuloSecurityException {
    log.debug("Scanning tablet server {} for table {}", tabletServerId, tableId);
    try {
      TabletClientService.Client client = ThriftUtil.getClient(
          new TabletClientService.Client.Factory(),
          HostAndPort.fromParts(tabletServerId.getHost(), tabletServerId.getPort()), getContext());
      try {
        return client
            .getTabletStats(TraceUtil.traceInfo(), getContext().rpcCreds(), tableId.canonical())
            .stream().map(TabletStatisticsImpl::new).collect(Collectors.toList());
      } catch (TTransportException e) {
        log.error("Unable to connect to {}: ", tabletServerId, e);
      } finally {
        ThriftUtil.returnClient(client);
      }
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e);
    } catch (TException e) {
      throw new AccumuloException(e);
    }
    return null;
  }

  @Override
  public List<TabletMigration> checkMigrationSanity(Set<TabletServerId> current,
      List<TabletMigration> migrations) {
    return migrations.stream().filter(m -> {
      boolean includeMigration = false;
      if (m.getTablet() == null) {
        log.warn("Balancer gave back a null tablet {}", m);
      } else if (m.getNewTabletServer() == null) {
        log.warn("Balancer did not set the destination {}", m);
      } else if (m.getOldTabletServer() == null) {
        log.warn("Balancer did not set the source {}", m);
      } else if (!current.contains(m.getOldTabletServer())) {
        log.warn("Balancer wants to move a tablet from a server that is not current: {}", m);
      } else if (!current.contains(m.getNewTabletServer())) {
        log.warn("Balancer wants to move a tablet to a server that is not current: {}", m);
      } else {
        includeMigration = true;
      }
      return includeMigration;
    }).collect(Collectors.toList());
  }
}
