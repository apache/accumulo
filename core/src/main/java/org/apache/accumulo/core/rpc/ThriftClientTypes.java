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
package org.apache.accumulo.core.rpc;

import org.apache.accumulo.core.clientImpl.thrift.ClientService;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.CompactorService;
import org.apache.accumulo.core.gc.thrift.GCMonitorService;
import org.apache.accumulo.core.manager.thrift.FateService;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletScanClientService;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;

public class ThriftClientTypes {

  public static class ThriftClientType<C extends TServiceClient,
      F extends TServiceClientFactory<C>> {

    private final String serviceName;
    private final F clientFactory;

    public ThriftClientType(String serviceName, F clientFactory) {
      super();
      this.serviceName = serviceName;
      this.clientFactory = clientFactory;
    }

    public String getServiceName() {
      return serviceName;
    }

    public F getClientFactory() {
      return clientFactory;
    }

    public C getClient(TProtocol prot) {
      // All server side TProcessors are multiplexed. Wrap this protocol.
      return clientFactory.getClient(new TMultiplexedProtocol(prot, getServiceName()));
    }

  }

  public static final ThriftClientType<ClientService.Client,ClientService.Client.Factory> CLIENT =
      new ThriftClientType<>("ClientService", new ClientService.Client.Factory());

  public static final ThriftClientType<CompactorService.Client,
      CompactorService.Client.Factory> COMPACTOR =
          new ThriftClientType<>("CompactorService", new CompactorService.Client.Factory());

  public static final ThriftClientType<CompactionCoordinatorService.Client,
      CompactionCoordinatorService.Client.Factory> COORDINATOR = new ThriftClientType<>(
          "CompactionCoordinatorService", new CompactionCoordinatorService.Client.Factory());

  public static final ThriftClientType<FateService.Client,FateService.Client.Factory> FATE =
      new ThriftClientType<>("FateService", new FateService.Client.Factory());

  public static final ThriftClientType<GCMonitorService.Client,GCMonitorService.Client.Factory> GC =
      new ThriftClientType<>("GCMonitorService", new GCMonitorService.Client.Factory());

  public static final ThriftClientType<ManagerClientService.Client,
      ManagerClientService.Client.Factory> MANAGER =
          new ThriftClientType<>("ManagerClientService", new ManagerClientService.Client.Factory());

  public static final ThriftClientType<ReplicationCoordinator.Client,
      ReplicationCoordinator.Client.Factory> REPLICATION_COORDINATOR = new ThriftClientType<>(
          "ReplicationCoordinator", new ReplicationCoordinator.Client.Factory());

  public static final ThriftClientType<ReplicationServicer.Client,
      ReplicationServicer.Client.Factory> REPLICATION_SERVICER =
          new ThriftClientType<>("ReplicationServicer", new ReplicationServicer.Client.Factory());

  public static final ThriftClientType<TabletClientService.Client,
      TabletClientService.Client.Factory> TABLET_SERVER =
          new ThriftClientType<>("TabletClientService", new TabletClientService.Client.Factory());

  public static final ThriftClientType<TabletScanClientService.Client,
      TabletScanClientService.Client.Factory> TABLET_SCAN = new ThriftClientType<>(
          "TabletScanClientService", new TabletScanClientService.Client.Factory());
}
