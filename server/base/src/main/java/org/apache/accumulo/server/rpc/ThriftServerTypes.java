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
package org.apache.accumulo.server.rpc;

import org.apache.accumulo.core.clientImpl.thrift.ClientService;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.CompactorService;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.gc.thrift.GCMonitorService;
import org.apache.accumulo.core.manager.thrift.FateService;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer;
import org.apache.accumulo.core.rpc.ThriftClientTypes;
import org.apache.accumulo.core.rpc.ThriftClientTypes.ThriftClientType;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;

public class ThriftServerTypes {

  private static class ServerType<C extends TServiceClient,F extends TServiceClientFactory<C>>
      extends ThriftClientType<C,F> {

    public ServerType(ThriftClientType<C,F> type) {
      super(type.getServiceName(), type.isMultiplexed(), type.getClientFactory());
    }

    private <I,H extends I,P extends TBaseProcessor<?>> TProcessor getServer(
        Class<P> processorClass, Class<I> interfaceClass, H serviceHandler, ServerContext context,
        AccumuloConfiguration conf) throws Exception {
      I rpcProxy = TraceUtil.wrapService(serviceHandler);
      if (context.getThriftServerType() == ThriftServerType.SASL) {
        @SuppressWarnings("unchecked")
        Class<H> clazz = (Class<H>) serviceHandler.getClass();
        rpcProxy = TCredentialsUpdatingWrapper.service(rpcProxy, clazz, conf);
      }
      return processorClass.getConstructor(interfaceClass).newInstance(rpcProxy);
    }
  }

  private static final ServerType<ClientService.Client,ClientService.Client.Factory> CLIENT =
      new ServerType<>(ThriftClientTypes.CLIENT);

  private static final ServerType<CompactorService.Client,
      CompactorService.Client.Factory> COMPACTOR = new ServerType<>(ThriftClientTypes.COMPACTOR);

  private static final ServerType<CompactionCoordinatorService.Client,
      CompactionCoordinatorService.Client.Factory> COORDINATOR =
          new ServerType<>(ThriftClientTypes.COORDINATOR);

  private static final ServerType<FateService.Client,FateService.Client.Factory> FATE =
      new ServerType<>(ThriftClientTypes.FATE);

  private static final ServerType<GCMonitorService.Client,GCMonitorService.Client.Factory> GC =
      new ServerType<>(ThriftClientTypes.GC);

  private static final ServerType<ManagerClientService.Client,
      ManagerClientService.Client.Factory> MANAGER = new ServerType<>(ThriftClientTypes.MANAGER);

  private static final ServerType<ReplicationCoordinator.Client,
      ReplicationCoordinator.Client.Factory> REPLICATION_COORDINATOR =
          new ServerType<>(ThriftClientTypes.REPLICATION_COORDINATOR);

  private static final ServerType<ReplicationServicer.Client,
      ReplicationServicer.Client.Factory> REPLICATION_SERVICER =
          new ServerType<>(ThriftClientTypes.REPLICATION_SERVICER);

  private static final ServerType<TabletClientService.Client,
      TabletClientService.Client.Factory> TABLET_SERVER =
          new ServerType<>(ThriftClientTypes.TABLET_SERVER);

  public static TProcessor getCompactorThriftServer(CompactorService.Iface serviceHandler,
      ServerContext context, AccumuloConfiguration conf) throws Exception {
    return COMPACTOR.getServer(CompactorService.Processor.class, CompactorService.Iface.class,
        serviceHandler, context, conf);
  }

  public static TProcessor getCoordinatorThriftServer(
      CompactionCoordinatorService.Iface serviceHandler, ServerContext context,
      AccumuloConfiguration conf) throws Exception {
    return COORDINATOR.getServer(CompactionCoordinatorService.Processor.class,
        CompactionCoordinatorService.Iface.class, serviceHandler, context, conf);
  }

  public static TProcessor getGcThriftServer(GCMonitorService.Iface serviceHandler,
      ServerContext context, AccumuloConfiguration conf) throws Exception {
    return GC.getServer(GCMonitorService.Processor.class, GCMonitorService.Iface.class,
        serviceHandler, context, conf);
  }

  public static TProcessor getManagerThriftServer(FateService.Iface fateServiceHandler,
      ManagerClientService.Iface managerServiceHandler, ServerContext context,
      AccumuloConfiguration conf) throws Exception {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(FATE.getServiceName(), FATE.getServer(
        FateService.Processor.class, FateService.Iface.class, fateServiceHandler, context, conf));
    muxProcessor.registerProcessor(MANAGER.getServiceName(),
        MANAGER.getServer(ManagerClientService.Processor.class, ManagerClientService.Iface.class,
            managerServiceHandler, context, conf));
    return muxProcessor;
  }

  public static TProcessor getReplicationCoordinatorThriftServer(
      ReplicationCoordinator.Iface serviceHandler, ServerContext context,
      AccumuloConfiguration conf) throws Exception {
    return REPLICATION_COORDINATOR.getServer(ReplicationCoordinator.Processor.class,
        ReplicationCoordinator.Iface.class, serviceHandler, context, conf);
  }

  public static TProcessor getReplicationClientThriftServer(
      ReplicationServicer.Iface serviceHandler, ServerContext context, AccumuloConfiguration conf)
      throws Exception {
    return REPLICATION_SERVICER.getServer(ReplicationServicer.Processor.class,
        ReplicationServicer.Iface.class, serviceHandler, context, conf);
  }

  public static TProcessor getTabletServerThriftServer(ClientServiceHandler clientHandler,
      TabletClientService.Iface tserverHandler, ServerContext context, AccumuloConfiguration conf)
      throws Exception {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(CLIENT.getServiceName(), CLIENT.getServer(
        ClientService.Processor.class, ClientService.Iface.class, clientHandler, context, conf));
    muxProcessor.registerProcessor(TABLET_SERVER.getServiceName(),
        TABLET_SERVER.getServer(TabletClientService.Processor.class,
            TabletClientService.Iface.class, tserverHandler, context, conf));
    return muxProcessor;
  }

}
