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
import org.apache.accumulo.core.tabletserver.thrift.TabletScanClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;

import com.google.common.annotations.VisibleForTesting;

public class ThriftProcessorTypes {

  @VisibleForTesting
  public static class ProcessorType<C extends TServiceClient,F extends TServiceClientFactory<C>>
      extends ThriftClientType<C,F> {

    public ProcessorType(ThriftClientType<C,F> type) {
      super(type.getServiceName(), type.getClientFactory());
    }

    @VisibleForTesting
    public <I,H extends I,P extends TBaseProcessor<?>> TProcessor getTProcessor(
        Class<P> processorClass, Class<I> interfaceClass, H serviceHandler, ServerContext context,
        AccumuloConfiguration conf) {
      I rpcProxy = TraceUtil.wrapService(serviceHandler);
      if (context.getThriftServerType() == ThriftServerType.SASL) {
        @SuppressWarnings("unchecked")
        Class<H> clazz = (Class<H>) serviceHandler.getClass();
        rpcProxy = TCredentialsUpdatingWrapper.service(rpcProxy, clazz, conf);
      }
      try {
        return processorClass.getConstructor(interfaceClass).newInstance(rpcProxy);
      } catch (ReflectiveOperationException e) {
        throw new IllegalArgumentException("Error constructing TProcessor instance", e);
      }
    }
  }

  @VisibleForTesting
  public static final ProcessorType<ClientService.Client,ClientService.Client.Factory> CLIENT =
      new ProcessorType<>(ThriftClientTypes.CLIENT);

  private static final ProcessorType<CompactorService.Client,
      CompactorService.Client.Factory> COMPACTOR = new ProcessorType<>(ThriftClientTypes.COMPACTOR);

  private static final ProcessorType<CompactionCoordinatorService.Client,
      CompactionCoordinatorService.Client.Factory> COORDINATOR =
          new ProcessorType<>(ThriftClientTypes.COORDINATOR);

  private static final ProcessorType<FateService.Client,FateService.Client.Factory> FATE =
      new ProcessorType<>(ThriftClientTypes.FATE);

  private static final ProcessorType<GCMonitorService.Client,GCMonitorService.Client.Factory> GC =
      new ProcessorType<>(ThriftClientTypes.GC);

  private static final ProcessorType<ManagerClientService.Client,
      ManagerClientService.Client.Factory> MANAGER = new ProcessorType<>(ThriftClientTypes.MANAGER);

  private static final ProcessorType<ReplicationCoordinator.Client,
      ReplicationCoordinator.Client.Factory> REPLICATION_COORDINATOR =
          new ProcessorType<>(ThriftClientTypes.REPLICATION_COORDINATOR);

  private static final ProcessorType<ReplicationServicer.Client,
      ReplicationServicer.Client.Factory> REPLICATION_SERVICER =
          new ProcessorType<>(ThriftClientTypes.REPLICATION_SERVICER);

  @VisibleForTesting
  public static final ProcessorType<TabletClientService.Client,
      TabletClientService.Client.Factory> TABLET_SERVER =
          new ProcessorType<>(ThriftClientTypes.TABLET_SERVER);

  @VisibleForTesting
  public static final ProcessorType<TabletScanClientService.Client,
      TabletScanClientService.Client.Factory> TABLET_SERVER_SCAN =
          new ProcessorType<>(ThriftClientTypes.TABLET_SCAN);

  public static TMultiplexedProcessor getCompactorTProcessor(CompactorService.Iface serviceHandler,
      ServerContext context, AccumuloConfiguration conf) {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(COMPACTOR.getServiceName(),
        COMPACTOR.getTProcessor(CompactorService.Processor.class, CompactorService.Iface.class,
            serviceHandler, context, conf));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getCoordinatorTProcessor(
      CompactionCoordinatorService.Iface serviceHandler, ServerContext context,
      AccumuloConfiguration conf) {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(COORDINATOR.getServiceName(),
        COORDINATOR.getTProcessor(CompactionCoordinatorService.Processor.class,
            CompactionCoordinatorService.Iface.class, serviceHandler, context, conf));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getGcTProcessor(GCMonitorService.Iface serviceHandler,
      ServerContext context, AccumuloConfiguration conf) {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(GC.getServiceName(),
        GC.getTProcessor(GCMonitorService.Processor.class, GCMonitorService.Iface.class,
            serviceHandler, context, conf));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getManagerTProcessor(FateService.Iface fateServiceHandler,
      ManagerClientService.Iface managerServiceHandler, ServerContext context,
      AccumuloConfiguration conf) {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(FATE.getServiceName(), FATE.getTProcessor(
        FateService.Processor.class, FateService.Iface.class, fateServiceHandler, context, conf));
    muxProcessor.registerProcessor(MANAGER.getServiceName(),
        MANAGER.getTProcessor(ManagerClientService.Processor.class,
            ManagerClientService.Iface.class, managerServiceHandler, context, conf));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getReplicationCoordinatorTProcessor(
      ReplicationCoordinator.Iface serviceHandler, ServerContext context,
      AccumuloConfiguration conf) {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(REPLICATION_COORDINATOR.getServiceName(),
        REPLICATION_COORDINATOR.getTProcessor(ReplicationCoordinator.Processor.class,
            ReplicationCoordinator.Iface.class, serviceHandler, context, conf));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getReplicationClientTProcessor(
      ReplicationServicer.Iface serviceHandler, ServerContext context, AccumuloConfiguration conf) {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(REPLICATION_SERVICER.getServiceName(),
        REPLICATION_SERVICER.getTProcessor(ReplicationServicer.Processor.class,
            ReplicationServicer.Iface.class, serviceHandler, context, conf));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getScanServerTProcessor(
      TabletScanClientService.Iface tserverHandler, ServerContext context,
      AccumuloConfiguration conf) {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(TABLET_SERVER_SCAN.getServiceName(),
        TABLET_SERVER_SCAN.getTProcessor(TabletScanClientService.Processor.class,
            TabletScanClientService.Iface.class, tserverHandler, context, conf));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getTabletServerTProcessor(ClientServiceHandler clientHandler,
      TabletClientService.Iface tserverHandler, TabletScanClientService.Iface tserverScanHandler,
      ServerContext context, AccumuloConfiguration conf) {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(CLIENT.getServiceName(), CLIENT.getTProcessor(
        ClientService.Processor.class, ClientService.Iface.class, clientHandler, context, conf));
    muxProcessor.registerProcessor(TABLET_SERVER.getServiceName(),
        TABLET_SERVER.getTProcessor(TabletClientService.Processor.class,
            TabletClientService.Iface.class, tserverHandler, context, conf));
    muxProcessor.registerProcessor(TABLET_SERVER_SCAN.getServiceName(),
        TABLET_SERVER_SCAN.getTProcessor(TabletScanClientService.Processor.class,
            TabletScanClientService.Iface.class, tserverScanHandler, context, conf));
    return muxProcessor;
  }

}
