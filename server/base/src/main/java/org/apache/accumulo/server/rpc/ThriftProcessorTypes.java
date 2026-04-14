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
package org.apache.accumulo.server.rpc;

import org.apache.accumulo.core.clientImpl.thrift.ClientService;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.CompactorService;
import org.apache.accumulo.core.gc.thrift.GCMonitorService;
import org.apache.accumulo.core.manager.thrift.FateService;
import org.apache.accumulo.core.manager.thrift.FateWorkerService;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.process.thrift.ServerProcessService;
import org.apache.accumulo.core.rpc.AccumuloTMultiplexedProcessor;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tablet.thrift.TabletManagementClientService;
import org.apache.accumulo.core.tabletingest.thrift.TabletIngestClientService;
import org.apache.accumulo.core.tabletscan.thrift.TabletScanClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletServerClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.thrift.TBaseProcessor;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TServiceClient;

import com.google.common.annotations.VisibleForTesting;

@VisibleForTesting
public class ThriftProcessorTypes<C extends TServiceClient> extends ThriftClientTypes<C> {

  public ThriftProcessorTypes(ThriftClientTypes<C> type) {
    super(type.getService(), type.getClientFactory());
  }

  @VisibleForTesting
  public <I,H extends I,P extends TBaseProcessor<?>> TProcessor getTProcessor(
      Class<P> processorClass, Class<I> interfaceClass, H serviceHandler, ServerContext context) {
    I rpcProxy = TraceUtil.wrapService(serviceHandler);
    if (context.getThriftServerType() == ThriftServerType.SASL) {
      @SuppressWarnings("unchecked")
      Class<H> clazz = (Class<H>) serviceHandler.getClass();
      rpcProxy = TCredentialsUpdatingWrapper.service(rpcProxy, clazz, context.getConfiguration());
    }
    try {
      return processorClass.getConstructor(interfaceClass).newInstance(rpcProxy);
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException("Error constructing TProcessor instance", e);
    }
  }

  @VisibleForTesting
  public static final ThriftProcessorTypes<ClientService.Client> CLIENT =
      new ThriftProcessorTypes<>(ThriftClientTypes.CLIENT);

  private static final ThriftProcessorTypes<CompactorService.Client> COMPACTOR =
      new ThriftProcessorTypes<>(ThriftClientTypes.COMPACTOR);

  private static final ThriftProcessorTypes<CompactionCoordinatorService.Client> COORDINATOR =
      new ThriftProcessorTypes<>(ThriftClientTypes.COORDINATOR);

  private static final ThriftProcessorTypes<FateService.Client> FATE =
      new ThriftProcessorTypes<>(ThriftClientTypes.FATE);

  private static final ThriftProcessorTypes<GCMonitorService.Client> GC =
      new ThriftProcessorTypes<>(ThriftClientTypes.GC);

  private static final ThriftProcessorTypes<ManagerClientService.Client> MANAGER =
      new ThriftProcessorTypes<>(ThriftClientTypes.MANAGER);

  private static final ThriftProcessorTypes<FateWorkerService.Client> FATE_WORKER =
      new ThriftProcessorTypes<>(ThriftClientTypes.FATE_WORKER);

  @VisibleForTesting
  public static final ThriftProcessorTypes<TabletServerClientService.Client> TABLET_SERVER =
      new ThriftProcessorTypes<>(ThriftClientTypes.TABLET_SERVER);

  @VisibleForTesting
  public static final ThriftProcessorTypes<TabletScanClientService.Client> TABLET_SCAN =
      new ThriftProcessorTypes<>(ThriftClientTypes.TABLET_SCAN);

  public static final ThriftProcessorTypes<TabletIngestClientService.Client> TABLET_INGEST =
      new ThriftProcessorTypes<>(ThriftClientTypes.TABLET_INGEST);

  public static final ThriftProcessorTypes<TabletManagementClientService.Client> TABLET_MGMT =
      new ThriftProcessorTypes<>(ThriftClientTypes.TABLET_MGMT);

  public static final ThriftProcessorTypes<ServerProcessService.Client> SERVER_PROCESS =
      new ThriftProcessorTypes<>(ThriftClientTypes.SERVER_PROCESS);

  public static TMultiplexedProcessor getCompactorTProcessor(
      ServerProcessService.Iface processHandler, ClientServiceHandler clientHandler,
      CompactorService.Iface serviceHandler, ServerContext context) {
    AccumuloTMultiplexedProcessor muxProcessor = new AccumuloTMultiplexedProcessor();
    muxProcessor.registerProcessor(CLIENT.getService(), CLIENT.getTProcessor(
        ClientService.Processor.class, ClientService.Iface.class, clientHandler, context));
    muxProcessor.registerProcessor(SERVER_PROCESS.getService(),
        SERVER_PROCESS.getTProcessor(ServerProcessService.Processor.class,
            ServerProcessService.Iface.class, processHandler, context));
    muxProcessor.registerProcessor(COMPACTOR.getService(), COMPACTOR.getTProcessor(
        CompactorService.Processor.class, CompactorService.Iface.class, serviceHandler, context));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getGcTProcessor(ServerProcessService.Iface processHandler,
      GCMonitorService.Iface serviceHandler, ServerContext context) {
    AccumuloTMultiplexedProcessor muxProcessor = new AccumuloTMultiplexedProcessor();
    muxProcessor.registerProcessor(SERVER_PROCESS.getService(),
        SERVER_PROCESS.getTProcessor(ServerProcessService.Processor.class,
            ServerProcessService.Iface.class, processHandler, context));
    muxProcessor.registerProcessor(GC.getService(), GC.getTProcessor(
        GCMonitorService.Processor.class, GCMonitorService.Iface.class, serviceHandler, context));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getManagerTProcessor(
      ServerProcessService.Iface processHandler, FateService.Iface fateServiceHandler,
      CompactionCoordinatorService.Iface coordinatorServiceHandler,
      ManagerClientService.Iface managerServiceHandler, FateWorkerService.Iface fateWorkerService,
      ServerContext context) {
    AccumuloTMultiplexedProcessor muxProcessor = new AccumuloTMultiplexedProcessor();
    muxProcessor.registerProcessor(SERVER_PROCESS.getService(),
        SERVER_PROCESS.getTProcessor(ServerProcessService.Processor.class,
            ServerProcessService.Iface.class, processHandler, context));
    muxProcessor.registerProcessor(FATE.getService(), FATE.getTProcessor(
        FateService.Processor.class, FateService.Iface.class, fateServiceHandler, context));
    muxProcessor.registerProcessor(COORDINATOR.getService(),
        COORDINATOR.getTProcessor(CompactionCoordinatorService.Processor.class,
            CompactionCoordinatorService.Iface.class, coordinatorServiceHandler, context));
    muxProcessor.registerProcessor(MANAGER.getService(),
        MANAGER.getTProcessor(ManagerClientService.Processor.class,
            ManagerClientService.Iface.class, managerServiceHandler, context));
    muxProcessor.registerProcessor(FATE_WORKER.getService(),
        FATE_WORKER.getTProcessor(FateWorkerService.Processor.class, FateWorkerService.Iface.class,
            fateWorkerService, context));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getScanServerTProcessor(
      ServerProcessService.Iface processHandler, ClientServiceHandler clientHandler,
      TabletScanClientService.Iface tserverHandler, ServerContext context) {
    AccumuloTMultiplexedProcessor muxProcessor = new AccumuloTMultiplexedProcessor();
    muxProcessor.registerProcessor(CLIENT.getService(), CLIENT.getTProcessor(
        ClientService.Processor.class, ClientService.Iface.class, clientHandler, context));
    muxProcessor.registerProcessor(SERVER_PROCESS.getService(),
        SERVER_PROCESS.getTProcessor(ServerProcessService.Processor.class,
            ServerProcessService.Iface.class, processHandler, context));
    muxProcessor.registerProcessor(TABLET_SCAN.getService(),
        TABLET_SCAN.getTProcessor(TabletScanClientService.Processor.class,
            TabletScanClientService.Iface.class, tserverHandler, context));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getTabletServerTProcessor(
      ServerProcessService.Iface processHandler, ClientServiceHandler clientHandler,
      TabletServerClientService.Iface tserverHandler,
      TabletScanClientService.Iface tserverScanHandler,
      TabletIngestClientService.Iface tserverIngestHandler,
      TabletManagementClientService.Iface tserverMgmtHandler, ServerContext context) {
    AccumuloTMultiplexedProcessor muxProcessor = new AccumuloTMultiplexedProcessor();
    muxProcessor.registerProcessor(SERVER_PROCESS.getService(),
        SERVER_PROCESS.getTProcessor(ServerProcessService.Processor.class,
            ServerProcessService.Iface.class, processHandler, context));
    muxProcessor.registerProcessor(CLIENT.getService(), CLIENT.getTProcessor(
        ClientService.Processor.class, ClientService.Iface.class, clientHandler, context));
    muxProcessor.registerProcessor(TABLET_SERVER.getService(),
        TABLET_SERVER.getTProcessor(TabletServerClientService.Processor.class,
            TabletServerClientService.Iface.class, tserverHandler, context));
    muxProcessor.registerProcessor(TABLET_SCAN.getService(),
        TABLET_SCAN.getTProcessor(TabletScanClientService.Processor.class,
            TabletScanClientService.Iface.class, tserverScanHandler, context));
    muxProcessor.registerProcessor(TABLET_INGEST.getService(),
        TABLET_INGEST.getTProcessor(TabletIngestClientService.Processor.class,
            TabletIngestClientService.Iface.class, tserverIngestHandler, context));
    muxProcessor.registerProcessor(TABLET_MGMT.getService(),
        TABLET_MGMT.getTProcessor(TabletManagementClientService.Processor.class,
            TabletManagementClientService.Iface.class, tserverMgmtHandler, context));
    return muxProcessor;
  }

}
