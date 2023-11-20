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
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
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
    super(type.getServiceName(), type.getClientFactory());
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

  public static TMultiplexedProcessor getCompactorTProcessor(ClientServiceHandler clientHandler,
      CompactorService.Iface serviceHandler, ServerContext context) {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(CLIENT.getServiceName(), CLIENT.getTProcessor(
        ClientService.Processor.class, ClientService.Iface.class, clientHandler, context));
    muxProcessor.registerProcessor(COMPACTOR.getServiceName(), COMPACTOR.getTProcessor(
        CompactorService.Processor.class, CompactorService.Iface.class, serviceHandler, context));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getGcTProcessor(GCMonitorService.Iface serviceHandler,
      ServerContext context) {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(GC.getServiceName(), GC.getTProcessor(
        GCMonitorService.Processor.class, GCMonitorService.Iface.class, serviceHandler, context));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getManagerTProcessor(FateService.Iface fateServiceHandler,
      CompactionCoordinatorService.Iface coordinatorServiceHandler,
      ManagerClientService.Iface managerServiceHandler, ServerContext context) {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(FATE.getServiceName(), FATE.getTProcessor(
        FateService.Processor.class, FateService.Iface.class, fateServiceHandler, context));
    muxProcessor.registerProcessor(COORDINATOR.getServiceName(),
        COORDINATOR.getTProcessor(CompactionCoordinatorService.Processor.class,
            CompactionCoordinatorService.Iface.class, coordinatorServiceHandler, context));
    muxProcessor.registerProcessor(MANAGER.getServiceName(),
        MANAGER.getTProcessor(ManagerClientService.Processor.class,
            ManagerClientService.Iface.class, managerServiceHandler, context));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getScanServerTProcessor(ClientServiceHandler clientHandler,
      TabletScanClientService.Iface tserverHandler, ServerContext context) {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(CLIENT.getServiceName(), CLIENT.getTProcessor(
        ClientService.Processor.class, ClientService.Iface.class, clientHandler, context));
    muxProcessor.registerProcessor(TABLET_SCAN.getServiceName(),
        TABLET_SCAN.getTProcessor(TabletScanClientService.Processor.class,
            TabletScanClientService.Iface.class, tserverHandler, context));
    return muxProcessor;
  }

  public static TMultiplexedProcessor getTabletServerTProcessor(ClientServiceHandler clientHandler,
      TabletServerClientService.Iface tserverHandler,
      TabletScanClientService.Iface tserverScanHandler,
      TabletIngestClientService.Iface tserverIngestHandler,
      TabletManagementClientService.Iface tserverMgmtHandler, ServerContext context) {
    TMultiplexedProcessor muxProcessor = new TMultiplexedProcessor();
    muxProcessor.registerProcessor(CLIENT.getServiceName(), CLIENT.getTProcessor(
        ClientService.Processor.class, ClientService.Iface.class, clientHandler, context));
    muxProcessor.registerProcessor(TABLET_SERVER.getServiceName(),
        TABLET_SERVER.getTProcessor(TabletServerClientService.Processor.class,
            TabletServerClientService.Iface.class, tserverHandler, context));
    muxProcessor.registerProcessor(TABLET_SCAN.getServiceName(),
        TABLET_SCAN.getTProcessor(TabletScanClientService.Processor.class,
            TabletScanClientService.Iface.class, tserverScanHandler, context));
    muxProcessor.registerProcessor(TABLET_INGEST.getServiceName(),
        TABLET_INGEST.getTProcessor(TabletIngestClientService.Processor.class,
            TabletIngestClientService.Iface.class, tserverIngestHandler, context));
    muxProcessor.registerProcessor(TABLET_MGMT.getServiceName(),
        TABLET_MGMT.getTProcessor(TabletManagementClientService.Processor.class,
            TabletManagementClientService.Iface.class, tserverMgmtHandler, context));
    return muxProcessor;
  }

}
