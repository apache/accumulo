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
package org.apache.accumulo.core.rpc.clients;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.ClientContext;
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

    /**
     * execute method with supplied client returning object of type R
     *
     * @param <R>
     *          return type
     * @param <C>
     *          client type
     */
    public interface Exec<R,C> {
      R execute(C client) throws Exception;
    }

    /**
     * execute method with supplied client
     *
     * @param <C>
     *          client type
     */
    public interface ExecVoid<C> {
      void execute(C client) throws Exception;
    }

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

    public C getConnection(ClientContext context) {
      throw new UnsupportedOperationException("This method has not been implemented");
    }

    public C getConnectionWithRetry(ClientContext context) {
      while (true) {

        C result = getConnection(context);
        if (result != null)
          return result;
        sleepUninterruptibly(250, MILLISECONDS);
      }
    }

    public <R> R execute(ClientContext context, Exec<R,C> exec)
        throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException("This method has not been implemented");
    }

    public void executeVoid(ClientContext context, ExecVoid<C> exec)
        throws AccumuloException, AccumuloSecurityException {
      throw new UnsupportedOperationException("This method has not been implemented");
    }

  }

  public static final ClientServiceThriftClient CLIENT =
      new ClientServiceThriftClient("ClientService", new ClientService.Client.Factory());

  public static final ThriftClientType<CompactorService.Client,
      CompactorService.Client.Factory> COMPACTOR =
          new ThriftClientType<>("CompactorService", new CompactorService.Client.Factory());

  public static final ThriftClientType<CompactionCoordinatorService.Client,
      CompactionCoordinatorService.Client.Factory> COORDINATOR = new ThriftClientType<>(
          "CompactionCoordinatorService", new CompactionCoordinatorService.Client.Factory());

  public static final FateThriftClient FATE =
      new FateThriftClient("FateService", new FateService.Client.Factory());

  public static final ThriftClientType<GCMonitorService.Client,GCMonitorService.Client.Factory> GC =
      new ThriftClientType<>("GCMonitorService", new GCMonitorService.Client.Factory());

  public static final ManagerThriftClient MANAGER =
      new ManagerThriftClient("ManagerClientService", new ManagerClientService.Client.Factory());

  public static final ReplicationCoordinatorThriftClient REPLICATION_COORDINATOR =
      new ReplicationCoordinatorThriftClient("ReplicationCoordinator",
          new ReplicationCoordinator.Client.Factory());

  public static final ThriftClientType<ReplicationServicer.Client,
      ReplicationServicer.Client.Factory> REPLICATION_SERVICER =
          new ThriftClientType<>("ReplicationServicer", new ReplicationServicer.Client.Factory());

  public static final TabletServerThriftClient TABLET_SERVER =
      new TabletServerThriftClient("TabletClientService", new TabletClientService.Client.Factory());

  public static final ThriftClientType<TabletScanClientService.Client,
      TabletScanClientService.Client.Factory> TABLET_SCAN = new ThriftClientType<>(
          "TabletScanClientService", new TabletScanClientService.Client.Factory());
}
