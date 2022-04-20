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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ThriftTransportKey;
import org.apache.accumulo.core.clientImpl.thrift.ClientService;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.CompactorService;
import org.apache.accumulo.core.gc.thrift.GCMonitorService;
import org.apache.accumulo.core.manager.thrift.FateService;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.manager.thrift.ManagerClientService.Client;
import org.apache.accumulo.core.replication.thrift.ReplicationCoordinator;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.fate.zookeeper.ServiceLock;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftClientTypes {
  
  private static final Logger LOG = LoggerFactory.getLogger(ThriftClientTypes.class);

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
    
    public C getManagerConnection(ClientContext context) {
      throw new UnsupportedOperationException("This method has not been implemented");
    }
    
    public C getManagerConnectionWithRetry(ClientContext context) throws AccumuloException {
      while (true) {

        C result = getManagerConnection(context);
        if (result != null)
          return result;
        sleepUninterruptibly(250, MILLISECONDS);
      }     
    }
    
    public Pair<String,C> getTabletServerConnection(ClientContext context, boolean preferCachedConnections) throws TTransportException{
      throw new UnsupportedOperationException("This method has not been implemented");
    }

  }

  public static final ThriftClientType<ClientService.Client,ClientService.Client.Factory> CLIENT =
      new ThriftClientType<>("ClientService", new ClientService.Client.Factory()) {
    volatile boolean warnedAboutTServersBeingDown = false;

    @Override
    public Pair<String,org.apache.accumulo.core.clientImpl.thrift.ClientService.Client>
        getTabletServerConnection(ClientContext context, boolean preferCachedConnections) throws TTransportException {
      checkArgument(context != null, "context is null");
      long rpcTimeout = context.getClientTimeoutInMillis();
      // create list of servers
      ArrayList<ThriftTransportKey> servers = new ArrayList<>();

      // add tservers
      ZooCache zc = context.getZooCache();
      for (String tserver : zc.getChildren(context.getZooKeeperRoot() + Constants.ZTSERVERS)) {
        var zLocPath =
            ServiceLock.path(context.getZooKeeperRoot() + Constants.ZTSERVERS + "/" + tserver);
        byte[] data = zc.getLockData(zLocPath);
        if (data != null) {
          String strData = new String(data, UTF_8);
          if (!strData.equals("manager"))
            servers.add(new ThriftTransportKey(
                new ServerServices(strData).getAddress(Service.TSERV_CLIENT), rpcTimeout, context));
        }
      }

      boolean opened = false;
      try {
        Pair<String,TTransport> pair =
            context.getTransportPool().getAnyTransport(servers, preferCachedConnections);
        var client = ThriftUtil.createClient(this, pair.getSecond());
        opened = true;
        warnedAboutTServersBeingDown = false;
        return new Pair<>(pair.getFirst(), client);
      } finally {
        if (!opened) {
          if (!warnedAboutTServersBeingDown) {
            if (servers.isEmpty()) {
              LOG.warn("There are no tablet servers: check that zookeeper and accumulo are running.");
            } else {
              LOG.warn("Failed to find an available server in the list of servers: {}", servers);
            }
            warnedAboutTServersBeingDown = true;
          }
        }
      }
    }
    
  };

  public static final ThriftClientType<CompactorService.Client,
      CompactorService.Client.Factory> COMPACTOR =
          new ThriftClientType<>("CompactorService", new CompactorService.Client.Factory());

  public static final ThriftClientType<CompactionCoordinatorService.Client,
      CompactionCoordinatorService.Client.Factory> COORDINATOR = new ThriftClientType<>(
          "CompactionCoordinatorService", new CompactionCoordinatorService.Client.Factory());

  public static final ThriftClientType<FateService.Client,FateService.Client.Factory> FATE =
      new ThriftClientType<>("FateService", new FateService.Client.Factory()) {

        @Override
        public org.apache.accumulo.core.manager.thrift.FateService.Client
            getManagerConnection(ClientContext context) {
          checkArgument(context != null, "context is null");

          List<String> locations = context.getManagerLocations();

          if (locations.isEmpty()) {
            LOG.debug("No managers...");
            return null;
          }

          HostAndPort manager = HostAndPort.fromString(locations.get(0));
          if (manager.getPort() == 0)
            return null;

          try {
            // Manager requests can take a long time: don't ever time out
            return ThriftUtil.getClientNoTimeout(ThriftClientTypes.FATE, manager, context);
          } catch (TTransportException tte) {
            Throwable cause = tte.getCause();
            if (cause != null && cause instanceof UnknownHostException) {
              // do not expect to recover from this
              throw new RuntimeException(tte);
            }
            LOG.debug("Failed to connect to manager=" + manager + ", will retry... ", tte);
            return null;
          }
        }
  };

  public static final ThriftClientType<GCMonitorService.Client,GCMonitorService.Client.Factory> GC =
      new ThriftClientType<>("GCMonitorService", new GCMonitorService.Client.Factory());

  public static final ThriftClientType<ManagerClientService.Client,
      ManagerClientService.Client.Factory> MANAGER =
          new ThriftClientType<>("ManagerClientService", new ManagerClientService.Client.Factory()) {

            @Override
            public Client getManagerConnection(ClientContext context) {
              checkArgument(context != null, "context is null");

              List<String> locations = context.getManagerLocations();

              if (locations.isEmpty()) {
                LOG.debug("No managers...");
                return null;
              }

              HostAndPort manager = HostAndPort.fromString(locations.get(0));
              if (manager.getPort() == 0)
                return null;

              try {
                // Manager requests can take a long time: don't ever time out
                return ThriftUtil.getClientNoTimeout(ThriftClientTypes.MANAGER, manager, context);
              } catch (TTransportException tte) {
                Throwable cause = tte.getCause();
                if (cause != null && cause instanceof UnknownHostException) {
                  // do not expect to recover from this
                  throw new RuntimeException(tte);
                }
                LOG.debug("Failed to connect to manager=" + manager + ", will retry... ", tte);
                return null;
              }

            }
    
  };

  public static final ThriftClientType<ReplicationCoordinator.Client,
      ReplicationCoordinator.Client.Factory> REPLICATION_COORDINATOR = new ThriftClientType<>(
          "ReplicationCoordinator", new ReplicationCoordinator.Client.Factory()) {

            @Override
            public org.apache.accumulo.core.replication.thrift.ReplicationCoordinator.Client
                getManagerConnection(ClientContext context) {
              List<String> locations = context.getManagerLocations();

              if (locations.isEmpty()) {
                LOG.debug("No managers for replication to instance {}", context.getInstanceName());
                return null;
              }

              // This is the manager thrift service, we just want the hostname, not the port
              String managerThriftService = locations.get(0);
              if (managerThriftService.endsWith(":0")) {
                LOG.warn("Manager found for {} did not have real location {}", context.getInstanceName(),
                    managerThriftService);
                return null;
              }

              String zkPath = context.getZooKeeperRoot() + Constants.ZMANAGER_REPLICATION_COORDINATOR_ADDR;
              String replCoordinatorAddr;

              LOG.debug("Using ZooKeeper quorum at {} with path {} to find peer Manager information",
                  context.getZooKeepers(), zkPath);

              // Get the coordinator port for the manager we're trying to connect to
              try {
                ZooReader reader = context.getZooReader();
                replCoordinatorAddr = new String(reader.getData(zkPath), UTF_8);
              } catch (KeeperException | InterruptedException e) {
                LOG.error("Could not fetch remote coordinator port", e);
                return null;
              }

              // Throw the hostname and port through HostAndPort to get some normalization
              HostAndPort coordinatorAddr = HostAndPort.fromString(replCoordinatorAddr);

              LOG.debug("Connecting to manager at {}", coordinatorAddr);

              try {
                // Manager requests can take a long time: don't ever time out
                return ThriftUtil.getClientNoTimeout(ThriftClientTypes.REPLICATION_COORDINATOR,
                    coordinatorAddr, context);
              } catch (TTransportException tte) {
                LOG.debug("Failed to connect to manager coordinator service ({})", coordinatorAddr, tte);
                return null;
              }
            }

            @Override
            public org.apache.accumulo.core.replication.thrift.ReplicationCoordinator.Client
                getManagerConnectionWithRetry(ClientContext context)  throws AccumuloException {
              requireNonNull(context);

              for (int attempts = 1; attempts <= 10; attempts++) {

                ReplicationCoordinator.Client result = getManagerConnection(context);
                if (result != null)
                  return result;
                LOG.debug("Could not get ReplicationCoordinator connection to {}, will retry",
                    context.getInstanceName());
                try {
                  Thread.sleep(attempts * 250L);
                } catch (InterruptedException e) {
                  throw new AccumuloException(e);
                }
              }

              throw new AccumuloException(
                  "Timed out trying to communicate with manager from " + context.getInstanceName());
            }
    
  };

  public static final ThriftClientType<ReplicationServicer.Client,
      ReplicationServicer.Client.Factory> REPLICATION_SERVICER =
          new ThriftClientType<>("ReplicationServicer", new ReplicationServicer.Client.Factory());

  public static final ThriftClientType<TabletClientService.Client,
      TabletClientService.Client.Factory> TABLET_SERVER =
          new ThriftClientType<>("TabletClientService", new TabletClientService.Client.Factory());

}
