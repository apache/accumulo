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
package org.apache.accumulo.manager;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.lock.ServiceLockSupport;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.manager.fate.FateWorker;
import org.apache.accumulo.manager.fate.FateWorker.FateFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

/**
 * An assistant to the manager
 */
// FOLLOW_ON because this does not extend abstract server it does not get some of the benefits like
// monitoring of lock
public class ManagerAssistant {

  private static final Logger log = LoggerFactory.getLogger(ManagerAssistant.class);
  private final ServerContext context;
  private final String bindAddress;
  private final LiveTServerSet liveTServerSet;
  private final FateFactory fateFactory;
  private final Supplier<Boolean> shutdownComplete;
  private volatile ServiceLock managerWorkerLock;
  private FateWorker fateWorker;
  private ServerAddress thriftServer;

  protected ManagerAssistant(ServerContext context, String bindAddress,
      LiveTServerSet liveTServerSet, FateFactory fateFactory, Supplier<Boolean> shutdownComplete) {
    // Create another server context because the server context has the lock info and this class
    // creates another lock separate from the manager lock.
    // FOLLOW_ON creating another context instance in the process may cause problems, like
    // duplicating some thread pools
    this.context = new ServerContext(context.getSiteConfiguration());
    this.bindAddress = bindAddress;
    this.liveTServerSet = liveTServerSet;
    this.fateFactory = fateFactory;
    this.shutdownComplete = shutdownComplete;
  }

  public ServerContext getContext() {
    return context;
  }

  private ResourceGroupId getResourceGroup() {
    return ResourceGroupId.DEFAULT;
  }

  private HostAndPort startClientService() throws UnknownHostException {
    fateWorker = new FateWorker(getContext(), liveTServerSet, fateFactory);

    // This class implements TabletClientService.Iface and then delegates calls. Be sure
    // to set up the ThriftProcessor using this class, not the delegate.
    TProcessor processor =
        ThriftProcessorTypes.getManagerWorkerTProcessor(fateWorker, getContext());

    thriftServer = TServerUtils.createThriftServer(getContext(), bindAddress,
        Property.MANAGER_ASSISTANT_PORT, processor, this.getClass().getSimpleName(),
        Property.MANAGER_ASSISTANT_PORTSEARCH, Property.MANAGER_ASSISTANT_MINTHREADS,
        Property.MANAGER_ASSISTANT_MINTHREADS_TIMEOUT, Property.MANAGER_THREADCHECK);
    thriftServer.startThriftServer("Thrift Manager Assistant Server");
    log.info("Starting {} Thrift server, listening on {}", this.getClass().getSimpleName(),
        thriftServer.address);
    return thriftServer.address;
  }

  private void announceExistence(HostAndPort advertiseAddress) {
    final ZooReaderWriter zoo = getContext().getZooSession().asReaderWriter();
    try {

      final ServiceLockPaths.ServiceLockPath zLockPath = getContext().getServerPaths()
          .createManagerWorkerPath(getResourceGroup(), advertiseAddress);
      ServiceLockSupport.createNonHaServiceLockPath(ServerId.Type.MANAGER_ASSISTANT, zoo,
          zLockPath);
      var serverLockUUID = UUID.randomUUID();
      managerWorkerLock = new ServiceLock(getContext().getZooSession(), zLockPath, serverLockUUID);
      ServiceLock.LockWatcher lw = new ServiceLockSupport.ServiceLockWatcher(
          ServerId.Type.MANAGER_ASSISTANT, shutdownComplete,
          (type) -> getContext().getLowMemoryDetector().logGCInfo(getContext().getConfiguration()));

      for (int i = 0; i < 120 / 5; i++) {
        zoo.putPersistentData(zLockPath.toString(), new byte[0], ZooUtil.NodeExistsPolicy.SKIP);

        ServiceLockData.ServiceDescriptors descriptors = new ServiceLockData.ServiceDescriptors();
        for (ServiceLockData.ThriftService svc : new ServiceLockData.ThriftService[] {
            ServiceLockData.ThriftService.CLIENT,
            ServiceLockData.ThriftService.MANAGER_ASSISTANT}) {
          descriptors.addService(new ServiceLockData.ServiceDescriptor(serverLockUUID, svc,
              advertiseAddress.toString(), this.getResourceGroup()));
        }

        if (managerWorkerLock.tryLock(lw, new ServiceLockData(descriptors))) {
          log.debug("Obtained manager assistant lock {}", managerWorkerLock.getLockPath());
          return;
        }
        log.info("Waiting for manager assistant lock");
        sleepUninterruptibly(5, TimeUnit.SECONDS);
      }
      String msg = "Too many retries, exiting.";
      log.info(msg);
      throw new RuntimeException(msg);
    } catch (Exception e) {
      log.info("Could not obtain manager assistant lock, exiting.", e);
      throw new RuntimeException(e);
    }
  }

  public void start() {
    HostAndPort advertiseAddress;
    try {
      advertiseAddress = startClientService();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the manager assistant client service", e1);
    }

    announceExistence(advertiseAddress);
    context.setServiceLock(getLock());
    fateWorker.setLock(getLock());
  }

  public void stop() {
    thriftServer.server.stop();
    fateWorker.stop();
  }

  public ServiceLock getLock() {
    return managerWorkerLock;
  }

  public List<MetricsProducer> getMetricsProducers() {
    return fateWorker.getMetricsProducers();
  }
}
