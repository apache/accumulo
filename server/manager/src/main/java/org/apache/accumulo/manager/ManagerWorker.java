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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.lock.ServiceLockSupport;
import org.apache.accumulo.manager.fate.FateWorker;
import org.apache.accumulo.server.ServerContext;
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
// TODO because this does not extend abstract server it does not get some of the benefits like
// monitoring of lock
public class ManagerWorker /* extends AbstractServer */ {

  private static final Logger log = LoggerFactory.getLogger(ManagerWorker.class);
  private final ServerContext context;
  private final String bindAddress;
  private volatile ServiceLock managerWorkerLock;
  private FateWorker fateWorker;
  private volatile ServerAddress thriftServer;

  protected ManagerWorker(ServerContext context, String bindAddress) {
    this.context = context;
    this.bindAddress = bindAddress;
  }

  public ServerContext getContext() {
    return context;
  }

  private ResourceGroupId getResourceGroup() {
    return ResourceGroupId.DEFAULT;
  }

  private HostAndPort startClientService() throws UnknownHostException {
    fateWorker = new FateWorker(getContext());

    // This class implements TabletClientService.Iface and then delegates calls. Be sure
    // to set up the ThriftProcessor using this class, not the delegate.
    TProcessor processor =
        ThriftProcessorTypes.getManagerWorkerTProcessor(fateWorker, getContext());

    // TODO should the minthreads and timeout have their own props? Probably, do not expect this to
    // have lots of RPCs so could be less.
    var thriftServer =
        TServerUtils.createThriftServer(getContext(), bindAddress, Property.MANAGER_ASSISTANTPORT,
            processor, this.getClass().getSimpleName(), null, Property.MANAGER_MINTHREADS,
            Property.MANAGER_MINTHREADS_TIMEOUT, Property.MANAGER_THREADCHECK);
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
      // TODO shutdown supplier, anything to do here?
      ServiceLock.LockWatcher lw = new ServiceLockSupport.ServiceLockWatcher(
          ServerId.Type.MANAGER_ASSISTANT, () -> false,
          (type) -> getContext().getLowMemoryDetector().logGCInfo(getContext().getConfiguration()));

      for (int i = 0; i < 120 / 5; i++) {
        zoo.putPersistentData(zLockPath.toString(), new byte[0], ZooUtil.NodeExistsPolicy.SKIP);

        ServiceLockData.ServiceDescriptors descriptors = new ServiceLockData.ServiceDescriptors();
        for (ServiceLockData.ThriftService svc : new ServiceLockData.ThriftService[] {
            ServiceLockData.ThriftService.CLIENT, ServiceLockData.ThriftService.FATE_WORKER}) {
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
    fateWorker.setLock(getLock());
  }

  public void stop() {
    thriftServer.server.stop();
  }

  public ServiceLock getLock() {
    return managerWorkerLock;
  }
}
