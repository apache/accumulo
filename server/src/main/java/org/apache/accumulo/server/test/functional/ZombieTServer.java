/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.test.functional;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Random;

import org.apache.accumulo.cloudtrace.thrift.TInfo;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.util.TServerUtils;
import org.apache.accumulo.server.util.TServerUtils.ServerPort;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.server.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

/**
 * Tablet server that creates a lock in zookeeper, responds to one status request, and then hangs on subsequent requests. Exits with code zero if halted.
 */
public class ZombieTServer {
  
  public static class ThriftClientHandler extends org.apache.accumulo.server.test.performance.thrift.NullTserver.ThriftClientHandler {
    
    int statusCount = 0;
    
    boolean halted = false;

    ThriftClientHandler(Instance instance, TransactionWatcher watcher) {
      super(instance, watcher);
    }
    
    @Override
    synchronized public void fastHalt(TInfo tinfo, AuthInfo credentials, String lock) {
      halted = true;
      notifyAll();
    }
    
    @Override
    public TabletServerStatus getTabletServerStatus(TInfo tinfo, AuthInfo credentials) throws ThriftSecurityException, TException {
      synchronized (this) {
        if (statusCount++ < 1) {
          TabletServerStatus result = new TabletServerStatus();
          result.tableMap = new HashMap<String,TableInfo>();
          return result;
        }
      }
      UtilWaitThread.sleep(Integer.MAX_VALUE);
      return null;
    }
    
    @Override
    synchronized public void halt(TInfo tinfo, AuthInfo credentials, String lock) throws ThriftSecurityException, TException {
      halted = true;
      notifyAll();
    }
    
  }
  
  public static void main(String[] args) throws Exception {
    final Logger log = Logger.getLogger(ZombieTServer.class);
    Random random = new Random(System.currentTimeMillis() % 1000);
    int port = random.nextInt(30000) + 2000;
    Instance instance = HdfsZooInstance.getInstance();
    
    TransactionWatcher watcher = new TransactionWatcher();
    final ThriftClientHandler tch = new ThriftClientHandler(instance, watcher);
    TabletClientService.Processor processor = new TabletClientService.Processor(tch);
    ServerPort serverPort = TServerUtils.startTServer(port, processor, "ZombieTServer", "walking dead", 2, 1000);
    
    InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost(), serverPort.port);
    String addressString = AddressUtil.toString(addr);
    String zPath = ZooUtil.getRoot(instance) + Constants.ZTSERVERS + "/" + addressString;
    ZooReaderWriter zoo = ZooReaderWriter.getInstance();
    zoo.putPersistentData(zPath, new byte[] {}, NodeExistsPolicy.SKIP);
    
    ZooLock zlock = new ZooLock(zPath);
    
    LockWatcher lw = new LockWatcher() {
      @Override
      public void lostLock(final LockLossReason reason) {
        try {
          tch.halt(null, null, null);
        } catch (Exception ex) {
          log.error(ex, ex);
          System.exit(1);
        }
      }
    };
    
    byte[] lockContent = new ServerServices(addressString, Service.TSERV_CLIENT).toString().getBytes();
    if (zlock.tryLock(lw, lockContent)) {
      log.debug("Obtained tablet server lock " + zlock.getLockPath());
    }
    // modify !METADATA
    while (!tch.halted) {
      synchronized (tch) {
        tch.wait();
      }
    }
    System.exit(0);
  }
}
