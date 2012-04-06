/*
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
package org.apache.accumulo.server.master.tserverOps;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.tabletserver.thrift.MutationLogger;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.fate.Repo;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.master.tableOps.MasterRepo;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.log4j.Logger;

public class DisconnectLogger extends MasterRepo {
  
  private static final long serialVersionUID = 1L;
  private static final Logger log = Logger.getLogger(DisconnectLogger.class);
  private String location;
  
  public DisconnectLogger(String location) {
    this.location = location;
  }
  
  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }
  
  @Override
  public Repo<Master> call(long tid, Master m) throws Exception {
    // TODO move this to isReady() and drop while loop?
    Map<String,String> loggers = m.getLoggers();
    boolean foundMatch = false;
    do {
      // TODO: doesn't work if there are multiple loggers on the same node
      for (Entry<String,String> entry : loggers.entrySet()) {
        InetSocketAddress addr = AddressUtil.parseAddress(entry.getValue(), 0);
        if (addr.getAddress().getHostAddress().equals(location)) {
          foundMatch = true;
          MutationLogger.Iface client = ThriftUtil.getClient(new MutationLogger.Client.Factory(), addr, m.getSystemConfiguration());
          try {
            client.beginShutdown(null, SecurityConstants.getSystemCredentials());
          } catch (Exception ex) {
            log.error("Unable to talk to logger at " + addr);
          } finally {
            ThriftUtil.returnClient(client);
          }
          String zpath = ZooUtil.getRoot(m.getInstance()) + Constants.ZLOGGERS + "/" + entry.getKey();
          ZooReaderWriter.getInstance().recursiveDelete(zpath, NodeMissingPolicy.SKIP);
          log.info("logger asked to halt " + location);
          return new FlushTablets(entry.getValue());
        }
      }
    } while (m.stillMaster() && foundMatch);
    return null;
  }
  
  @Override
  public void undo(long tid, Master m) throws Exception {}
  
}
