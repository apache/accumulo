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
package org.apache.accumulo.core.client.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.core.zookeeper.ZooLock;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.log4j.Logger;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ServerClient {
  private static final Logger log = Logger.getLogger(ServerClient.class);
  private static final Map<String,ZooCache> zooCaches = new HashMap<String,ZooCache>();
  
  private synchronized static ZooCache getZooCache(Instance instance) {
    ZooCache result = zooCaches.get(instance.getZooKeepers());
    if (result == null) {
      result = new ZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut(), null);
      zooCaches.put(instance.getZooKeepers(), result);
    }
    return result;
  }
  
  public static synchronized ClientService.Iface getConnection(Instance instance) throws TTransportException {
    ArgumentChecker.notNull(instance);
    
    // create list of servers
    ArrayList<ThriftTransportKey> servers = new ArrayList<ThriftTransportKey>();
    
    // add tservers
    for (String tserver : getZooCache(instance).getChildren(ZooUtil.getRoot(instance) + Constants.ZTSERVERS)) {
      byte[] data = ZooLock.getLockData(getZooCache(instance), ZooUtil.getRoot(instance) + Constants.ZTSERVERS + "/" + tserver);
      if (data != null && !new String(data).equals("master"))
        servers.add(new ThriftTransportKey(new ServerServices(new String(data)).getAddressString(Service.TSERV_CLIENT), instance.getConfiguration().getPort(
            Property.TSERV_CLIENTPORT), instance.getConfiguration().getTimeInMillis(Property.GENERAL_RPC_TIMEOUT)));
    }
    
    // add masters
    for (String master : instance.getMasterLocations())
      servers.add(new ThriftTransportKey(master, instance.getConfiguration().getPort(Property.MASTER_CLIENTPORT), instance.getConfiguration().getTimeInMillis(
          Property.GENERAL_RPC_TIMEOUT)));
    
    boolean opened = false;
    try {
      TTransport socket = ThriftTransportPool.getInstance().getAnyTransport(servers);
      ClientService.Iface client = ThriftUtil.createClient(new ClientService.Client.Factory(), socket);
      opened = true;
      return client;
    } finally {
      if (!opened)
        log.warn("Failed to find an available server in the list of servers: " + servers);
    }
  }
  
  public static void close(ClientService.Iface iface) {
    TServiceClient client = (TServiceClient) iface;
    if (client != null && client.getInputProtocol() != null && client.getInputProtocol().getTransport() != null) {
      ThriftTransportPool.getInstance().returnTransport(client.getInputProtocol().getTransport());
    } else {
      log.debug("Attempt to close null connection to a server", new Exception());
    }
  }
}
