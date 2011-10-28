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
package org.apache.accumulo.core.client.admin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.impl.ServerClient;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.ConfigurationType;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Iface;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.core.zookeeper.ZooLock;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.log4j.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;

/**
 * Provides a class for administering the accumulo instance
 */
public class InstanceOperations {
  private static final Logger log = Logger.getLogger(InstanceOperations.class);
  private Instance instance;
  private AuthInfo credentials;
  
  /**
   * @param instance
   *          the connection information for this instance
   * @param credentials
   *          the username/password for this connection
   */
  public InstanceOperations(Instance instance, AuthInfo credentials) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.credentials = credentials;
  }
  
  /**
   * Sets an instance property in zookeeper. Tablet servers will pull this setting and override the equivalent setting in accumulo-site.xml
   * 
   * @param property
   *          the name of a per-table property
   * @param value
   *          the value to set a per-table property to
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  public void setProperty(final String property, final String value) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(property, value);
    MasterClient.execute(instance, new ClientExec<MasterClientService.Iface>() {
      @Override
      public void execute(MasterClientService.Iface client) throws Exception {
        client.setSystemProperty(null, credentials, property, value);
      }
    });
  }
  
  /**
   * Removes a instance property from zookeeper
   * 
   * @param tableName
   *          the name of the table
   * @param property
   *          the name of a per-table property
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  public void removeProperty(final String property) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(property);
    MasterClient.execute(instance, new ClientExec<MasterClientService.Iface>() {
      @Override
      public void execute(MasterClientService.Iface client) throws Exception {
        client.removeSystemProperty(null, credentials, property);
      }
    });
  }
  
  public Map<String,String> getSystemConfiguration() throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(instance, new ClientExecReturn<Map<String,String>,ClientService.Iface>() {
      @Override
      public Map<String,String> execute(ClientService.Iface client) throws Exception {
        return client.getConfiguration(ConfigurationType.CURRENT);
      }
    });
  }
  
  public Map<String,String> getSiteConfiguration() throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(instance, new ClientExecReturn<Map<String,String>,ClientService.Iface>() {
      @Override
      public Map<String,String> execute(ClientService.Iface client) throws Exception {
        return client.getConfiguration(ConfigurationType.SITE);
      }
    });
  }
  
  /**
   * List the currently active tablet servers participating in the accumulo instance
   * 
   * @return
   */
  
  public List<String> getTabletServers() {
    ZooCache cache = ZooCache.getInstance(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    String path = ZooUtil.getRoot(instance) + Constants.ZTSERVERS;
    List<String> results = new ArrayList<String>();
    for (String candidate : cache.getChildren(path)) {
      try {
        byte[] data = ZooLock.getLockData(cache, path + "/" + candidate);
        if (data != null && !"master".equals(new String(data))) {
          results.add(candidate);
        }
      } catch (Exception ex) {
        log.error("Unable to read lock data:" + path);
      }
    }
    return results;
  }
  
  /**
   * List the active scans on tablet server. The tablet server address should be of the form <ip address>:<port>
   * 
   * @param tserver
   * @return
   * @throws AccumuloException
   * @throws AccumuloSecurityException
   */
  
  public List<ActiveScan> getActiveScans(String tserver) throws AccumuloException, AccumuloSecurityException {
    List<org.apache.accumulo.core.tabletserver.thrift.ActiveScan> tas = ThriftUtil.execute(tserver, instance.getConfiguration(),
        new ClientExecReturn<List<org.apache.accumulo.core.tabletserver.thrift.ActiveScan>,TabletClientService.Iface>() {
          @Override
          public List<org.apache.accumulo.core.tabletserver.thrift.ActiveScan> execute(Iface client) throws Exception {
            return client.getActiveScans(null, credentials);
          }
        });
    List<ActiveScan> as = new ArrayList<ActiveScan>();
    for (org.apache.accumulo.core.tabletserver.thrift.ActiveScan activeScan : tas) {
      try {
        as.add(new ActiveScan(instance, activeScan));
      } catch (TableNotFoundException e) {
        throw new AccumuloException(e);
      }
    }
    return as;
  }
  
  /**
   * Test to see if the instance can load the given class as the given type.
   * 
   * @param className
   * @param asTypeName
   * @return
   * @throws AccumuloException
   */
  public boolean testClassLoad(final String className, final String asTypeName) throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(instance, new ClientExecReturn<Boolean,ClientService.Iface>() {
      @Override
      public Boolean execute(ClientService.Iface client) throws Exception {
        return client.checkClass(null, className, asTypeName);
      }
    });
  }
}
