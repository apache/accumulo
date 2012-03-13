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
import java.util.Collections;
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
import org.apache.accumulo.core.zookeeper.ZooUtil;

/**
 * Provides a class for administering the accumulo instance
 */
public class InstanceOperationsImpl implements InstanceOperations {
  private Instance instance;
  private AuthInfo credentials;
  
  /**
   * @param instance
   *          the connection information for this instance
   * @param credentials
   *          the username/password for this connection
   */
  public InstanceOperationsImpl(Instance instance, AuthInfo credentials) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.credentials = credentials;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#setProperty(java.lang.String, java.lang.String)
   */
  @Override
  public void setProperty(final String property, final String value) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(property, value);
    MasterClient.execute(instance, new ClientExec<MasterClientService.Iface>() {
      @Override
      public void execute(MasterClientService.Iface client) throws Exception {
        client.setSystemProperty(null, credentials, property, value);
      }
    });
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#removeProperty(java.lang.String)
   */
  @Override
  public void removeProperty(final String property) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(property);
    MasterClient.execute(instance, new ClientExec<MasterClientService.Iface>() {
      @Override
      public void execute(MasterClientService.Iface client) throws Exception {
        client.removeSystemProperty(null, credentials, property);
      }
    });
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#getSystemConfiguration()
   */
  @Override
  public Map<String,String> getSystemConfiguration() throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(instance, new ClientExecReturn<Map<String,String>,ClientService.Iface>() {
      @Override
      public Map<String,String> execute(ClientService.Iface client) throws Exception {
        return client.getConfiguration(ConfigurationType.CURRENT);
      }
    });
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#getSiteConfiguration()
   */
  @Override
  public Map<String,String> getSiteConfiguration() throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(instance, new ClientExecReturn<Map<String,String>,ClientService.Iface>() {
      @Override
      public Map<String,String> execute(ClientService.Iface client) throws Exception {
        return client.getConfiguration(ConfigurationType.SITE);
      }
    });
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#getTabletServers()
   */
  
  @Override
  public List<String> getTabletServers() {
    ZooCache cache = ZooCache.getInstance(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    String path = ZooUtil.getRoot(instance) + Constants.ZTSERVERS;
    List<String> results = new ArrayList<String>();
    for (String candidate : cache.getChildren(path)) {
      List<String> children = cache.getChildren(path + "/" + candidate);
      if (children != null && children.size() > 0) {
        List<String> copy = new ArrayList<String>(children);
        Collections.sort(copy);
        byte[] data = cache.get(path + "/" + candidate + "/" + copy.get(0));
        if (data != null && !"master".equals(new String(data))) {
          results.add(candidate);
        }
      }
    }
    return results;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#getActiveScans(java.lang.String)
   */
  
  @Override
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
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.core.client.admin.InstanceOperations#testClassLoad(java.lang.String, java.lang.String)
   */
  @Override
  public boolean testClassLoad(final String className, final String asTypeName) throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(instance, new ClientExecReturn<Boolean,ClientService.Iface>() {
      @Override
      public Boolean execute(ClientService.Iface client) throws Exception {
        return client.checkClass(null, className, asTypeName);
      }
    });
  }
}
