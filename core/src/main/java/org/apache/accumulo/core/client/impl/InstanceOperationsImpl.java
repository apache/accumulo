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

import static com.google.common.base.Charsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.ConfigurationType;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Provides a class for administering the accumulo instance
 */
public class InstanceOperationsImpl implements InstanceOperations {
  private Instance instance;
  private Credentials credentials;

  /**
   * @param instance
   *          the connection information for this instance
   * @param credentials
   *          the Credential, containing principal and Authentication Token
   */
  public InstanceOperationsImpl(Instance instance, Credentials credentials) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.credentials = credentials;
  }

  @Override
  public void setProperty(final String property, final String value) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(property, value);
    MasterClient.execute(instance, new ClientExec<MasterClientService.Client>() {
      @Override
      public void execute(MasterClientService.Client client) throws Exception {
        client.setSystemProperty(Tracer.traceInfo(), credentials.toThrift(instance), property, value);
      }
    });
  }

  @Override
  public void removeProperty(final String property) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(property);
    MasterClient.execute(instance, new ClientExec<MasterClientService.Client>() {
      @Override
      public void execute(MasterClientService.Client client) throws Exception {
        client.removeSystemProperty(Tracer.traceInfo(), credentials.toThrift(instance), property);
      }
    });
  }

  @Override
  public Map<String,String> getSystemConfiguration() throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(instance, new ClientExecReturn<Map<String,String>,ClientService.Client>() {
      @Override
      public Map<String,String> execute(ClientService.Client client) throws Exception {
        return client.getConfiguration(Tracer.traceInfo(), credentials.toThrift(instance), ConfigurationType.CURRENT);
      }
    });
  }

  @Override
  public Map<String,String> getSiteConfiguration() throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(instance, new ClientExecReturn<Map<String,String>,ClientService.Client>() {
      @Override
      public Map<String,String> execute(ClientService.Client client) throws Exception {
        return client.getConfiguration(Tracer.traceInfo(), credentials.toThrift(instance), ConfigurationType.SITE);
      }
    });
  }

  @Override
  public List<String> getTabletServers() {
    ZooCache cache = new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    String path = ZooUtil.getRoot(instance) + Constants.ZTSERVERS;
    List<String> results = new ArrayList<String>();
    for (String candidate : cache.getChildren(path)) {
      List<String> children = cache.getChildren(path + "/" + candidate);
      if (children != null && children.size() > 0) {
        List<String> copy = new ArrayList<String>(children);
        Collections.sort(copy);
        byte[] data = cache.get(path + "/" + candidate + "/" + copy.get(0));
        if (data != null && !"master".equals(new String(data, UTF_8))) {
          results.add(candidate);
        }
      }
    }
    return results;
  }

  @Override
  public List<ActiveScan> getActiveScans(String tserver) throws AccumuloException, AccumuloSecurityException {
    Client client = null;
    try {
      client = ThriftUtil.getTServerClient(tserver, ServerConfigurationUtil.getConfiguration(instance));

      List<ActiveScan> as = new ArrayList<ActiveScan>();
      for (org.apache.accumulo.core.tabletserver.thrift.ActiveScan activeScan : client.getActiveScans(Tracer.traceInfo(), credentials.toThrift(instance))) {
        try {
          as.add(new ActiveScanImpl(instance, activeScan));
        } catch (TableNotFoundException e) {
          throw new AccumuloException(e);
        }
      }
      return as;
    } catch (TTransportException e) {
      throw new AccumuloException(e);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (TException e) {
      throw new AccumuloException(e);
    } finally {
      if (client != null)
        ThriftUtil.returnClient(client);
    }
  }

  @Override
  public boolean testClassLoad(final String className, final String asTypeName) throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(instance, new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.checkClass(Tracer.traceInfo(), credentials.toThrift(instance), className, asTypeName);
      }
    });
  }

  @Override
  public List<ActiveCompaction> getActiveCompactions(String tserver) throws AccumuloException, AccumuloSecurityException {
    Client client = null;
    try {
      client = ThriftUtil.getTServerClient(tserver, ServerConfigurationUtil.getConfiguration(instance));

      List<ActiveCompaction> as = new ArrayList<ActiveCompaction>();
      for (org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction activeCompaction : client.getActiveCompactions(Tracer.traceInfo(),
          credentials.toThrift(instance))) {
        as.add(new ActiveCompactionImpl(instance, activeCompaction));
      }
      return as;
    } catch (TTransportException e) {
      throw new AccumuloException(e);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (TException e) {
      throw new AccumuloException(e);
    } finally {
      if (client != null)
        ThriftUtil.returnClient(client);
    }
  }

  @Override
  public void ping(String tserver) throws AccumuloException {
    TTransport transport = null;
    try {
      transport = ThriftUtil.createTransport(AddressUtil.parseAddress(tserver, false), ServerConfigurationUtil.getConfiguration(instance));
      TabletClientService.Client client = ThriftUtil.createClient(new TabletClientService.Client.Factory(), transport);
      client.getTabletServerStatus(Tracer.traceInfo(), credentials.toThrift(instance));
    } catch (TTransportException e) {
      throw new AccumuloException(e);
    } catch (ThriftSecurityException e) {
      throw new AccumuloException(e);
    } catch (TException e) {
      throw new AccumuloException(e);
    } finally {
      if (transport != null) {
        transport.close();
      }
    }
  }
}
