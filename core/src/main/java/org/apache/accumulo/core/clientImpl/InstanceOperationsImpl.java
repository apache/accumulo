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
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.clientImpl.thrift.ConfigurationType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.LoggerFactory;

/**
 * Provides a class for administering the accumulo instance
 */
public class InstanceOperationsImpl implements InstanceOperations {
  private final ClientContext context;

  public InstanceOperationsImpl(ClientContext context) {
    checkArgument(context != null, "context is null");
    this.context = context;
  }

  @Override
  public void setProperty(final String property, final String value)
      throws AccumuloException, AccumuloSecurityException, IllegalArgumentException {
    checkArgument(property != null, "property is null");
    checkArgument(value != null, "value is null");
    MasterClient.executeVoid(context, client -> client.setSystemProperty(Tracer.traceInfo(),
        context.rpcCreds(), property, value));
    checkLocalityGroups(property);
  }

  @Override
  public void removeProperty(final String property)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(property != null, "property is null");
    MasterClient.executeVoid(context,
        client -> client.removeSystemProperty(Tracer.traceInfo(), context.rpcCreds(), property));
    checkLocalityGroups(property);
  }

  void checkLocalityGroups(String propChanged) throws AccumuloSecurityException, AccumuloException {
    if (LocalityGroupUtil.isLocalityGroupProperty(propChanged)) {
      try {
        LocalityGroupUtil.checkLocalityGroups(getSystemConfiguration().entrySet());
      } catch (LocalityGroupConfigurationError | RuntimeException e) {
        LoggerFactory.getLogger(this.getClass()).warn("Changing '" + propChanged
            + "' resulted in bad locality group config. This may be a transient situation since "
            + "the config spreads over multiple properties. Setting properties in a different "
            + "order may help. Even though this warning was displayed, the property was updated. "
            + "Please check your config to ensure consistency.", e);
      }
    }
  }

  @Override
  public Map<String,String> getSystemConfiguration()
      throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(context, client -> client.getConfiguration(Tracer.traceInfo(),
        context.rpcCreds(), ConfigurationType.CURRENT));
  }

  @Override
  public Map<String,String> getSiteConfiguration()
      throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(context, client -> client.getConfiguration(Tracer.traceInfo(),
        context.rpcCreds(), ConfigurationType.SITE));
  }

  @Override
  public List<String> getTabletServers() {
    ZooCache cache = context.getZooCache();
    String path = context.getZooKeeperRoot() + Constants.ZTSERVERS;
    List<String> results = new ArrayList<>();
    for (String candidate : cache.getChildren(path)) {
      List<String> children = cache.getChildren(path + "/" + candidate);
      if (children != null && children.size() > 0) {
        List<String> copy = new ArrayList<>(children);
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
  public List<ActiveScan> getActiveScans(String tserver)
      throws AccumuloException, AccumuloSecurityException {
    final HostAndPort parsedTserver = HostAndPort.fromString(tserver);
    Client client = null;
    try {
      client = ThriftUtil.getTServerClient(parsedTserver, context);

      List<ActiveScan> as = new ArrayList<>();
      for (org.apache.accumulo.core.tabletserver.thrift.ActiveScan activeScan : client
          .getActiveScans(Tracer.traceInfo(), context.rpcCreds())) {
        try {
          as.add(new ActiveScanImpl(context, activeScan));
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
  public boolean testClassLoad(final String className, final String asTypeName)
      throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(context,
        client -> client.checkClass(Tracer.traceInfo(), context.rpcCreds(), className, asTypeName));
  }

  @Override
  public List<ActiveCompaction> getActiveCompactions(String tserver)
      throws AccumuloException, AccumuloSecurityException {
    final HostAndPort parsedTserver = HostAndPort.fromString(tserver);
    Client client = null;
    try {
      client = ThriftUtil.getTServerClient(parsedTserver, context);

      List<ActiveCompaction> as = new ArrayList<>();
      for (org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction activeCompaction : client
          .getActiveCompactions(Tracer.traceInfo(), context.rpcCreds())) {
        as.add(new ActiveCompactionImpl(context, activeCompaction));
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
      transport = ThriftUtil.createTransport(AddressUtil.parseAddress(tserver, false), context);
      TabletClientService.Client client = ThriftUtil
          .createClient(new TabletClientService.Client.Factory(), transport);
      client.getTabletServerStatus(Tracer.traceInfo(), context.rpcCreds());
    } catch (TException e) {
      throw new AccumuloException(e);
    } finally {
      if (transport != null) {
        transport.close();
      }
    }
  }

  @Override
  public void waitForBalance() throws AccumuloException {
    try {
      MasterClient.executeVoid(context, client -> client.waitForBalance(Tracer.traceInfo()));
    } catch (AccumuloSecurityException ex) {
      // should never happen
      throw new RuntimeException("Unexpected exception thrown", ex);
    }

  }

  /**
   * Given a zooCache and instanceId, look up the instance name.
   */
  public static String lookupInstanceName(ZooCache zooCache, UUID instanceId) {
    checkArgument(zooCache != null, "zooCache is null");
    checkArgument(instanceId != null, "instanceId is null");
    for (String name : zooCache.getChildren(Constants.ZROOT + Constants.ZINSTANCES)) {
      String instanceNamePath = Constants.ZROOT + Constants.ZINSTANCES + "/" + name;
      byte[] bytes = zooCache.get(instanceNamePath);
      UUID iid = UUID.fromString(new String(bytes, UTF_8));
      if (iid.equals(instanceId)) {
        return name;
      }
    }
    return null;
  }

  @Override
  public String getInstanceID() {

    return context.getInstanceID();
  }
}
