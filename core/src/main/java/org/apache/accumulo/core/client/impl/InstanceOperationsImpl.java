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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

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
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.TableInfoUtil;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.google.common.net.HostAndPort;

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
  public void setProperty(final String property, final String value) throws AccumuloException, AccumuloSecurityException, IllegalArgumentException {
    checkArgument(property != null, "property is null");
    checkArgument(value != null, "value is null");
    MasterClient.executeVoid(context, new ClientExec<MasterClientService.Client>() {
      @Override
      public void execute(MasterClientService.Client client) throws Exception {
        client.setSystemProperty(Tracer.traceInfo(), context.rpcCreds(), property, value);
      }
    });
  }

  @Override
  public void removeProperty(final String property) throws AccumuloException, AccumuloSecurityException {
    checkArgument(property != null, "property is null");
    MasterClient.executeVoid(context, new ClientExec<MasterClientService.Client>() {
      @Override
      public void execute(MasterClientService.Client client) throws Exception {
        client.removeSystemProperty(Tracer.traceInfo(), context.rpcCreds(), property);
      }
    });
  }

  @Override
  public Map<String,String> getSystemConfiguration() throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(context, new ClientExecReturn<Map<String,String>,ClientService.Client>() {
      @Override
      public Map<String,String> execute(ClientService.Client client) throws Exception {
        return client.getConfiguration(Tracer.traceInfo(), context.rpcCreds(), ConfigurationType.CURRENT);
      }
    });
  }

  @Override
  public Map<String,String> getSiteConfiguration() throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(context, new ClientExecReturn<Map<String,String>,ClientService.Client>() {
      @Override
      public Map<String,String> execute(ClientService.Client client) throws Exception {
        return client.getConfiguration(Tracer.traceInfo(), context.rpcCreds(), ConfigurationType.SITE);
      }
    });
  }

  @Override
  public List<String> getTabletServers() {
    Instance instance = context.getInstance();
    ZooCache cache = new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
    String path = ZooUtil.getRoot(instance) + Constants.ZTSERVERS;
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
  public List<ActiveScan> getActiveScans(String tserver) throws AccumuloException, AccumuloSecurityException {
    final HostAndPort parsedTserver = HostAndPort.fromString(tserver);
    Client client = null;
    try {
      client = ThriftUtil.getTServerClient(parsedTserver, context);

      List<ActiveScan> as = new ArrayList<>();
      for (org.apache.accumulo.core.tabletserver.thrift.ActiveScan activeScan : client.getActiveScans(Tracer.traceInfo(), context.rpcCreds())) {
        try {
          as.add(new ActiveScanImpl(context.getInstance(), activeScan));
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
    return ServerClient.execute(context, new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.checkClass(Tracer.traceInfo(), context.rpcCreds(), className, asTypeName);
      }
    });
  }

  @Override
  public List<ActiveCompaction> getActiveCompactions(String tserver) throws AccumuloException, AccumuloSecurityException {
    final HostAndPort parsedTserver = HostAndPort.fromString(tserver);
    Client client = null;
    try {
      client = ThriftUtil.getTServerClient(parsedTserver, context);

      List<ActiveCompaction> as = new ArrayList<>();
      for (org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction activeCompaction : client.getActiveCompactions(Tracer.traceInfo(), context.rpcCreds())) {
        as.add(new ActiveCompactionImpl(context.getInstance(), activeCompaction));
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
      TabletClientService.Client client = ThriftUtil.createClient(new TabletClientService.Client.Factory(), transport);
      client.getTabletServerStatus(Tracer.traceInfo(), context.rpcCreds());
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

  @Override
  public List<Map<String,String>> getTabletServerStatus() throws AccumuloException {
    List<Map<String,String>> status = new ArrayList<>();
    MasterMonitorInfo mmi = null;
    boolean retry = true;
    long now = System.currentTimeMillis();

    while (retry) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnection(context);
        if (client != null) {
          mmi = client.getMasterStats(Tracer.traceInfo(), context.rpcCreds());
          retry = false;
        } else {
          mmi = null;
        }
      } catch (Exception e) {
        mmi = null;
      } finally {
        if (client != null) {
          MasterClient.close(client);
        }
      }
      if (mmi == null) {
        break;
      }
    }
    if (mmi != null) {
      for (TabletServerStatus ts : mmi.getTServerInfo()) {

        TableInfo summary = TableInfoUtil.summarizeTableStats(ts);
        if (summary == null)
          return status;
        String name = ts.getName();
        String tablets = Integer.toString(summary.tablets);
        String lastContact = Long.toString(now - ts.lastContact);
        String entries = Long.toString(summary.recs);
        String ingest = Double.toString(summary.ingestRate);
        String query = Double.toString(summary.queryRate);
        String holdTime = Long.toString(ts.holdTime);
        String scans = summary.scans != null ? Integer.toString(summary.scans.running) : null;
        String minor = summary.minors != null ? Integer.toString(summary.minors.running) : null;
        String major = summary.majors != null ? Integer.toString(summary.majors.running) : null;
        String indexHitRate = Double.toString(ts.indexCacheHits / (double) Math.max(ts.indexCacheRequest, 1));
        String dataHitRate = Double.toString(ts.dataCacheHits / (double) Math.max(ts.dataCacheRequest, 1));
        String osLoad = Double.toString(ts.osLoad);
        String version = ts.version;

        Map<String,String> stat = new TreeMap<String,String>();
        stat.put("server", name);
        stat.put("tablets", tablets);
        stat.put("lastContact", lastContact);
        stat.put("entries", entries);
        stat.put("ingest", ingest);
        stat.put("query", query);
        stat.put("holdtime", holdTime);
        stat.put("scans", scans);
        stat.put("minor", minor);
        stat.put("major", major);
        stat.put("indexCacheHitRate", indexHitRate);
        stat.put("dataCacheHitRate", dataHitRate);
        stat.put("osload", osLoad);
        stat.put("version", version);

        status.add(stat);
      }
    }

    return status;
  }

  @Override
  public void waitForBalance() throws AccumuloException {
    try {
      MasterClient.executeVoid(context, new ClientExec<MasterClientService.Client>() {
        @Override
        public void execute(MasterClientService.Client client) throws Exception {
          client.waitForBalance(Tracer.traceInfo());
        }
      });
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
}
