/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.accumulo.core.rpc.ThriftUtil.createClient;
import static org.apache.accumulo.core.rpc.ThriftUtil.createTransport;
import static org.apache.accumulo.core.rpc.ThriftUtil.getTServerClient;
import static org.apache.accumulo.core.rpc.ThriftUtil.returnClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.ActiveCompaction.CompactionHost;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.TransactionStatus;
import org.apache.accumulo.core.clientImpl.thrift.AdminOperation;
import org.apache.accumulo.core.clientImpl.thrift.ConfigurationType;
import org.apache.accumulo.core.clientImpl.thrift.FateTransaction;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.DeprecatedPropertyUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Client;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
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
    DeprecatedPropertyUtil.getReplacementName(property, (log, replacement) -> {
      // force a warning on the client side, but send the name the user used to the server-side
      // to trigger a warning in the server logs, and to handle it there
      log.warn("{} was deprecated and will be removed in a future release;"
          + " setting its replacement {} instead", property, replacement);
    });
    ManagerClient.executeVoid(context, client -> client.setSystemProperty(TraceUtil.traceInfo(),
        context.rpcCreds(), property, value));
    checkLocalityGroups(property);
  }

  @Override
  public void removeProperty(final String property)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(property != null, "property is null");
    DeprecatedPropertyUtil.getReplacementName(property, (log, replacement) -> {
      // force a warning on the client side, but send the name the user used to the server-side
      // to trigger a warning in the server logs, and to handle it there
      log.warn("{} was deprecated and will be removed in a future release; assuming user meant"
          + " its replacement {} and will remove that instead", property, replacement);
    });
    ManagerClient.executeVoid(context,
        client -> client.removeSystemProperty(TraceUtil.traceInfo(), context.rpcCreds(), property));
    checkLocalityGroups(property);
  }

  private void checkLocalityGroups(String propChanged)
      throws AccumuloSecurityException, AccumuloException {
    if (LocalityGroupUtil.isLocalityGroupProperty(propChanged)) {
      try {
        LocalityGroupUtil.checkLocalityGroups(getSystemConfiguration());
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
    return ServerClient.execute(context, client -> client.getConfiguration(TraceUtil.traceInfo(),
        context.rpcCreds(), ConfigurationType.CURRENT));
  }

  @Override
  public Map<String,String> getSiteConfiguration()
      throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(context, client -> client.getConfiguration(TraceUtil.traceInfo(),
        context.rpcCreds(), ConfigurationType.SITE));
  }

  @Override
  public List<String> getManagerLocations() {
    return context.getManagerLocations();
  }

  @Override
  public List<String> getTabletServers() {
    ZooCache cache = context.getZooCache();
    String path = context.getZooKeeperRoot() + Constants.ZTSERVERS;
    List<String> results = new ArrayList<>();
    for (String candidate : cache.getChildren(path)) {
      var children = cache.getChildren(path + "/" + candidate);
      if (children != null && !children.isEmpty()) {
        var copy = new ArrayList<>(children);
        Collections.sort(copy);
        var data = cache.get(path + "/" + candidate + "/" + copy.get(0));
        if (data != null && !"manager".equals(new String(data, UTF_8))) {
          results.add(candidate);
        }
      }
    }
    return results;
  }

  @Override
  public List<ActiveScan> getActiveScans(String tserver)
      throws AccumuloException, AccumuloSecurityException {
    final var parsedTserver = HostAndPort.fromString(tserver);
    Client client = null;
    try {
      client = getTServerClient(parsedTserver, context);

      List<ActiveScan> as = new ArrayList<>();
      for (var activeScan : client.getActiveScans(TraceUtil.traceInfo(), context.rpcCreds())) {
        try {
          as.add(new ActiveScanImpl(context, activeScan));
        } catch (TableNotFoundException e) {
          throw new AccumuloException(e);
        }
      }
      return as;
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (TException e) {
      throw new AccumuloException(e);
    } finally {
      if (client != null)
        returnClient(client);
    }
  }

  @Override
  public boolean testClassLoad(final String className, final String asTypeName)
      throws AccumuloException, AccumuloSecurityException {
    return ServerClient.execute(context, client -> client.checkClass(TraceUtil.traceInfo(),
        context.rpcCreds(), className, asTypeName));
  }

  @Override
  public List<ActiveCompaction> getActiveCompactions(String tserver)
      throws AccumuloException, AccumuloSecurityException {
    final var parsedTserver = HostAndPort.fromString(tserver);
    Client client = null;
    try {
      client = getTServerClient(parsedTserver, context);

      List<ActiveCompaction> as = new ArrayList<>();
      for (var tac : client.getActiveCompactions(TraceUtil.traceInfo(), context.rpcCreds())) {
        as.add(new ActiveCompactionImpl(context, tac, parsedTserver, CompactionHost.Type.TSERVER));
      }
      return as;
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (TException e) {
      throw new AccumuloException(e);
    } finally {
      if (client != null)
        returnClient(client);
    }
  }

  @Override
  public List<ActiveCompaction> getActiveCompactions()
      throws AccumuloException, AccumuloSecurityException {

    List<HostAndPort> compactors = ExternalCompactionUtil.getCompactorAddrs(context);
    List<String> tservers = getTabletServers();

    int numThreads = Math.max(4, Math.min((tservers.size() + compactors.size()) / 10, 256));
    var executorService =
        ThreadPools.createFixedThreadPool(numThreads, "getactivecompactions", false);
    try {
      List<Future<List<ActiveCompaction>>> futures = new ArrayList<>();

      for (String tserver : tservers) {
        futures.add(executorService.submit(() -> getActiveCompactions(tserver)));
      }

      for (HostAndPort compactorAddr : compactors) {
        Callable<List<ActiveCompaction>> task =
            () -> ExternalCompactionUtil.getActiveCompaction(compactorAddr, context).stream()
                .map(tac -> new ActiveCompactionImpl(context, tac, compactorAddr,
                    CompactionHost.Type.COMPACTOR))
                .collect(toList());

        futures.add(executorService.submit(task));
      }

      List<ActiveCompaction> ret = new ArrayList<>();
      for (Future<List<ActiveCompaction>> future : futures) {
        try {
          ret.addAll(future.get());
        } catch (InterruptedException | ExecutionException e) {
          if (e.getCause() instanceof ThriftSecurityException) {
            ThriftSecurityException tse = (ThriftSecurityException) e.getCause();
            throw new AccumuloSecurityException(tse.user, tse.code, e);
          }
          throw new AccumuloException(e);
        }
      }

      return ret;

    } finally {
      executorService.shutdown();
    }
  }

  @Override
  public void ping(String tserver) throws AccumuloException {
    try (
        TTransport transport = createTransport(AddressUtil.parseAddress(tserver, false), context)) {
      var client = createClient(new Client.Factory(), transport);
      client.getTabletServerStatus(TraceUtil.traceInfo(), context.rpcCreds());
    } catch (TException e) {
      throw new AccumuloException(e);
    }
  }

  @Override
  public void waitForBalance() throws AccumuloException {
    try {
      ManagerClient.executeVoid(context, client -> client.waitForBalance(TraceUtil.traceInfo()));
    } catch (AccumuloSecurityException ex) {
      // should never happen
      throw new RuntimeException("Unexpected exception thrown", ex);
    }

  }

  @Override
  public void fateFail(List<String> txids) throws AccumuloException {
    checkArgument(txids != null, "txids is null");
    executeAdminOperation(AdminOperation.FAIL, txids, null);
  }

  @Override
  public void fateDelete(List<String> txids) throws AccumuloException {
    checkArgument(txids != null, "txids is null");
    executeAdminOperation(AdminOperation.DELETE, txids, null);
  }

  @Override
  public List<TransactionStatus> fatePrint(List<String> txids, List<String> tStatus)
      throws AccumuloException {
    checkArgument(txids != null, "txids is null");
    List<TransactionStatus> txStatus = new ArrayList<>();
    for (var tx : executeAdminOperation(AdminOperation.PRINT, txids, tStatus)) {
      txStatus.add(new TransactionStatus(tx.txid, tx.tstatus, tx.debug, tx.hlocks, tx.wlocks,
          tx.top, tx.timecreated));
    }
    return txStatus;
  }

  // @Override
  // public String fateDump(List<String> txids) throws AccumuloException {
  // checkArgument(txids != null, "txids is null");
  // return executeAdminOperation(AdminOperation.DUMP, txids, null);
  // }

  private List<FateTransaction> executeAdminOperation(AdminOperation op, List<String> txids,
      List<String> filterStatuses) throws AccumuloException {
    try {
      return ServerClient.execute(context,
          client -> client.executeAdminOperation(TraceUtil.traceInfo(), context.rpcCreds(), op,
              txids, filterStatuses));
    } catch (AccumuloSecurityException e) {
      throw new RuntimeException("Unexpected exception thrown", e);
    }

  }

  /**
   * Given a zooCache and instanceId, look up the instance name.
   */
  public static String lookupInstanceName(ZooCache zooCache, UUID instanceId) {
    checkArgument(zooCache != null, "zooCache is null");
    checkArgument(instanceId != null, "instanceId is null");
    for (String name : zooCache.getChildren(Constants.ZROOT + Constants.ZINSTANCES)) {
      var bytes = zooCache.get(Constants.ZROOT + Constants.ZINSTANCES + "/" + name);
      var iid = UUID.fromString(new String(bytes, UTF_8));
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
