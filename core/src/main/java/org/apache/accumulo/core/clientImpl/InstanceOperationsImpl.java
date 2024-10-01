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
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.rpc.ThriftUtil.createClient;
import static org.apache.accumulo.core.rpc.ThriftUtil.createTransport;
import static org.apache.accumulo.core.rpc.ThriftUtil.getClient;
import static org.apache.accumulo.core.rpc.ThriftUtil.returnClient;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.INSTANCE_OPS_COMPACTIONS_FINDER_POOL;

import java.time.Duration;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.admin.servers.ServerTypeName;
import org.apache.accumulo.core.clientImpl.thrift.ConfigurationType;
import org.apache.accumulo.core.clientImpl.thrift.TVersionedProperties;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.DeprecatedPropertyUtil;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletscan.thrift.TabletScanClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletServerClientService.Client;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
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
    ThriftClientTypes.MANAGER.executeVoid(context, client -> client
        .setSystemProperty(TraceUtil.traceInfo(), context.rpcCreds(), property, value));
    checkLocalityGroups(property);
  }

  private Map<String,String> tryToModifyProperties(final Consumer<Map<String,String>> mapMutator)
      throws AccumuloException, AccumuloSecurityException, IllegalArgumentException {
    checkArgument(mapMutator != null, "mapMutator is null");

    final TVersionedProperties vProperties = ThriftClientTypes.CLIENT.execute(context,
        client -> client.getVersionedSystemProperties(TraceUtil.traceInfo(), context.rpcCreds()));
    mapMutator.accept(vProperties.getProperties());

    // A reference to the map was passed to the user, maybe they still have the reference and are
    // modifying it. Buggy Accumulo code could attempt to make modifications to the map after this
    // point. Because of these potential issues, create an immutable snapshot of the map so that
    // from here on the code is assured to always be dealing with the same map.
    vProperties.setProperties(Map.copyOf(vProperties.getProperties()));

    for (Map.Entry<String,String> entry : vProperties.getProperties().entrySet()) {
      final String property = Objects.requireNonNull(entry.getKey(), "property key is null");

      DeprecatedPropertyUtil.getReplacementName(property, (log, replacement) -> {
        // force a warning on the client side, but send the name the user used to the
        // server-side
        // to trigger a warning in the server logs, and to handle it there
        log.warn("{} was deprecated and will be removed in a future release;"
            + " setting its replacement {} instead", property, replacement);
      });
      checkLocalityGroups(property);
    }

    // Send to server
    ThriftClientTypes.MANAGER.executeVoid(context, client -> client
        .modifySystemProperties(TraceUtil.traceInfo(), context.rpcCreds(), vProperties));

    return vProperties.getProperties();
  }

  @Override
  public Map<String,String> modifyProperties(final Consumer<Map<String,String>> mapMutator)
      throws AccumuloException, AccumuloSecurityException, IllegalArgumentException {

    var log = LoggerFactory.getLogger(InstanceOperationsImpl.class);

    Retry retry = Retry.builder().infiniteRetries().retryAfter(Duration.ofMillis(25))
        .incrementBy(Duration.ofMillis(25)).maxWait(Duration.ofSeconds(30)).backOffFactor(1.5)
        .logInterval(Duration.ofMinutes(3)).createRetry();

    while (true) {
      try {
        var props = tryToModifyProperties(mapMutator);
        retry.logCompletion(log, "Modifying instance properties");
        return props;
      } catch (ConcurrentModificationException cme) {
        try {
          retry.logRetry(log,
              "Unable to modify instance properties for because of concurrent modification");
          retry.waitForNextAttempt(log, "Modify instance properties");
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      } finally {
        retry.useRetry();
      }
    }

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
    ThriftClientTypes.MANAGER.executeVoid(context,
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
    return ThriftClientTypes.CLIENT.execute(context, client -> client
        .getConfiguration(TraceUtil.traceInfo(), context.rpcCreds(), ConfigurationType.CURRENT));
  }

  @Override
  public Map<String,String> getSiteConfiguration()
      throws AccumuloException, AccumuloSecurityException {
    return ThriftClientTypes.CLIENT.execute(context, client -> client
        .getConfiguration(TraceUtil.traceInfo(), context.rpcCreds(), ConfigurationType.SITE));
  }

  @Override
  public Map<String,String> getSystemProperties()
      throws AccumuloException, AccumuloSecurityException {
    return ThriftClientTypes.CLIENT.execute(context,
        client -> client.getSystemProperties(TraceUtil.traceInfo(), context.rpcCreds()));
  }

  @Override
  @Deprecated(since = "4.0.0")
  public List<String> getManagerLocations() {
    ServiceLockPath slp = context.getServerPaths().getManager(true);
    if (slp == null) {
      return null;
    } else {
      return List.of(slp.getServer());
    }
  }

  @Override
  @Deprecated(since = "4.0.0")
  public Set<String> getCompactors() {
    Set<String> results = new HashSet<>();
    context.getServerPaths().getCompactor(Optional.empty(), Optional.empty(), true)
        .forEach(t -> results.add(t.getServer()));
    return results;
  }

  @Override
  @Deprecated(since = "4.0.0")
  public Set<String> getScanServers() {
    Set<String> results = new HashSet<>();
    context.getServerPaths().getScanServer(Optional.empty(), Optional.empty(), true)
        .forEach(t -> results.add(t.getServer()));
    return results;
  }

  @Override
  @Deprecated(since = "4.0.0")
  public List<String> getTabletServers() {
    List<String> results = new ArrayList<>();
    context.getServerPaths().getTabletServer(Optional.empty(), Optional.empty(), true)
        .forEach(t -> results.add(t.getServer()));
    return results;
  }

  @Override
  @Deprecated(since = "4.0.0")
  public List<ActiveScan> getActiveScans(String tserver)
      throws AccumuloException, AccumuloSecurityException {
    final var parsedTserver = HostAndPort.fromString(tserver);
    TabletScanClientService.Client client = null;
    try {
      client = getClient(ThriftClientTypes.TABLET_SCAN, parsedTserver, context);

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
      if (client != null) {
        returnClient(client, context);
      }
    }
  }

  @Override
  public List<ActiveScan> getActiveScans(ServerId server)
      throws AccumuloException, AccumuloSecurityException {

    @SuppressWarnings("unused")
    var unused = Objects.nonNull(server);
    Preconditions.checkArgument(server.getType() == ServerTypeName.SCAN_SERVER
        || server.getType() == ServerTypeName.TABLET_SERVER);

    final var parsedTserver = HostAndPort.fromParts(server.getHost(), server.getPort());
    TabletScanClientService.Client client = null;
    try {
      client = getClient(ThriftClientTypes.TABLET_SCAN, parsedTserver, context);

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
      if (client != null) {
        returnClient(client, context);
      }
    }
  }

  @Override
  public boolean testClassLoad(final String className, final String asTypeName)
      throws AccumuloException, AccumuloSecurityException {
    return ThriftClientTypes.CLIENT.execute(context, client -> client
        .checkClass(TraceUtil.traceInfo(), context.rpcCreds(), className, asTypeName));
  }

  @Override
  @Deprecated
  public List<ActiveCompaction> getActiveCompactions(String server)
      throws AccumuloException, AccumuloSecurityException {

    HostAndPort hp = HostAndPort.fromString(server);

    ServerId si = getServer(ServerTypeName.COMPACTOR, null, hp.getHost(), hp.getPort());
    if (si == null) {
      si = getServer(ServerTypeName.TABLET_SERVER, null, hp.getHost(), hp.getPort());
    }
    if (si == null) {
      return new ArrayList<>();
    }
    return getActiveCompactions(si);
  }

  @Override
  public List<ActiveCompaction> getActiveCompactions(ServerId server)
      throws AccumuloException, AccumuloSecurityException {

    final HostAndPort serverHostAndPort = HostAndPort.fromParts(server.getHost(), server.getPort());
    final List<ActiveCompaction> as = new ArrayList<>();
    try {
      if (server.getType() == ServerTypeName.TABLET_SERVER) {
        Client client = null;
        try {
          client = getClient(ThriftClientTypes.TABLET_SERVER, serverHostAndPort, context);
          for (var tac : client.getActiveCompactions(TraceUtil.traceInfo(), context.rpcCreds())) {
            as.add(new ActiveCompactionImpl(context, tac, server));
          }
        } finally {
          if (client != null) {
            returnClient(client, context);
          }
        }
      } else {
        // if not a TabletServer address, maybe it's a Compactor
        for (var tac : ExternalCompactionUtil.getActiveCompaction(serverHostAndPort, context)) {
          as.add(new ActiveCompactionImpl(context, tac, server));
        }
      }
      return as;
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (TException e) {
      throw new AccumuloException(e);
    }
  }

  @Override
  public List<ActiveCompaction> getActiveCompactions()
      throws AccumuloException, AccumuloSecurityException {

    Set<ServerId> compactionServers = new HashSet<>();
    compactionServers.addAll(getServers(ServerTypeName.COMPACTOR));
    compactionServers.addAll(getServers(ServerTypeName.TABLET_SERVER));

    int numThreads = Math.max(4, Math.min((compactionServers.size()) / 10, 256));
    var executorService = context.threadPools().getPoolBuilder(INSTANCE_OPS_COMPACTIONS_FINDER_POOL)
        .numCoreThreads(numThreads).build();
    try {
      List<Future<List<ActiveCompaction>>> futures = new ArrayList<>();

      for (ServerId server : compactionServers) {
        futures.add(executorService.submit(() -> getActiveCompactions(server)));
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
    try (TTransport transport = createTransport(AddressUtil.parseAddress(tserver), context)) {
      Client client = createClient(ThriftClientTypes.TABLET_SERVER, transport);
      client.getTabletServerStatus(TraceUtil.traceInfo(), context.rpcCreds());
    } catch (TException e) {
      throw new AccumuloException(e);
    }
  }

  @Override
  public void waitForBalance() throws AccumuloException {
    try {
      ThriftClientTypes.MANAGER.executeVoid(context,
          client -> client.waitForBalance(TraceUtil.traceInfo()));
    } catch (AccumuloSecurityException ex) {
      // should never happen
      throw new IllegalStateException("Unexpected exception thrown", ex);
    }

  }

  /**
   * Given a zooCache and instanceId, look up the instance name.
   */
  public static String lookupInstanceName(ZooCache zooCache, InstanceId instanceId) {
    checkArgument(zooCache != null, "zooCache is null");
    checkArgument(instanceId != null, "instanceId is null");
    for (String name : zooCache.getChildren(Constants.ZROOT + Constants.ZINSTANCES)) {
      var bytes = zooCache.get(Constants.ZROOT + Constants.ZINSTANCES + "/" + name);
      InstanceId iid = InstanceId.of(new String(bytes, UTF_8));
      if (iid.equals(instanceId)) {
        return name;
      }
    }
    return null;
  }

  @Override
  public InstanceId getInstanceId() {
    return context.getInstanceID();
  }

  @Override
  public ServerId getServer(ServerTypeName type, String resourceGroup, String host, int port) {
    Objects.requireNonNull(type, "type parameter cannot be null");
    Objects.requireNonNull(host, "host parameter cannot be null");

    final Optional<String> rg = Optional.ofNullable(resourceGroup);
    final Optional<HostAndPort> hp = Optional.of(HostAndPort.fromParts(host, port));

    switch (type) {
      case COMPACTOR:
        Set<ServiceLockPath> compactors = context.getServerPaths().getCompactor(rg, hp, true);
        if (compactors.isEmpty()) {
          return null;
        } else if (compactors.size() == 1) {
          return createServerId(type, compactors.iterator().next());
        } else {
          throw new IllegalStateException("Multiple servers matching provided address");
        }
      case MANAGER:
        Set<ServerId> managers = getServers(type, null);
        if (managers.isEmpty()) {
          return null;
        } else {
          return managers.iterator().next();
        }
      case SCAN_SERVER:
        Set<ServiceLockPath> sservers = context.getServerPaths().getScanServer(rg, hp, true);
        if (sservers.isEmpty()) {
          return null;
        } else if (sservers.size() == 1) {
          return createServerId(type, sservers.iterator().next());
        } else {
          throw new IllegalStateException("Multiple servers matching provided address");
        }
      case TABLET_SERVER:
        Set<ServiceLockPath> tservers = context.getServerPaths().getScanServer(rg, hp, true);
        if (tservers.isEmpty()) {
          return null;
        } else if (tservers.size() == 1) {
          return createServerId(type, tservers.iterator().next());
        } else {
          throw new IllegalStateException("Multiple servers matching provided address");
        }
      default:
        throw new IllegalArgumentException("Unhandled server type: " + type);
    }
  }

  @Override
  public Set<ServerId> getServers(ServerTypeName type) {
    return getServers(type, null);
  }

  @Override
  public Set<ServerId> getServers(ServerTypeName type, Predicate<ServerId> test) {
    final Set<ServerId> results = new HashSet<>();
    switch (type) {
      case COMPACTOR:
        context.getServerPaths().getCompactor(Optional.empty(), Optional.empty(), true)
            .forEach(c -> results.add(createServerId(type, c)));
        break;
      case MANAGER:
        ServiceLockPath m = context.getServerPaths().getManager(true);
        results.add(createServerId(type, m));
        break;
      case SCAN_SERVER:
        context.getServerPaths().getScanServer(Optional.empty(), Optional.empty(), true)
            .forEach(s -> results.add(createServerId(type, s)));
        break;
      case TABLET_SERVER:
        context.getServerPaths().getTabletServer(Optional.empty(), Optional.empty(), true)
            .forEach(t -> results.add(createServerId(type, t)));
        break;
      default:
        break;
    }
    if (test == null) {
      return results;
    }
    return results.stream().filter(test).collect(Collectors.toCollection(HashSet::new));
  }

  private ServerId createServerId(ServerTypeName type, ServiceLockPath slp) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(slp);
    String resourceGroup = Objects.requireNonNull(slp.getResourceGroup());
    HostAndPort hp = HostAndPort.fromString(Objects.requireNonNull(slp.getServer()));
    String host = hp.getHost();
    int port = hp.getPort();
    return new ServerId(type, resourceGroup, host, port);
  }

}
