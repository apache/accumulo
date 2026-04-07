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
package org.apache.accumulo.manager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.DelegationTokenConfigSerializer;
import org.apache.accumulo.core.clientImpl.TabletMergeabilityUtil;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateClient;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.fate.TraceRepo;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.manager.thrift.ManagerGoalState;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.manager.thrift.PrimaryManagerClientService;
import org.apache.accumulo.core.manager.thrift.TEvent;
import org.apache.accumulo.core.manager.thrift.TTabletMergeability;
import org.apache.accumulo.core.manager.thrift.TabletLoadState;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.TabletMergeabilityMetadata;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.securityImpl.thrift.TDelegationToken;
import org.apache.accumulo.core.securityImpl.thrift.TDelegationTokenConfig;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tserverOps.BeginTserverShutdown;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.thrift.TException;
import org.slf4j.Logger;

import com.google.common.collect.Lists;

public class ManagerClientServiceHandler implements PrimaryManagerClientService.Iface {

  private static final Logger log = Manager.log;
  private final Manager manager;
  private final ServerContext context;
  private final AuditedSecurityOperation security;

  protected ManagerClientServiceHandler(Manager manager) {
    this.manager = manager;
    this.context = manager.getContext();
    this.security = context.getSecurityOperation();
  }

  private NamespaceId getNamespaceIdFromTableId(TableOperation tableOp, TableId tableId)
      throws ThriftTableOperationException {
    NamespaceId namespaceId;
    try {
      namespaceId = context.getNamespaceId(tableId);
    } catch (TableNotFoundException e) {
      throw new ThriftTableOperationException(tableId.canonical(), null, tableOp,
          TableOperationExceptionType.NOTFOUND, e.getMessage());
    }
    return namespaceId;
  }

  @Override
  public ManagerMonitorInfo getManagerStats(TInfo info, TCredentials credentials) {
    return manager.getManagerMonitorInfo();
  }

  @Override
  public void shutdown(TInfo info, TCredentials c, boolean stopTabletServers)
      throws ThriftSecurityException {
    if (!security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }
    if (stopTabletServers) {
      manager.setManagerGoalState(ManagerGoalState.CLEAN_STOP);
      EventCoordinator.Tracker eventTracker = manager.nextEvent.getTracker();
      do {
        eventTracker.waitForEvents(Manager.ONE_SECOND);
      } while (manager.tserverSet.size() > 0);
    }
    manager.setManagerState(ManagerState.STOP);
  }

  @Override
  public void shutdownTabletServer(TInfo info, TCredentials c, String tabletServer, boolean force)
      throws ThriftSecurityException {
    if (!security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    final TServerInstance doomed = manager.tserverSet.find(tabletServer);
    if (doomed == null) {
      Manager.log.warn("No server found for name {}, unable to shut it down", tabletServer);
      return;
    }
    if (!force) {
      final TServerConnection server = manager.tserverSet.getConnection(doomed);
      if (server == null) {
        Manager.log.warn("No server found for name {}, unable to shut it down", tabletServer);
        return;
      }
    }

    FateClient<FateEnv> fate = manager.fateClient(FateInstanceType.META);

    var repo = new TraceRepo<>(
        new BeginTserverShutdown(doomed, manager.tserverSet.getResourceGroup(doomed), force));

    CompletableFuture<Optional<FateId>> future;
    try (var seeder = fate.beginSeeding()) {
      future = seeder.attemptToSeedTransaction(Fate.FateOperation.SHUTDOWN_TSERVER,
          FateKey.forShutdown(doomed), repo, true);
    }
    future.join().ifPresent(fate::waitForCompletion);

    log.debug("FATE op shutting down " + tabletServer + " finished");
  }

  @Override
  public void tabletServerStopping(TInfo tinfo, TCredentials credentials, String tabletServer,
      String resourceGroup)
      throws ThriftSecurityException, ThriftNotActiveServiceException, TException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }
    log.info("Tablet Server {} has reported it's shutting down", tabletServer);
    var tserver = new TServerInstance(tabletServer);
    // If there is an exception seeding the fate tx this should cause the RPC to fail which should
    // cause the tserver to halt. Because of that not making an attempt to handle failure here.
    FateClient<FateEnv> fate = manager.fateClient(FateInstanceType.META);

    var repo = new TraceRepo<>(
        new BeginTserverShutdown(tserver, ResourceGroupId.of(resourceGroup), false));
    // only seed a new transaction if nothing is running for this tserver
    fate.seedTransaction(Fate.FateOperation.SHUTDOWN_TSERVER, FateKey.forShutdown(tserver), repo,
        true);
  }

  @Override
  public void reportTabletStatus(TInfo info, TCredentials credentials, String serverName,
      TabletLoadState status, TKeyExtent ttablet) throws ThriftSecurityException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    KeyExtent tablet = KeyExtent.fromThrift(ttablet);

    switch (status) {
      case LOAD_FAILURE:
        Manager.log.error("{} reports assignment failed for tablet {}", serverName, tablet);
        break;
      case LOADED:
        manager.nextEvent.event(tablet, "tablet %s was loaded on %s", tablet, serverName);
        break;
      case UNLOADED:
        manager.nextEvent.event(tablet, "tablet %s was unloaded from %s", tablet, serverName);
        break;
      case UNLOAD_ERROR:
        Manager.log.error("{} reports unload failed for tablet {}", serverName, tablet);
        break;
      case UNLOAD_FAILURE_NOT_SERVING:
        if (Manager.log.isTraceEnabled()) {
          Manager.log.trace("{} reports unload failed: not serving tablet, could be a split: {}",
              serverName, tablet);
        }
        break;
    }
  }

  @Override
  public void setManagerGoalState(TInfo info, TCredentials c, ManagerGoalState state)
      throws ThriftSecurityException {
    if (!security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    manager.setManagerGoalState(state);
  }

  @Override
  public void waitForBalance(TInfo tinfo) {
    manager.getBalanceManager().waitForBalance();
  }

  @Override
  public List<String> getActiveTservers(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    Set<TServerInstance> tserverInstances = manager.onlineTabletServers();
    List<String> servers = new ArrayList<>();
    for (TServerInstance tserverInstance : tserverInstances) {
      servers.add(tserverInstance.getHostPort());
    }

    return servers;
  }

  @Override
  public TDelegationToken getDelegationToken(TInfo tinfo, TCredentials credentials,
      TDelegationTokenConfig tConfig) throws ThriftSecurityException, TException {
    if (!security.canObtainDelegationToken(credentials)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    // Make sure we're actually generating the secrets to make delegation tokens
    // Round-about way to verify that SASL is also enabled.
    if (!manager.delegationTokensAvailable()) {
      throw new TException("Delegation tokens are not available for use");
    }

    final DelegationTokenConfig config = DelegationTokenConfigSerializer.deserialize(tConfig);
    final AuthenticationTokenSecretManager secretManager = context.getSecretManager();
    try {
      Entry<Token<AuthenticationTokenIdentifier>,AuthenticationTokenIdentifier> pair =
          secretManager.generateToken(credentials.principal, config);

      return new TDelegationToken(ByteBuffer.wrap(pair.getKey().getPassword()),
          pair.getValue().getThriftIdentifier());
    } catch (Exception e) {
      throw new TException(e.getMessage());
    }
  }

  public static void mustBeOnline(ServerContext context, final TableId tableId)
      throws ThriftTableOperationException {
    context.clearTableListCache();
    if (context.getTableState(tableId) != TableState.ONLINE) {
      throw new ThriftTableOperationException(tableId.canonical(), null, TableOperation.MERGE,
          TableOperationExceptionType.OFFLINE, "table is not online");
    }
  }

  @Override
  public void requestTabletHosting(TInfo tinfo, TCredentials credentials, String tableIdStr,
      List<TKeyExtent> extents) throws ThriftSecurityException, ThriftTableOperationException {

    final TableId tableId = TableId.of(tableIdStr);
    NamespaceId namespaceId = getNamespaceIdFromTableId(null, tableId);
    if (!security.canScan(credentials, tableId, namespaceId)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    mustBeOnline(manager.getContext(), tableId);

    manager.hostOndemand(Lists.transform(extents, KeyExtent::fromThrift));
  }

  @Override
  public List<TKeyExtent> updateTabletMergeability(TInfo tinfo, TCredentials credentials,
      String tableName, Map<TKeyExtent,TTabletMergeability> splits)
      throws ThriftTableOperationException, ThriftSecurityException {

    final TableId tableId = getTableId(context, tableName);
    NamespaceId namespaceId = getNamespaceIdFromTableId(null, tableId);

    if (!security.canSplitTablet(credentials, tableId, namespaceId)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    try (var tabletsMutator = context.getAmple().conditionallyMutateTablets()) {
      for (Entry<TKeyExtent,TTabletMergeability> split : splits.entrySet()) {
        var currentTime = manager.getSteadyTime();
        var extent = KeyExtent.fromThrift(split.getKey());
        var tabletMergeability = TabletMergeabilityUtil.fromThrift(split.getValue());

        // Update the TabletMergeabilty metadata with the current manager time as
        // long as there is no existing operation set
        var updatedTm = TabletMergeabilityMetadata.toMetadata(tabletMergeability, currentTime);
        tabletsMutator.mutateTablet(extent).requireAbsentOperation()
            .putTabletMergeability(updatedTm)
            .submit(tm -> updatedTm.equals(tm.getTabletMergeability()),
                () -> "update TabletMergeability for " + extent + " to " + updatedTm);
      }

      var results = tabletsMutator.process();
      List<TKeyExtent> updated = new ArrayList<>();
      results.forEach((key, result) -> {
        if (result.getStatus() == Status.ACCEPTED) {
          updated.add(result.getExtent().toThrift());
        } else {
          log.debug("Unable to update TableMergeabilty: {}", result.getExtent());
        }
      });
      return updated;
    }
  }

  @Override
  public long getManagerTimeNanos(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    security.authenticateUser(credentials, credentials);
    return manager.getSteadyTime().getNanos();
  }

  @Override
  public void processEvents(TInfo tinfo, TCredentials credentials, List<TEvent> tEvents)
      throws TException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    manager.getEventCoordinator().events(tEvents.stream().map(EventCoordinator.Event::fromThrift)
        .peek(event -> log.trace("remote event : {}", event)).iterator());
  }

  protected TableId getTableId(ClientContext context, String tableName)
      throws ThriftTableOperationException {
    return ClientServiceHandler.checkTableId(context, tableName, null);
  }
}
