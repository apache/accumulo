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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FLUSH_ID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.DelegationTokenConfigSerializer;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.TVersionedProperties;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftConcurrentModificationException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.DeprecatedPropertyUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.manager.thrift.ManagerGoalState;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.manager.thrift.TabletLoadState;
import org.apache.accumulo.core.manager.thrift.ThriftPropertyException;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.TabletDeletedException;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.securityImpl.thrift.TDelegationToken;
import org.apache.accumulo.core.securityImpl.thrift.TDelegationTokenConfig;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.manager.tableOps.TraceRepo;
import org.apache.accumulo.manager.tserverOps.ShutdownTServer;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.accumulo.server.util.PropUtil;
import org.apache.accumulo.server.util.SystemPropUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;

public class ManagerClientServiceHandler implements ManagerClientService.Iface {

  private static final Logger log = Manager.log;
  private final Manager manager;
  private final Set<KeyExtent> hostingRequestInProgress;

  protected ManagerClientServiceHandler(Manager manager) {
    this.manager = manager;
    this.hostingRequestInProgress = new ConcurrentSkipListSet<>();
  }

  @Override
  public long initiateFlush(TInfo tinfo, TCredentials c, String tableIdStr)
      throws ThriftSecurityException, ThriftTableOperationException {
    TableId tableId = TableId.of(tableIdStr);
    NamespaceId namespaceId = getNamespaceIdFromTableId(TableOperation.FLUSH, tableId);
    if (!manager.security.canFlush(c, tableId, namespaceId)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    String zTablePath = Constants.ZROOT + "/" + manager.getInstanceID() + Constants.ZTABLES + "/"
        + tableId + Constants.ZTABLE_FLUSH_ID;

    ZooReaderWriter zoo = manager.getContext().getZooReaderWriter();
    byte[] fid;
    try {
      fid = zoo.mutateExisting(zTablePath, currentValue -> {
        long flushID = Long.parseLong(new String(currentValue, UTF_8));
        return Long.toString(flushID + 1).getBytes(UTF_8);
      });
    } catch (NoNodeException nne) {
      throw new ThriftTableOperationException(tableId.canonical(), null, TableOperation.FLUSH,
          TableOperationExceptionType.NOTFOUND, null);
    } catch (Exception e) {
      Manager.log.warn("{}", e.getMessage(), e);
      throw new ThriftTableOperationException(tableId.canonical(), null, TableOperation.FLUSH,
          TableOperationExceptionType.OTHER, null);
    }
    return Long.parseLong(new String(fid, UTF_8));
  }

  @Override
  public void waitForFlush(TInfo tinfo, TCredentials c, String tableIdStr, ByteBuffer startRowBB,
      ByteBuffer endRowBB, long flushID, long maxLoops)
      throws ThriftSecurityException, ThriftTableOperationException {
    TableId tableId = TableId.of(tableIdStr);
    NamespaceId namespaceId = getNamespaceIdFromTableId(TableOperation.FLUSH, tableId);
    if (!manager.security.canFlush(c, tableId, namespaceId)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    Text startRow = ByteBufferUtil.toText(startRowBB);
    Text endRow = ByteBufferUtil.toText(endRowBB);

    if (endRow != null && startRow != null && startRow.compareTo(endRow) >= 0) {
      throw new ThriftTableOperationException(tableId.canonical(), null, TableOperation.FLUSH,
          TableOperationExceptionType.BAD_RANGE, "start row must be less than end row");
    }

    Set<TServerInstance> serversToFlush = new HashSet<>(manager.tserverSet.getCurrentServers());

    for (long l = 0; l < maxLoops; l++) {

      for (TServerInstance instance : serversToFlush) {
        try {
          final TServerConnection server = manager.tserverSet.getConnection(instance);
          if (server != null) {
            server.flush(manager.managerLock, tableId, ByteBufferUtil.toBytes(startRowBB),
                ByteBufferUtil.toBytes(endRowBB));
          }
        } catch (TException ex) {
          Manager.log.error(ex.toString());
        }
      }

      if (tableId.equals(AccumuloTable.ROOT.tableId())) {
        break; // this code does not properly handle the root tablet. See #798
      }

      if (l == maxLoops - 1) {
        break;
      }

      sleepUninterruptibly(50, TimeUnit.MILLISECONDS);

      serversToFlush.clear();

      try (TabletsMetadata tablets = TabletsMetadata.builder(manager.getContext()).forTable(tableId)
          .overlapping(startRow, endRow).fetch(FLUSH_ID, LOCATION, LOGS, PREV_ROW).build()) {
        int tabletsToWaitFor = 0;
        int tabletCount = 0;

        for (TabletMetadata tablet : tablets) {
          int logs = tablet.getLogs().size();

          // when tablet is not online and has no logs, there is no reason to wait for it
          if ((tablet.hasCurrent() || logs > 0) && tablet.getFlushId().orElse(-1) < flushID) {
            tabletsToWaitFor++;
            if (tablet.hasCurrent()) {
              serversToFlush.add(tablet.getLocation().getServerInstance());
            }
          }

          tabletCount++;
        }

        if (tabletsToWaitFor == 0) {
          break;
        }

        // TODO detect case of table offline AND tablets w/ logs? - ACCUMULO-1296

        if (tabletCount == 0 && !manager.getContext().tableNodeExists(tableId)) {
          throw new ThriftTableOperationException(tableId.canonical(), null, TableOperation.FLUSH,
              TableOperationExceptionType.NOTFOUND, null);
        }

      } catch (TabletDeletedException e) {
        Manager.log.debug("Failed to scan {} table to wait for flush {}",
            AccumuloTable.METADATA.tableName(), tableId, e);
      }
    }

  }

  private NamespaceId getNamespaceIdFromTableId(TableOperation tableOp, TableId tableId)
      throws ThriftTableOperationException {
    NamespaceId namespaceId;
    try {
      namespaceId = manager.getContext().getNamespaceId(tableId);
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
  public void removeTableProperty(TInfo info, TCredentials credentials, String tableName,
      String property)
      throws ThriftSecurityException, ThriftTableOperationException, ThriftPropertyException {
    alterTableProperty(credentials, tableName, property, null, TableOperation.REMOVE_PROPERTY);
  }

  @Override
  public void setTableProperty(TInfo info, TCredentials credentials, String tableName,
      String property, String value)
      throws ThriftSecurityException, ThriftTableOperationException, ThriftPropertyException {
    alterTableProperty(credentials, tableName, property, value, TableOperation.SET_PROPERTY);
  }

  @Override
  public void modifyTableProperties(TInfo tinfo, TCredentials credentials, String tableName,
      TVersionedProperties properties)
      throws ThriftSecurityException, ThriftTableOperationException,
      ThriftConcurrentModificationException, ThriftPropertyException {
    final TableId tableId = ClientServiceHandler.checkTableId(manager.getContext(), tableName,
        TableOperation.SET_PROPERTY);
    NamespaceId namespaceId = getNamespaceIdFromTableId(TableOperation.SET_PROPERTY, tableId);
    if (!manager.security.canAlterTable(credentials, tableId, namespaceId)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      PropUtil.replaceProperties(manager.getContext(),
          TablePropKey.of(manager.getContext(), tableId), properties.getVersion(),
          properties.getProperties());
    } catch (ConcurrentModificationException cme) {
      log.warn("Error modifying table properties, properties have changed", cme);
      throw new ThriftConcurrentModificationException(cme.getMessage());
    } catch (IllegalStateException ex) {
      log.warn("Error modifying table properties: tableId: {}", tableId.canonical());
      // race condition... table no longer exists? This call will throw an exception if the table
      // was deleted:
      ClientServiceHandler.checkTableId(manager.getContext(), tableName,
          TableOperation.SET_PROPERTY);
      throw new ThriftTableOperationException(tableId.canonical(), tableName,
          TableOperation.SET_PROPERTY, TableOperationExceptionType.OTHER,
          "Error modifying table properties: tableId: " + tableId.canonical());
    } catch (IllegalArgumentException iae) {
      throw new ThriftPropertyException();
    }

  }

  @Override
  public void shutdown(TInfo info, TCredentials c, boolean stopTabletServers)
      throws ThriftSecurityException {
    if (!manager.security.canPerformSystemActions(c)) {
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
    if (!manager.security.canPerformSystemActions(c)) {
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

    Fate<Manager> fate = manager.fate(FateInstanceType.META);
    FateId fateId = fate.startTransaction();

    String msg = "Shutdown tserver " + tabletServer;

    fate.seedTransaction("ShutdownTServer", fateId,
        new TraceRepo<>(new ShutdownTServer(doomed, force)), false, msg);
    fate.waitForCompletion(fateId);
    fate.delete(fateId);

    log.debug("FATE op shutting down " + tabletServer + " finished");
  }

  @Override
  public void reportTabletStatus(TInfo info, TCredentials credentials, String serverName,
      TabletLoadState status, TKeyExtent ttablet) throws ThriftSecurityException {
    if (!manager.security.canPerformSystemActions(credentials)) {
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
    if (!manager.security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    manager.setManagerGoalState(state);
  }

  @Override
  public void removeSystemProperty(TInfo info, TCredentials c, String property)
      throws ThriftSecurityException {
    if (!manager.security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      SystemPropUtil.removeSystemProperty(manager.getContext(), property);
      updatePlugins(property);
    } catch (Exception e) {
      Manager.log.error("Problem removing config property in zookeeper", e);
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void setSystemProperty(TInfo info, TCredentials c, String property, String value)
      throws TException {
    if (!manager.security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      SystemPropUtil.setSystemProperty(manager.getContext(), property, value);
      updatePlugins(property);
    } catch (IllegalArgumentException iae) {
      Manager.log.error("Problem setting invalid property", iae);
      throw new ThriftPropertyException(property, value, "Property is invalid");
    } catch (Exception e) {
      Manager.log.error("Problem setting config property in zookeeper", e);
      throw new TException(e.getMessage());
    }
  }

  @Override
  public void modifySystemProperties(TInfo info, TCredentials c, TVersionedProperties properties)
      throws TException {
    if (!manager.security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      SystemPropUtil.modifyProperties(manager.getContext(), properties.getVersion(),
          properties.getProperties());
      for (Map.Entry<String,String> entry : properties.getProperties().entrySet()) {
        updatePlugins(entry.getKey());
      }
    } catch (IllegalArgumentException iae) {
      Manager.log.error("Problem setting invalid property", iae);
      throw new ThriftPropertyException("Modify properties", "failed", iae.getMessage());
    } catch (ConcurrentModificationException cme) {
      log.warn("Error modifying system properties, properties have changed", cme);
      throw new ThriftConcurrentModificationException(cme.getMessage());
    } catch (Exception e) {
      Manager.log.error("Problem setting config property in zookeeper", e);
      throw new TException(e.getMessage());
    }
  }

  @Override
  public void setNamespaceProperty(TInfo tinfo, TCredentials credentials, String ns,
      String property, String value)
      throws ThriftSecurityException, ThriftTableOperationException, ThriftPropertyException {
    alterNamespaceProperty(credentials, ns, property, value, TableOperation.SET_PROPERTY);
  }

  @Override
  public void modifyNamespaceProperties(TInfo tinfo, TCredentials credentials, String ns,
      TVersionedProperties properties) throws TException {
    final NamespaceId namespaceId = ClientServiceHandler.checkNamespaceId(manager.getContext(), ns,
        TableOperation.SET_PROPERTY);
    if (!manager.security.canAlterNamespace(credentials, namespaceId)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      PropUtil.replaceProperties(manager.getContext(),
          NamespacePropKey.of(manager.getContext(), namespaceId), properties.getVersion(),
          properties.getProperties());
    } catch (ConcurrentModificationException cme) {
      log.warn("Error modifying namespace properties, properties have changed", cme);
      throw new ThriftConcurrentModificationException(cme.getMessage());
    } catch (IllegalStateException ex) {
      // race condition on delete... namespace no longer exists? An undelying ZooKeeper.NoNode
      // exception will be thrown an exception if the namespace was deleted:
      ClientServiceHandler.checkNamespaceId(manager.getContext(), ns, TableOperation.SET_PROPERTY);
      log.warn("Error modifying namespace properties", ex);
      throw new ThriftTableOperationException(namespaceId.canonical(), ns,
          TableOperation.SET_PROPERTY, TableOperationExceptionType.OTHER,
          "Error modifying namespace properties");
    } catch (IllegalArgumentException iae) {
      throw new ThriftPropertyException("All properties", "failed", iae.getMessage());
    }
  }

  @Override
  public void removeNamespaceProperty(TInfo tinfo, TCredentials credentials, String ns,
      String property)
      throws ThriftSecurityException, ThriftTableOperationException, ThriftPropertyException {
    alterNamespaceProperty(credentials, ns, property, null, TableOperation.REMOVE_PROPERTY);
  }

  private void alterNamespaceProperty(TCredentials c, String namespace, String property,
      String value, TableOperation op)
      throws ThriftSecurityException, ThriftTableOperationException, ThriftPropertyException {

    NamespaceId namespaceId =
        ClientServiceHandler.checkNamespaceId(manager.getContext(), namespace, op);

    if (!manager.security.canAlterNamespace(c, namespaceId)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      if (value == null) {
        PropUtil.removeProperties(manager.getContext(),
            NamespacePropKey.of(manager.getContext(), namespaceId), List.of(property));
      } else {
        PropUtil.setProperties(manager.getContext(),
            NamespacePropKey.of(manager.getContext(), namespaceId), Map.of(property, value));
      }
    } catch (IllegalStateException ex) {
      // race condition on delete... namespace no longer exists? An undelying ZooKeeper.NoNode
      // exception will be thrown an exception if the namespace was deleted:
      ClientServiceHandler.checkNamespaceId(manager.getContext(), namespace, op);
      log.info("Error altering namespace property", ex);
      throw new ThriftTableOperationException(namespaceId.canonical(), namespace, op,
          TableOperationExceptionType.OTHER, "Problem altering namespace property");
    } catch (IllegalArgumentException iae) {
      throw new ThriftPropertyException(property, value, iae.getMessage());
    }
  }

  private void alterTableProperty(TCredentials c, String tableName, String property, String value,
      TableOperation op)
      throws ThriftSecurityException, ThriftTableOperationException, ThriftPropertyException {
    final TableId tableId = ClientServiceHandler.checkTableId(manager.getContext(), tableName, op);
    NamespaceId namespaceId = getNamespaceIdFromTableId(op, tableId);
    if (!manager.security.canAlterTable(c, tableId, namespaceId)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      if (value == null || value.isEmpty()) {
        PropUtil.removeProperties(manager.getContext(),
            TablePropKey.of(manager.getContext(), tableId), List.of(property));
      } else {
        PropUtil.setProperties(manager.getContext(), TablePropKey.of(manager.getContext(), tableId),
            Map.of(property, value));
      }
    } catch (IllegalStateException ex) {
      log.warn("Invalid table property, tried to set: tableId: " + tableId.canonical() + " to: "
          + property + "=" + value);
      // race condition... table no longer exists? This call will throw an exception if the table
      // was deleted:
      ClientServiceHandler.checkTableId(manager.getContext(), tableName, op);
      throw new ThriftTableOperationException(tableId.canonical(), tableName, op,
          TableOperationExceptionType.OTHER, "Invalid table property, tried to set: tableId: "
              + tableId.canonical() + " to: " + property + "=" + value);
    } catch (IllegalArgumentException iae) {
      throw new ThriftPropertyException(property, value, iae.getMessage());
    }
  }

  private void updatePlugins(String property) {
    // resolve without warning; any warnings should have already occurred
    String resolved = DeprecatedPropertyUtil.getReplacementName(property, (log, replacement) -> {});
    if (resolved.equals(Property.MANAGER_TABLET_BALANCER.getKey())) {
      manager.initializeBalancer();
      log.info("tablet balancer changed to {}", manager.getBalancerClass().getName());
    }
  }

  @Override
  public void waitForBalance(TInfo tinfo) {
    manager.waitForBalance();
  }

  @Override
  public List<String> getActiveTservers(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    if (!manager.security.canPerformSystemActions(credentials)) {
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
    if (!manager.security.canObtainDelegationToken(credentials)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    // Make sure we're actually generating the secrets to make delegation tokens
    // Round-about way to verify that SASL is also enabled.
    if (!manager.delegationTokensAvailable()) {
      throw new TException("Delegation tokens are not available for use");
    }

    final DelegationTokenConfig config = DelegationTokenConfigSerializer.deserialize(tConfig);
    final AuthenticationTokenSecretManager secretManager = manager.getContext().getSecretManager();
    try {
      Entry<Token<AuthenticationTokenIdentifier>,AuthenticationTokenIdentifier> pair =
          secretManager.generateToken(credentials.principal, config);

      return new TDelegationToken(ByteBuffer.wrap(pair.getKey().getPassword()),
          pair.getValue().getThriftIdentifier());
    } catch (Exception e) {
      throw new TException(e.getMessage());
    }
  }

  @Override
  public void requestTabletHosting(TInfo tinfo, TCredentials credentials, String tableIdStr,
      List<TKeyExtent> extents) throws ThriftSecurityException, ThriftTableOperationException {

    final TableId tableId = TableId.of(tableIdStr);
    NamespaceId namespaceId = getNamespaceIdFromTableId(null, tableId);
    if (!manager.security.canScan(credentials, tableId, namespaceId)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    manager.mustBeOnline(tableId);

    final List<KeyExtent> success = new ArrayList<>();
    final List<KeyExtent> inProgress = new ArrayList<>();
    extents.forEach(e -> {
      KeyExtent ke = KeyExtent.fromThrift(e);
      if (hostingRequestInProgress.add(ke)) {
        log.info("Tablet hosting requested for: {} ", KeyExtent.fromThrift(e));
        inProgress.add(ke);
      } else {
        log.trace("Ignoring hosting request because another thread is currently processing it {}",
            ke);
      }
    });
    // Do not add any code here, it may interfere with the finally block removing extents from
    // hostingRequestInProgress
    try (var mutator = manager.getContext().getAmple().conditionallyMutateTablets()) {
      inProgress.forEach(ke -> {
        mutator.mutateTablet(ke).requireAbsentOperation()
            .requireTabletAvailability(TabletAvailability.ONDEMAND).requireAbsentLocation()
            .setHostingRequested().submit(TabletMetadata::getHostingRequested);

      });

      mutator.process().forEach((extent, result) -> {
        if (result.getStatus() == Status.ACCEPTED) {
          // cache this success for a bit
          success.add(extent);
        } else {
          if (log.isTraceEnabled()) {
            // only read the metadata if the logging is enabled
            log.trace("Failed to set hosting request {}", result.readMetadata());
          }
        }
      });
    } finally {
      inProgress.forEach(hostingRequestInProgress::remove);
    }

    manager.getEventCoordinator().event(success,
        "Tablet hosting requested for %d of %d tablets in %s", success.size(), extents.size(),
        tableId);
  }

  protected TableId getTableId(ClientContext context, String tableName)
      throws ThriftTableOperationException {
    return ClientServiceHandler.checkTableId(context, tableName, null);
  }

}
