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
import static org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil.checkIteratorPriorityConflicts;
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
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.DelegationTokenConfigSerializer;
import org.apache.accumulo.core.clientImpl.TabletMergeabilityUtil;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.TVersionedProperties;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftConcurrentModificationException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.TraceRepo;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.manager.thrift.ManagerGoalState;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.manager.thrift.TTabletMergeability;
import org.apache.accumulo.core.manager.thrift.TabletLoadState;
import org.apache.accumulo.core.manager.thrift.ThriftPropertyException;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.TabletDeletedException;
import org.apache.accumulo.core.metadata.schema.TabletMergeabilityMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.securityImpl.thrift.TDelegationToken;
import org.apache.accumulo.core.securityImpl.thrift.TDelegationTokenConfig;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tserverOps.ShutdownTServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.ResourceGroupPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.manager.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.accumulo.server.util.PropUtil;
import org.apache.accumulo.server.util.SystemPropUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ManagerClientServiceHandler implements ManagerClientService.Iface {

  private static final Logger log = Manager.log;
  private final Manager manager;
  private final ServerContext context;
  private final AuditedSecurityOperation security;

  protected ManagerClientServiceHandler(Manager manager) {
    this.manager = manager;
    this.context = manager.getContext();
    this.security = context.getSecurityOperation();
  }

  @Override
  public long initiateFlush(TInfo tinfo, TCredentials c, String tableIdStr)
      throws ThriftSecurityException, ThriftTableOperationException {
    TableId tableId = TableId.of(tableIdStr);
    NamespaceId namespaceId = getNamespaceIdFromTableId(TableOperation.FLUSH, tableId);
    if (!security.canFlush(c, tableId, namespaceId)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    String zTablePath = Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_FLUSH_ID;

    ZooReaderWriter zoo = context.getZooSession().asReaderWriter();
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
    if (!security.canFlush(c, tableId, namespaceId)) {
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

      if (tableId.equals(SystemTables.ROOT.tableId())) {
        break; // this code does not properly handle the root tablet. See #798
      }

      if (l == maxLoops - 1) {
        break;
      }

      sleepUninterruptibly(50, TimeUnit.MILLISECONDS);

      serversToFlush.clear();

      try (TabletsMetadata tablets = TabletsMetadata.builder(context).forTable(tableId)
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

        if (tabletCount == 0 && !context.tableNodeExists(tableId)) {
          throw new ThriftTableOperationException(tableId.canonical(), null, TableOperation.FLUSH,
              TableOperationExceptionType.NOTFOUND, null);
        }

      } catch (TabletDeletedException e) {
        Manager.log.debug("Failed to scan {} table to wait for flush {}",
            SystemTables.METADATA.tableName(), tableId, e);
      }
    }

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
    final TableId tableId =
        ClientServiceHandler.checkTableId(context, tableName, TableOperation.SET_PROPERTY);
    NamespaceId namespaceId = getNamespaceIdFromTableId(TableOperation.SET_PROPERTY, tableId);
    if (!security.canAlterTable(credentials, tableId, namespaceId)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      checkIteratorPriorityConflicts("table:" + tableName + " tableId:" + tableId,
          properties.getProperties(), context.getNamespaceConfiguration(namespaceId)
              .getAllPropertiesWithPrefix(Property.TABLE_ITERATOR_PREFIX));
      PropUtil.replaceProperties(context, TablePropKey.of(tableId), properties.getVersion(),
          properties.getProperties());
    } catch (ConcurrentModificationException cme) {
      log.warn("Error modifying table properties, properties have changed", cme);
      throw new ThriftConcurrentModificationException(cme.getMessage());
    } catch (IllegalStateException ex) {
      log.warn("Error modifying table properties: tableId: {}", tableId.canonical());
      // race condition... table no longer exists? This call will throw an exception if the table
      // was deleted:
      ClientServiceHandler.checkTableId(context, tableName, TableOperation.SET_PROPERTY);
      throw new ThriftTableOperationException(tableId.canonical(), tableName,
          TableOperation.SET_PROPERTY, TableOperationExceptionType.OTHER,
          "Error modifying table properties: tableId: " + tableId.canonical());
    } catch (IllegalArgumentException iae) {
      throw new ThriftPropertyException("Modify properties", "failed", iae.getMessage());
    }

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

    Fate<FateEnv> fate = manager.fate(FateInstanceType.META);
    FateId fateId = fate.startTransaction();

    String msg = "Shutdown tserver " + tabletServer;

    fate.seedTransaction(Fate.FateOperation.SHUTDOWN_TSERVER, fateId,
        new TraceRepo<>(
            new ShutdownTServer(doomed, manager.tserverSet.getResourceGroup(doomed), force)),
        false, msg);
    fate.waitForCompletion(fateId);
    fate.delete(fateId);

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
    if (manager.shutdownTServer(tserver)) {
      // If there is an exception seeding the fate tx this should cause the RPC to fail which should
      // cause the tserver to halt. Because of that not making an attempt to handle failure here.
      Fate<FateEnv> fate = manager.fate(FateInstanceType.META);
      var tid = fate.startTransaction();
      String msg = "Shutdown tserver " + tabletServer;

      fate.seedTransaction(Fate.FateOperation.SHUTDOWN_TSERVER, tid,
          new TraceRepo<>(new ShutdownTServer(tserver, ResourceGroupId.of(resourceGroup), false)),
          true, msg);
    }
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
  public void removeSystemProperty(TInfo info, TCredentials c, String property)
      throws ThriftSecurityException {
    if (!security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      SystemPropUtil.removeSystemProperty(context, property);
    } catch (Exception e) {
      Manager.log.error("Problem removing config property in zookeeper", e);
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void setSystemProperty(TInfo info, TCredentials c, String property, String value)
      throws TException {
    if (!security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      SystemPropUtil.setSystemProperty(context, property, value);
    } catch (IllegalArgumentException iae) {
      Manager.log.error("Problem setting invalid property", iae);
      throw new ThriftPropertyException(property, value,
          "Property is invalid. message: " + iae.getMessage());
    } catch (Exception e) {
      Manager.log.error("Problem setting config property in zookeeper", e);
      throw new TException(e.getMessage());
    }
  }

  @Override
  public void modifySystemProperties(TInfo info, TCredentials c, TVersionedProperties properties)
      throws TException {
    if (!security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      SystemPropUtil.modifyProperties(context, properties.getVersion(), properties.getProperties());
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
  public void createResourceGroupNode(TInfo tinfo, TCredentials c, String resourceGroup)
      throws ThriftSecurityException, ThriftNotActiveServiceException, TException {

    if (!security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      Preconditions.checkArgument(resourceGroup != null && !resourceGroup.isBlank(),
          "Supplied resource group name is null or empty");
      final ResourceGroupId rgid = ResourceGroupId.of(resourceGroup);
      final ResourceGroupPropKey key = ResourceGroupPropKey.of(rgid);
      key.createZNode(context.getZooSession().asReaderWriter());
    } catch (KeeperException | InterruptedException e) {
      Manager.log.error("Problem creating resource group config node in zookeeper", e);
      throw new TException(e.getMessage());
    }

  }

  @Override
  public void removeResourceGroupNode(TInfo tinfo, TCredentials c, String resourceGroup)
      throws ThriftSecurityException, ThriftNotActiveServiceException, TException {

    if (!security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    final ResourceGroupId rgid = ClientServiceHandler.checkResourceGroupId(context, resourceGroup);
    try {
      if (rgid.equals(ResourceGroupId.DEFAULT)) {
        throw new IllegalArgumentException(
            "Cannot remove default resource group configuration node");
      }
      final ResourceGroupPropKey key = ResourceGroupPropKey.of(rgid);
      key.removeZNode(context.getZooSession());
    } catch (Exception e) {
      Manager.log.error("Problem removing resource group config node in zookeeper", e);
      throw new TException(e.getMessage());
    }

  }

  @Override
  public void removeResourceGroupProperty(TInfo info, TCredentials c, String resourceGroup,
      String property) throws TException {
    if (!security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }
    ResourceGroupId rgid = ClientServiceHandler.checkResourceGroupId(context, resourceGroup);
    try {
      PropUtil.removeProperties(context, ResourceGroupPropKey.of(rgid), List.of(property));
    } catch (Exception e) {
      Manager.log.error("Problem removing config property in zookeeper", e);
      throw new TException(e.getMessage());
    }
  }

  @Override
  public void setResourceGroupProperty(TInfo info, TCredentials c, String resourceGroup,
      String property, String value) throws TException {
    if (!security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    final ResourceGroupId rgid = ClientServiceHandler.checkResourceGroupId(context, resourceGroup);
    try {
      PropUtil.setProperties(context, ResourceGroupPropKey.of(rgid), Map.of(property, value));
    } catch (IllegalArgumentException iae) {
      Manager.log.error("Problem setting invalid property", iae);
      throw new ThriftPropertyException(property, value,
          "Property is invalid. message: " + iae.getMessage());
    } catch (Exception e) {
      Manager.log.error("Problem setting config property in zookeeper", e);
      throw new TException(e.getMessage());
    }
  }

  @Override
  public void modifyResourceGroupProperties(TInfo info, TCredentials c, String resourceGroup,
      TVersionedProperties properties) throws TException {
    if (!security.canPerformSystemActions(c)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }
    ResourceGroupId rgid = ClientServiceHandler.checkResourceGroupId(context, resourceGroup);
    try {
      PropUtil.replaceProperties(context, ResourceGroupPropKey.of(rgid), properties.getVersion(),
          properties.getProperties());
    } catch (IllegalArgumentException iae) {
      Manager.log.error("Problem setting invalid property", iae);
      throw new ThriftPropertyException("Modify properties", "failed", iae.getMessage());
    } catch (ConcurrentModificationException cme) {
      log.warn("Error modifying resource group properties, properties have changed", cme);
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
    final NamespaceId namespaceId =
        ClientServiceHandler.checkNamespaceId(context, ns, TableOperation.SET_PROPERTY);
    if (!security.canAlterNamespace(credentials, namespaceId)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      checkIteratorPriorityConflicts("namespace:" + ns + " namespaceId:" + namespaceId,
          properties.getProperties(),
          context.getConfiguration().getAllPropertiesWithPrefix(Property.TABLE_ITERATOR_PREFIX));
      PropUtil.replaceProperties(context, NamespacePropKey.of(namespaceId), properties.getVersion(),
          properties.getProperties());
    } catch (ConcurrentModificationException cme) {
      log.warn("Error modifying namespace properties, properties have changed", cme);
      throw new ThriftConcurrentModificationException(cme.getMessage());
    } catch (IllegalStateException ex) {
      // race condition on delete... namespace no longer exists? An undelying ZooKeeper.NoNode
      // exception will be thrown an exception if the namespace was deleted:
      ClientServiceHandler.checkNamespaceId(context, ns, TableOperation.SET_PROPERTY);
      log.warn("Error modifying namespace properties", ex);
      throw new ThriftTableOperationException(namespaceId.canonical(), ns,
          TableOperation.SET_PROPERTY, TableOperationExceptionType.OTHER,
          "Error modifying namespace properties");
    } catch (IllegalArgumentException iae) {
      throw new ThriftPropertyException("Modify properties", "failed", iae.getMessage());
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

    NamespaceId namespaceId = ClientServiceHandler.checkNamespaceId(context, namespace, op);

    if (!security.canAlterNamespace(c, namespaceId)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      if (value == null) {
        PropUtil.removeProperties(context, NamespacePropKey.of(namespaceId), List.of(property));
      } else {
        checkIteratorPriorityConflicts("namespace:" + namespace + " namespaceId:" + namespaceId,
            Map.of(property, value), context.getNamespaceConfiguration(namespaceId)
                .getAllPropertiesWithPrefix(Property.TABLE_ITERATOR_PREFIX));
        PropUtil.setProperties(context, NamespacePropKey.of(namespaceId), Map.of(property, value));
      }
    } catch (IllegalStateException ex) {
      // race condition on delete... namespace no longer exists? An undelying ZooKeeper.NoNode
      // exception will be thrown an exception if the namespace was deleted:
      ClientServiceHandler.checkNamespaceId(context, namespace, op);
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
    final TableId tableId = ClientServiceHandler.checkTableId(context, tableName, op);
    NamespaceId namespaceId = getNamespaceIdFromTableId(op, tableId);
    if (!security.canAlterTable(c, tableId, namespaceId)) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      if (op == TableOperation.REMOVE_PROPERTY) {
        PropUtil.removeProperties(context, TablePropKey.of(tableId), List.of(property));
      } else if (op == TableOperation.SET_PROPERTY) {
        if (value == null || value.isEmpty()) {
          value = "";
        }
        checkIteratorPriorityConflicts("table:" + tableName + "tableId:" + tableId,
            Map.of(property, value), context.getTableConfiguration(tableId)
                .getAllPropertiesWithPrefix(Property.TABLE_ITERATOR_PREFIX));
        PropUtil.setProperties(context, TablePropKey.of(tableId), Map.of(property, value));
      } else {
        throw new UnsupportedOperationException("table operation:" + op.name());
      }
    } catch (IllegalStateException ex) {
      log.warn("Invalid table property, tried to {}: tableId: {} to: {}={}", op.name(),
          tableId.canonical(), property, value, ex);
      // race condition... table no longer exists? This call will throw an exception if the table
      // was deleted:
      ClientServiceHandler.checkTableId(context, tableName, op);
      throw new ThriftTableOperationException(tableId.canonical(), tableName, op,
          TableOperationExceptionType.OTHER, "Invalid table property, tried to set: tableId: "
              + tableId.canonical() + " to: " + property + "=" + value);
    } catch (IllegalArgumentException iae) {
      throw new ThriftPropertyException(property, value, iae.getMessage());
    }
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

  protected TableId getTableId(ClientContext context, String tableName)
      throws ThriftTableOperationException {
    return ClientServiceHandler.checkTableId(context, tableName, null);
  }
}
