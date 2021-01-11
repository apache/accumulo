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
package org.apache.accumulo.master;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FLUSH_ID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.DelegationTokenConfigSerializer;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.master.thrift.TabletSplit;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.metadata.schema.TabletDeletedException;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.OrderSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.securityImpl.thrift.TDelegationToken;
import org.apache.accumulo.core.securityImpl.thrift.TDelegationTokenConfig;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.master.tableOps.TraceRepo;
import org.apache.accumulo.master.tserverOps.ShutdownTServer;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.balancer.DefaultLoadBalancer;
import org.apache.accumulo.server.master.balancer.TabletBalancer;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.accumulo.server.util.NamespacePropUtil;
import org.apache.accumulo.server.util.SystemPropUtil;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

public class MasterClientServiceHandler extends FateServiceHandler
    implements MasterClientService.Iface {

  private static final Logger log = Master.log;
  private static final Logger drainLog =
      LoggerFactory.getLogger("org.apache.accumulo.master.MasterDrainImpl");

  protected MasterClientServiceHandler(Master master) {
    super(master);
  }

  @Override
  public long initiateFlush(TInfo tinfo, TCredentials c, String tableIdStr)
      throws ThriftSecurityException, ThriftTableOperationException {
    TableId tableId = TableId.of(tableIdStr);
    NamespaceId namespaceId = getNamespaceIdFromTableId(TableOperation.FLUSH, tableId);
    if (!master.security.canFlush(c, tableId, namespaceId))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    String zTablePath = Constants.ZROOT + "/" + master.getInstanceID() + Constants.ZTABLES + "/"
        + tableId + Constants.ZTABLE_FLUSH_ID;

    ZooReaderWriter zoo = master.getContext().getZooReaderWriter();
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
      Master.log.warn("{}", e.getMessage(), e);
      throw new ThriftTableOperationException(tableId.canonical(), null, TableOperation.FLUSH,
          TableOperationExceptionType.OTHER, null);
    }
    return Long.parseLong(new String(fid));
  }

  @Override
  public void waitForFlush(TInfo tinfo, TCredentials c, String tableIdStr, ByteBuffer startRowBB,
      ByteBuffer endRowBB, long flushID, long maxLoops)
      throws ThriftSecurityException, ThriftTableOperationException {
    TableId tableId = TableId.of(tableIdStr);
    NamespaceId namespaceId = getNamespaceIdFromTableId(TableOperation.FLUSH, tableId);
    if (!master.security.canFlush(c, tableId, namespaceId))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    Text startRow = ByteBufferUtil.toText(startRowBB);
    Text endRow = ByteBufferUtil.toText(endRowBB);

    if (endRow != null && startRow != null && startRow.compareTo(endRow) >= 0)
      throw new ThriftTableOperationException(tableId.canonical(), null, TableOperation.FLUSH,
          TableOperationExceptionType.BAD_RANGE, "start row must be less than end row");

    Set<TServerInstance> serversToFlush = new HashSet<>(master.tserverSet.getCurrentServers());

    for (long l = 0; l < maxLoops; l++) {

      for (TServerInstance instance : serversToFlush) {
        try {
          final TServerConnection server = master.tserverSet.getConnection(instance);
          if (server != null)
            server.flush(master.masterLock, tableId, ByteBufferUtil.toBytes(startRowBB),
                ByteBufferUtil.toBytes(endRowBB));
        } catch (TException ex) {
          Master.log.error(ex.toString());
        }
      }

      if (tableId.equals(RootTable.ID))
        break; // this code does not properly handle the root tablet. See #798

      if (l == maxLoops - 1)
        break;

      sleepUninterruptibly(50, TimeUnit.MILLISECONDS);

      serversToFlush.clear();

      try (TabletsMetadata tablets =
          TabletsMetadata.builder().forTable(tableId).overlapping(startRow, endRow)
              .fetch(FLUSH_ID, LOCATION, LOGS, PREV_ROW).build(master.getContext())) {
        int tabletsToWaitFor = 0;
        int tabletCount = 0;

        for (TabletMetadata tablet : tablets) {
          int logs = tablet.getLogs().size();

          // when tablet is not online and has no logs, there is no reason to wait for it
          if ((tablet.hasCurrent() || logs > 0) && tablet.getFlushId().orElse(-1) < flushID) {
            tabletsToWaitFor++;
            if (tablet.hasCurrent())
              serversToFlush.add(tablet.getLocation());
          }

          tabletCount++;
        }

        if (tabletsToWaitFor == 0)
          break;

        // TODO detect case of table offline AND tablets w/ logs? - ACCUMULO-1296

        if (tabletCount == 0 && !Tables.exists(master.getContext(), tableId))
          throw new ThriftTableOperationException(tableId.canonical(), null, TableOperation.FLUSH,
              TableOperationExceptionType.NOTFOUND, null);

      } catch (TabletDeletedException e) {
        Master.log.debug("Failed to scan {} table to wait for flush {}", MetadataTable.NAME,
            tableId, e);
      }
    }

  }

  private NamespaceId getNamespaceIdFromTableId(TableOperation tableOp, TableId tableId)
      throws ThriftTableOperationException {
    NamespaceId namespaceId;
    try {
      namespaceId = Tables.getNamespaceId(master.getContext(), tableId);
    } catch (TableNotFoundException e) {
      throw new ThriftTableOperationException(tableId.canonical(), null, tableOp,
          TableOperationExceptionType.NOTFOUND, e.getMessage());
    }
    return namespaceId;
  }

  @Override
  public MasterMonitorInfo getMasterStats(TInfo info, TCredentials credentials) {
    return master.getMasterMonitorInfo();
  }

  @Override
  public void removeTableProperty(TInfo info, TCredentials credentials, String tableName,
      String property) throws ThriftSecurityException, ThriftTableOperationException {
    alterTableProperty(credentials, tableName, property, null, TableOperation.REMOVE_PROPERTY);
  }

  @Override
  public void setTableProperty(TInfo info, TCredentials credentials, String tableName,
      String property, String value) throws ThriftSecurityException, ThriftTableOperationException {
    alterTableProperty(credentials, tableName, property, value, TableOperation.SET_PROPERTY);
  }

  @Override
  public void shutdown(TInfo info, TCredentials c, boolean stopTabletServers)
      throws ThriftSecurityException {
    if (!master.security.canPerformSystemActions(c))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    if (stopTabletServers) {
      master.setMasterGoalState(MasterGoalState.CLEAN_STOP);
      EventCoordinator.Listener eventListener = master.nextEvent.getListener();
      do {
        eventListener.waitForEvents(Master.ONE_SECOND);
      } while (master.tserverSet.size() > 0);
    }
    master.setMasterState(MasterState.STOP);
  }

  @Override
  public void shutdownTabletServer(TInfo info, TCredentials c, String tabletServer, boolean force)
      throws ThriftSecurityException {
    if (!master.security.canPerformSystemActions(c))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    final TServerInstance doomed = master.tserverSet.find(tabletServer);
    if (!force) {
      final TServerConnection server = master.tserverSet.getConnection(doomed);
      if (server == null) {
        Master.log.warn("No server found for name {}", tabletServer);
        return;
      }
    }

    long tid = master.fate.startTransaction();

    log.debug("Seeding FATE op to shutdown " + tabletServer + " with tid " + tid);

    master.fate.seedTransaction(tid, new TraceRepo<>(new ShutdownTServer(doomed, force)), false);
    master.fate.waitForCompletion(tid);
    master.fate.delete(tid);

    log.debug("FATE op shutting down " + tabletServer + " finished");
  }

  @Override
  public void reportSplitExtent(TInfo info, TCredentials credentials, String serverName,
      TabletSplit split) {
    KeyExtent oldTablet = KeyExtent.fromThrift(split.oldTablet);
    if (master.migrations.remove(oldTablet) != null) {
      Master.log.info("Canceled migration of {}", split.oldTablet);
    }
    for (TServerInstance instance : master.tserverSet.getCurrentServers()) {
      if (serverName.equals(instance.getHostPort())) {
        master.nextEvent.event("%s reported split %s, %s", serverName,
            KeyExtent.fromThrift(split.newTablets.get(0)),
            KeyExtent.fromThrift(split.newTablets.get(1)));
        return;
      }
    }
    Master.log.warn("Got a split from a server we don't recognize: {}", serverName);
  }

  @Override
  public void reportTabletStatus(TInfo info, TCredentials credentials, String serverName,
      TabletLoadState status, TKeyExtent ttablet) {
    KeyExtent tablet = KeyExtent.fromThrift(ttablet);

    switch (status) {
      case LOAD_FAILURE:
        Master.log.error("{} reports assignment failed for tablet {}", serverName, tablet);
        break;
      case LOADED:
        master.nextEvent.event("tablet %s was loaded on %s", tablet, serverName);
        break;
      case UNLOADED:
        master.nextEvent.event("tablet %s was unloaded from %s", tablet, serverName);
        break;
      case UNLOAD_ERROR:
        Master.log.error("{} reports unload failed for tablet {}", serverName, tablet);
        break;
      case UNLOAD_FAILURE_NOT_SERVING:
        if (Master.log.isTraceEnabled()) {
          Master.log.trace("{} reports unload failed: not serving tablet, could be a split: {}",
              serverName, tablet);
        }
        break;
      case CHOPPED:
        master.nextEvent.event("tablet %s chopped", tablet);
        break;
    }
  }

  @Override
  public void setMasterGoalState(TInfo info, TCredentials c, MasterGoalState state)
      throws ThriftSecurityException {
    if (!master.security.canPerformSystemActions(c))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    master.setMasterGoalState(state);
  }

  @Override
  public void removeSystemProperty(TInfo info, TCredentials c, String property)
      throws ThriftSecurityException {
    if (!master.security.canPerformSystemActions(c))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    try {
      SystemPropUtil.removeSystemProperty(master.getContext(), property);
      updatePlugins(property);
    } catch (Exception e) {
      Master.log.error("Problem removing config property in zookeeper", e);
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void setSystemProperty(TInfo info, TCredentials c, String property, String value)
      throws ThriftSecurityException, TException {
    if (!master.security.canPerformSystemActions(c))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    try {
      SystemPropUtil.setSystemProperty(master.getContext(), property, value);
      updatePlugins(property);
    } catch (IllegalArgumentException iae) {
      // throw the exception here so it is not caught and converted to a generic TException
      throw iae;
    } catch (Exception e) {
      Master.log.error("Problem setting config property in zookeeper", e);
      throw new TException(e.getMessage());
    }
  }

  @Override
  public void setNamespaceProperty(TInfo tinfo, TCredentials credentials, String ns,
      String property, String value) throws ThriftSecurityException, ThriftTableOperationException {
    alterNamespaceProperty(credentials, ns, property, value, TableOperation.SET_PROPERTY);
  }

  @Override
  public void removeNamespaceProperty(TInfo tinfo, TCredentials credentials, String ns,
      String property) throws ThriftSecurityException, ThriftTableOperationException {
    alterNamespaceProperty(credentials, ns, property, null, TableOperation.REMOVE_PROPERTY);
  }

  private void alterNamespaceProperty(TCredentials c, String namespace, String property,
      String value, TableOperation op)
      throws ThriftSecurityException, ThriftTableOperationException {

    NamespaceId namespaceId = null;
    namespaceId = ClientServiceHandler.checkNamespaceId(master.getContext(), namespace, op);

    if (!master.security.canAlterNamespace(c, namespaceId))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    try {
      if (value == null) {
        NamespacePropUtil.removeNamespaceProperty(master.getContext(), namespaceId, property);
      } else {
        NamespacePropUtil.setNamespaceProperty(master.getContext(), namespaceId, property, value);
      }
    } catch (KeeperException.NoNodeException e) {
      // race condition... namespace no longer exists? This call will throw an exception if the
      // namespace was deleted:
      ClientServiceHandler.checkNamespaceId(master.getContext(), namespace, op);
      log.info("Error altering namespace property", e);
      throw new ThriftTableOperationException(namespaceId.canonical(), namespace, op,
          TableOperationExceptionType.OTHER, "Problem altering namespaceproperty");
    } catch (Exception e) {
      log.error("Problem altering namespace property", e);
      throw new ThriftTableOperationException(namespaceId.canonical(), namespace, op,
          TableOperationExceptionType.OTHER, "Problem altering namespace property");
    }
  }

  private void alterTableProperty(TCredentials c, String tableName, String property, String value,
      TableOperation op) throws ThriftSecurityException, ThriftTableOperationException {
    final TableId tableId = ClientServiceHandler.checkTableId(master.getContext(), tableName, op);
    NamespaceId namespaceId = getNamespaceIdFromTableId(op, tableId);
    if (!master.security.canAlterTable(c, tableId, namespaceId))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    try {
      if (value == null || value.isEmpty()) {
        TablePropUtil.removeTableProperty(master.getContext(), tableId, property);
      } else if (!TablePropUtil.setTableProperty(master.getContext(), tableId, property, value)) {
        throw new Exception("Invalid table property.");
      }
    } catch (KeeperException.NoNodeException e) {
      // race condition... table no longer exists? This call will throw an exception if the table
      // was deleted:
      ClientServiceHandler.checkTableId(master.getContext(), tableName, op);
      log.info("Error altering table property", e);
      throw new ThriftTableOperationException(tableId.canonical(), tableName, op,
          TableOperationExceptionType.OTHER, "Problem altering table property");
    } catch (Exception e) {
      log.error("Problem altering table property", e);
      throw new ThriftTableOperationException(tableId.canonical(), tableName, op,
          TableOperationExceptionType.OTHER, "Problem altering table property");
    }
  }

  private void updatePlugins(String property) {
    if (property.equals(Property.MASTER_TABLET_BALANCER.getKey())) {
      AccumuloConfiguration conf = master.getConfiguration();
      TabletBalancer balancer = Property.createInstanceFromPropertyName(conf,
          Property.MASTER_TABLET_BALANCER, TabletBalancer.class, new DefaultLoadBalancer());
      balancer.init(master.getContext());
      master.tabletBalancer = balancer;
      log.info("tablet balancer changed to {}", master.tabletBalancer.getClass().getName());
    }
  }

  @Override
  public void waitForBalance(TInfo tinfo) {
    master.waitForBalance();
  }

  @Override
  public List<String> getActiveTservers(TInfo tinfo, TCredentials credentials) {
    Set<TServerInstance> tserverInstances = master.onlineTabletServers();
    List<String> servers = new ArrayList<>();
    for (TServerInstance tserverInstance : tserverInstances) {
      servers.add(tserverInstance.getHostPort());
    }

    return servers;
  }

  @Override
  public TDelegationToken getDelegationToken(TInfo tinfo, TCredentials credentials,
      TDelegationTokenConfig tConfig) throws ThriftSecurityException, TException {
    if (!master.security.canObtainDelegationToken(credentials)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    // Make sure we're actually generating the secrets to make delegation tokens
    // Round-about way to verify that SASL is also enabled.
    if (!master.delegationTokensAvailable()) {
      throw new TException("Delegation tokens are not available for use");
    }

    final DelegationTokenConfig config = DelegationTokenConfigSerializer.deserialize(tConfig);
    final AuthenticationTokenSecretManager secretManager = master.getContext().getSecretManager();
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
  public boolean drainReplicationTable(TInfo tfino, TCredentials credentials, String tableName,
      Set<String> logsToWatch) throws TException {
    AccumuloClient client = master.getContext();

    final Text tableId = new Text(getTableId(master.getContext(), tableName).canonical());

    drainLog.trace("Waiting for {} to be replicated for {}", logsToWatch, tableId);

    drainLog.trace("Reading from metadata table");
    final Set<Range> range = Collections.singleton(new Range(ReplicationSection.getRange()));
    BatchScanner bs;
    try {
      bs = client.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 4);
    } catch (TableNotFoundException e) {
      throw new RuntimeException("Could not read metadata table", e);
    }
    bs.setRanges(range);
    bs.fetchColumnFamily(ReplicationSection.COLF);
    try {
      // Return immediately if there are records in metadata for these WALs
      if (!allReferencesReplicated(bs, tableId, logsToWatch)) {
        return false;
      }
    } finally {
      bs.close();
    }

    drainLog.trace("reading from replication table");
    try {
      bs = client.createBatchScanner(ReplicationTable.NAME, Authorizations.EMPTY, 4);
    } catch (TableNotFoundException e) {
      throw new RuntimeException("Replication table was not found", e);
    }
    bs.setRanges(Collections.singleton(new Range()));
    try {
      // No records in metadata, check replication table
      return allReferencesReplicated(bs, tableId, logsToWatch);
    } finally {
      bs.close();
    }
  }

  protected TableId getTableId(ClientContext context, String tableName)
      throws ThriftTableOperationException {
    return ClientServiceHandler.checkTableId(context, tableName, null);
  }

  /**
   * @return return true records are only in place which are fully replicated
   */
  protected boolean allReferencesReplicated(BatchScanner bs, Text tableId,
      Set<String> relevantLogs) {
    Text rowHolder = new Text(), colfHolder = new Text();
    for (Entry<Key,Value> entry : bs) {
      drainLog.trace("Got key {}", entry.getKey().toStringNoTruncate());

      entry.getKey().getColumnQualifier(rowHolder);
      if (tableId.equals(rowHolder)) {
        entry.getKey().getRow(rowHolder);
        entry.getKey().getColumnFamily(colfHolder);

        String file;
        if (colfHolder.equals(ReplicationSection.COLF)) {
          file = rowHolder.toString();
          file = file.substring(ReplicationSection.getRowPrefix().length());
        } else if (colfHolder.equals(OrderSection.NAME)) {
          file = OrderSection.getFile(entry.getKey(), rowHolder);
          long timeClosed = OrderSection.getTimeClosed(entry.getKey(), rowHolder);
          drainLog.trace("Order section: {} and {}", timeClosed, file);
        } else {
          file = rowHolder.toString();
        }

        // Skip files that we didn't observe when we started (new files/data)
        if (relevantLogs.contains(file)) {
          drainLog.trace("Found file that we *do* care about {}", file);
        } else {
          drainLog.trace("Found file that we didn't care about {}", file);
          continue;
        }

        try {
          Status stat = Status.parseFrom(entry.getValue().get());
          if (!StatusUtil.isFullyReplicated(stat)) {
            drainLog.trace("{} and {} is not replicated", file, ProtobufUtil.toString(stat));
            return false;
          }
          drainLog.trace("{} and {} is replicated", file, ProtobufUtil.toString(stat));
        } catch (InvalidProtocolBufferException e) {
          drainLog.trace("Could not parse protobuf for {}", entry.getKey(), e);
        }
      }
    }

    return true;
  }
}
