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
package org.apache.accumulo.master;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterGoalState;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.MasterState;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.master.thrift.TabletSplit;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter.Mutator;
import org.apache.accumulo.master.tableOps.TraceRepo;
import org.apache.accumulo.master.tserverOps.ShutdownTServer;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.balancer.DefaultLoadBalancer;
import org.apache.accumulo.server.master.balancer.TabletBalancer;
import org.apache.accumulo.server.master.state.DeadServerList;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletServerState;
import org.apache.accumulo.server.util.DefaultMap;
import org.apache.accumulo.server.util.NamespacePropUtil;
import org.apache.accumulo.server.util.SystemPropUtil;
import org.apache.accumulo.server.util.TableInfoUtil;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.accumulo.server.util.TabletIterator.TabletDeletedException;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.trace.thrift.TInfo;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

class MasterClientServiceHandler extends FateServiceHandler implements MasterClientService.Iface {

  private static final Logger log = Master.log;
  private Instance instance;

  MasterClientServiceHandler(Master master) {
    super(master);
    this.instance = master.getInstance();
  }

  @Override
  public long initiateFlush(TInfo tinfo, TCredentials c, String tableId) throws ThriftSecurityException, ThriftTableOperationException {
    String namespaceId = Tables.getNamespaceId(instance, tableId);
    master.security.canFlush(c, tableId, namespaceId);

    String zTablePath = Constants.ZROOT + "/" + master.getConfiguration().getInstance().getInstanceID() + Constants.ZTABLES + "/" + tableId
        + Constants.ZTABLE_FLUSH_ID;

    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    byte fid[];
    try {
      fid = zoo.mutate(zTablePath, null, null, new Mutator() {
        @Override
        public byte[] mutate(byte[] currentValue) throws Exception {
          long flushID = Long.parseLong(new String(currentValue));
          flushID++;
          return ("" + flushID).getBytes();
        }
      });
    } catch (NoNodeException nne) {
      throw new ThriftTableOperationException(tableId, null, TableOperation.FLUSH, TableOperationExceptionType.NOTFOUND, null);
    } catch (Exception e) {
      Master.log.warn(e.getMessage(), e);
      throw new ThriftTableOperationException(tableId, null, TableOperation.FLUSH, TableOperationExceptionType.OTHER, null);
    }
    return Long.parseLong(new String(fid));
  }

  @Override
  public void waitForFlush(TInfo tinfo, TCredentials c, String tableId, ByteBuffer startRow, ByteBuffer endRow, long flushID, long maxLoops)
      throws ThriftSecurityException, ThriftTableOperationException {
    String namespaceId = Tables.getNamespaceId(instance, tableId);
    master.security.canFlush(c, tableId, namespaceId);

    if (endRow != null && startRow != null && ByteBufferUtil.toText(startRow).compareTo(ByteBufferUtil.toText(endRow)) >= 0)
      throw new ThriftTableOperationException(tableId, null, TableOperation.FLUSH, TableOperationExceptionType.BAD_RANGE, "start row must be less than end row");

    Set<TServerInstance> serversToFlush = new HashSet<TServerInstance>(master.tserverSet.getCurrentServers());

    for (long l = 0; l < maxLoops; l++) {

      for (TServerInstance instance : serversToFlush) {
        try {
          final TServerConnection server = master.tserverSet.getConnection(instance);
          if (server != null)
            server.flush(master.masterLock, tableId, ByteBufferUtil.toBytes(startRow), ByteBufferUtil.toBytes(endRow));
        } catch (TException ex) {
          Master.log.error(ex.toString());
        }
      }

      if (l == maxLoops - 1)
        break;

      UtilWaitThread.sleep(50);

      serversToFlush.clear();

      try {
        Connector conn = master.getConnector();
        Scanner scanner;
        if (tableId.equals(MetadataTable.ID)) {
          scanner = new IsolatedScanner(conn.createScanner(RootTable.NAME, Authorizations.EMPTY));
          scanner.setRange(MetadataSchema.TabletsSection.getRange());
        } else {
          scanner = new IsolatedScanner(conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY));
          scanner.setRange(new KeyExtent(new Text(tableId), null, ByteBufferUtil.toText(startRow)).toMetadataRange());
        }
        TabletsSection.ServerColumnFamily.FLUSH_COLUMN.fetch(scanner);
        TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.fetch(scanner);
        scanner.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);
        scanner.fetchColumnFamily(LogColumnFamily.NAME);

        RowIterator ri = new RowIterator(scanner);

        int tabletsToWaitFor = 0;
        int tabletCount = 0;

        Text ert = ByteBufferUtil.toText(endRow);

        while (ri.hasNext()) {
          Iterator<Entry<Key,Value>> row = ri.next();
          long tabletFlushID = -1;
          int logs = 0;
          boolean online = false;

          TServerInstance server = null;

          Entry<Key,Value> entry = null;
          while (row.hasNext()) {
            entry = row.next();
            Key key = entry.getKey();

            if (TabletsSection.ServerColumnFamily.FLUSH_COLUMN.equals(key.getColumnFamily(), key.getColumnQualifier())) {
              tabletFlushID = Long.parseLong(entry.getValue().toString());
            }

            if (LogColumnFamily.NAME.equals(key.getColumnFamily()))
              logs++;

            if (TabletsSection.CurrentLocationColumnFamily.NAME.equals(key.getColumnFamily())) {
              online = true;
              server = new TServerInstance(entry.getValue(), key.getColumnQualifier());
            }

          }

          // when tablet is not online and has no logs, there is no reason to wait for it
          if ((online || logs > 0) && tabletFlushID < flushID) {
            tabletsToWaitFor++;
            if (server != null)
              serversToFlush.add(server);
          }

          tabletCount++;

          Text tabletEndRow = new KeyExtent(entry.getKey().getRow(), (Text) null).getEndRow();
          if (tabletEndRow == null || (ert != null && tabletEndRow.compareTo(ert) >= 0))
            break;
        }

        if (tabletsToWaitFor == 0)
          break;

        // TODO detect case of table offline AND tablets w/ logs? - ACCUMULO-1296

        if (tabletCount == 0 && !Tables.exists(master.getInstance(), tableId))
          throw new ThriftTableOperationException(tableId, null, TableOperation.FLUSH, TableOperationExceptionType.NOTFOUND, null);

      } catch (AccumuloException e) {
        Master.log.debug("Failed to scan " + MetadataTable.NAME + " table to wait for flush " + tableId, e);
      } catch (TabletDeletedException tde) {
        Master.log.debug("Failed to scan " + MetadataTable.NAME + " table to wait for flush " + tableId, tde);
      } catch (AccumuloSecurityException e) {
        Master.log.warn(e.getMessage(), e);
        throw new ThriftSecurityException();
      } catch (TableNotFoundException e) {
        Master.log.error(e.getMessage(), e);
        throw new ThriftTableOperationException();
      }
    }

  }

  @Override
  public MasterMonitorInfo getMasterStats(TInfo info, TCredentials credentials) throws ThriftSecurityException {
    final MasterMonitorInfo result = new MasterMonitorInfo();

    result.tServerInfo = new ArrayList<TabletServerStatus>();
    result.tableMap = new DefaultMap<String,TableInfo>(new TableInfo());
    for (Entry<TServerInstance,TabletServerStatus> serverEntry : master.tserverStatus.entrySet()) {
      final TabletServerStatus status = serverEntry.getValue();
      result.tServerInfo.add(status);
      for (Entry<String,TableInfo> entry : status.tableMap.entrySet()) {
        TableInfoUtil.add(result.tableMap.get(entry.getKey()), entry.getValue());
      }
    }
    result.badTServers = new HashMap<String,Byte>();
    synchronized (master.badServers) {
      for (TServerInstance bad : master.badServers.keySet()) {
        result.badTServers.put(bad.hostPort(), TabletServerState.UNRESPONSIVE.getId());
      }
    }
    result.state = master.getMasterState();
    result.goalState = master.getMasterGoalState();
    result.unassignedTablets = master.displayUnassigned();
    result.serversShuttingDown = new HashSet<String>();
    synchronized (master.serversToShutdown) {
      for (TServerInstance server : master.serversToShutdown)
        result.serversShuttingDown.add(server.hostPort());
    }
    DeadServerList obit = new DeadServerList(ZooUtil.getRoot(master.getInstance()) + Constants.ZDEADTSERVERS);
    result.deadTabletServers = obit.getList();
    return result;
  }

  @Override
  public void removeTableProperty(TInfo info, TCredentials credentials, String tableName, String property) throws ThriftSecurityException,
      ThriftTableOperationException {
    alterTableProperty(credentials, tableName, property, null, TableOperation.REMOVE_PROPERTY);
  }

  @Override
  public void setTableProperty(TInfo info, TCredentials credentials, String tableName, String property, String value) throws ThriftSecurityException,
      ThriftTableOperationException {
    alterTableProperty(credentials, tableName, property, value, TableOperation.SET_PROPERTY);
  }

  @Override
  public void shutdown(TInfo info, TCredentials c, boolean stopTabletServers) throws ThriftSecurityException {
    master.security.canPerformSystemActions(c);
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
  public void shutdownTabletServer(TInfo info, TCredentials c, String tabletServer, boolean force) throws ThriftSecurityException {
    master.security.canPerformSystemActions(c);

    final TServerInstance doomed = master.tserverSet.find(tabletServer);
    if (!force) {
      final TServerConnection server = master.tserverSet.getConnection(doomed);
      if (server == null) {
        Master.log.warn("No server found for name " + tabletServer);
        return;
      }
    }

    long tid = master.fate.startTransaction();
    master.fate.seedTransaction(tid, new TraceRepo<Master>(new ShutdownTServer(doomed, force)), false);
    master.fate.waitForCompletion(tid);
    master.fate.delete(tid);
  }

  @Override
  public void reportSplitExtent(TInfo info, TCredentials credentials, String serverName, TabletSplit split) {
    KeyExtent oldTablet = new KeyExtent(split.oldTablet);
    if (master.migrations.remove(oldTablet) != null) {
      Master.log.info("Canceled migration of " + split.oldTablet);
    }
    for (TServerInstance instance : master.tserverSet.getCurrentServers()) {
      if (serverName.equals(instance.hostPort())) {
        master.nextEvent.event("%s reported split %s, %s", serverName, new KeyExtent(split.newTablets.get(0)), new KeyExtent(split.newTablets.get(1)));
        return;
      }
    }
    Master.log.warn("Got a split from a server we don't recognize: " + serverName);
  }

  @Override
  public void reportTabletStatus(TInfo info, TCredentials credentials, String serverName, TabletLoadState status, TKeyExtent ttablet) {
    KeyExtent tablet = new KeyExtent(ttablet);

    switch (status) {
      case LOAD_FAILURE:
        Master.log.error(serverName + " reports assignment failed for tablet " + tablet);
        break;
      case LOADED:
        master.nextEvent.event("tablet %s was loaded on %s", tablet, serverName);
        break;
      case UNLOADED:
        master.nextEvent.event("tablet %s was unloaded from %s", tablet, serverName);
        break;
      case UNLOAD_ERROR:
        Master.log.error(serverName + " reports unload failed for tablet " + tablet);
        break;
      case UNLOAD_FAILURE_NOT_SERVING:
        if (Master.log.isTraceEnabled()) {
          Master.log.trace(serverName + " reports unload failed: not serving tablet, could be a split: " + tablet);
        }
        break;
      case CHOPPED:
        master.nextEvent.event("tablet %s chopped", tablet);
        break;
    }
  }

  @Override
  public void setMasterGoalState(TInfo info, TCredentials c, MasterGoalState state) throws ThriftSecurityException {
    master.security.canPerformSystemActions(c);

    master.setMasterGoalState(state);
  }

  @Override
  public void removeSystemProperty(TInfo info, TCredentials c, String property) throws ThriftSecurityException {
    master.security.canPerformSystemActions(c);

    try {
      SystemPropUtil.removeSystemProperty(property);
      updatePlugins(property);
    } catch (Exception e) {
      Master.log.error("Problem removing config property in zookeeper", e);
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void setSystemProperty(TInfo info, TCredentials c, String property, String value) throws ThriftSecurityException, TException {
    master.security.canPerformSystemActions(c);

    try {
      SystemPropUtil.setSystemProperty(property, value);
      updatePlugins(property);
    } catch (Exception e) {
      Master.log.error("Problem setting config property in zookeeper", e);
      throw new TException(e.getMessage());
    }
  }

  @Override
  public void setNamespaceProperty(TInfo tinfo, TCredentials credentials, String ns, String property, String value) throws ThriftSecurityException,
      ThriftTableOperationException {
    alterNamespaceProperty(credentials, ns, property, value, TableOperation.SET_PROPERTY);
  }

  @Override
  public void removeNamespaceProperty(TInfo tinfo, TCredentials credentials, String ns, String property) throws ThriftSecurityException,
      ThriftTableOperationException {
    alterNamespaceProperty(credentials, ns, property, null, TableOperation.REMOVE_PROPERTY);
  }

  private void alterNamespaceProperty(TCredentials c, String namespace, String property, String value, TableOperation op) throws ThriftSecurityException,
      ThriftTableOperationException {

    String namespaceId = null;
    namespaceId = ClientServiceHandler.checkNamespaceId(master.getInstance(), namespace, op);

    if (!master.security.canAlterNamespace(c, namespaceId))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    try {
      if (value == null) {
        NamespacePropUtil.removeNamespaceProperty(namespaceId, property);
      } else {
        NamespacePropUtil.setNamespaceProperty(namespaceId, property, value);
      }
    } catch (KeeperException.NoNodeException e) {
      // race condition... namespace no longer exists? This call will throw an exception if the namespace was deleted:
      ClientServiceHandler.checkNamespaceId(master.getInstance(), namespaceId, op);
      log.info("Error altering namespace property", e);
      throw new ThriftTableOperationException(namespaceId, namespace, op, TableOperationExceptionType.OTHER, "Problem altering namespaceproperty");
    } catch (Exception e) {
      log.error("Problem altering namespace property", e);
      throw new ThriftTableOperationException(namespaceId, namespace, op, TableOperationExceptionType.OTHER, "Problem altering namespace property");
    }
  }

  private void alterTableProperty(TCredentials c, String tableName, String property, String value, TableOperation op) throws ThriftSecurityException,
      ThriftTableOperationException {
    final String tableId = ClientServiceHandler.checkTableId(master.getInstance(), tableName, op);
    String namespaceId = Tables.getNamespaceId(master.getInstance(), tableId);
    if (!master.security.canAlterTable(c, tableId, namespaceId))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    try {
      if (value == null || value.isEmpty()) {
        TablePropUtil.removeTableProperty(tableId, property);
      } else if (!TablePropUtil.setTableProperty(tableId, property, value)) {
        throw new Exception("Invalid table property.");
      }
    } catch (KeeperException.NoNodeException e) {
      // race condition... table no longer exists? This call will throw an exception if the table was deleted:
      ClientServiceHandler.checkTableId(master.getInstance(), tableName, op);
      log.info("Error altering table property", e);
      throw new ThriftTableOperationException(tableId, tableName, op, TableOperationExceptionType.OTHER, "Problem altering table property");
    } catch (Exception e) {
      log.error("Problem altering table property", e);
      throw new ThriftTableOperationException(tableId, tableName, op, TableOperationExceptionType.OTHER, "Problem altering table property");
    }
  }

  private void updatePlugins(String property) {
    if (property.equals(Property.MASTER_TABLET_BALANCER.getKey())) {
      TabletBalancer balancer = ServerConfiguration.getSystemConfiguration(master.getInstance()).instantiateClassProperty(Property.MASTER_TABLET_BALANCER,
          TabletBalancer.class, new DefaultLoadBalancer());
      balancer.init(master.getConfiguration());
      master.tabletBalancer = balancer;
      log.info("tablet balancer changed to " + master.tabletBalancer.getClass().getName());
    }
  }

}
