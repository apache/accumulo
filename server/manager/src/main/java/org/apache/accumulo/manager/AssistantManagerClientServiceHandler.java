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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.TVersionedProperties;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftConcurrentModificationException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.manager.thrift.AssistantManagerClientService;
import org.apache.accumulo.core.manager.thrift.ThriftPropertyException;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletDeletedException;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;

public class AssistantManagerClientServiceHandler implements AssistantManagerClientService.Iface {

  private static final Logger log = Manager.log;
  private final Manager manager;
  private final ServerContext context;
  private final AuditedSecurityOperation security;

  protected AssistantManagerClientServiceHandler(Manager manager) {
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
    } catch (KeeperException.NoNodeException nne) {
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
          final LiveTServerSet.TServerConnection server =
              manager.tserverSet.getConnection(instance);
          if (server != null) {
            server.flush(manager.primaryManagerLock, tableId, ByteBufferUtil.toBytes(startRowBB),
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

  @Override
  public void setTableProperty(TInfo tinfo, TCredentials credentials, String tableName,
      String property, String value) throws ThriftSecurityException, ThriftTableOperationException,
      ThriftNotActiveServiceException, ThriftPropertyException, TException {

  }

  @Override
  public void modifyTableProperties(TInfo tinfo, TCredentials credentials, String tableName,
      TVersionedProperties vProperties) throws ThriftSecurityException,
      ThriftTableOperationException, ThriftNotActiveServiceException,
      ThriftConcurrentModificationException, ThriftPropertyException, TException {

  }

  @Override
  public void removeTableProperty(TInfo tinfo, TCredentials credentials, String tableName,
      String property) throws ThriftSecurityException, ThriftTableOperationException,
      ThriftNotActiveServiceException, TException {

  }

  @Override
  public void setNamespaceProperty(TInfo tinfo, TCredentials credentials, String ns,
      String property, String value) throws ThriftSecurityException, ThriftTableOperationException,
      ThriftNotActiveServiceException, ThriftPropertyException, TException {

  }

  @Override
  public void modifyNamespaceProperties(TInfo tinfo, TCredentials credentials, String ns,
      TVersionedProperties vProperties) throws ThriftSecurityException,
      ThriftTableOperationException, ThriftNotActiveServiceException,
      ThriftConcurrentModificationException, ThriftPropertyException, TException {

  }

  @Override
  public void removeNamespaceProperty(TInfo tinfo, TCredentials credentials, String ns,
      String property) throws ThriftSecurityException, ThriftTableOperationException,
      ThriftNotActiveServiceException, TException {

  }

  @Override
  public void setSystemProperty(TInfo tinfo, TCredentials credentials, String property,
      String value) throws ThriftSecurityException, ThriftNotActiveServiceException,
      ThriftPropertyException, TException {

  }

  @Override
  public void modifySystemProperties(TInfo tinfo, TCredentials credentials,
      TVersionedProperties vProperties)
      throws ThriftSecurityException, ThriftNotActiveServiceException,
      ThriftConcurrentModificationException, ThriftPropertyException, TException {

  }

  @Override
  public void removeSystemProperty(TInfo tinfo, TCredentials credentials, String property)
      throws ThriftSecurityException, ThriftNotActiveServiceException, TException {

  }
}
