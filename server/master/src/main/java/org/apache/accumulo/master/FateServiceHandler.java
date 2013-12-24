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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperationsImpl;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.master.thrift.FateOperation;
import org.apache.accumulo.core.master.thrift.FateService;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.fate.TStore.TStatus;
import org.apache.accumulo.master.tableOps.BulkImport;
import org.apache.accumulo.master.tableOps.CancelCompactions;
import org.apache.accumulo.master.tableOps.ChangeTableState;
import org.apache.accumulo.master.tableOps.CloneTable;
import org.apache.accumulo.master.tableOps.CompactRange;
import org.apache.accumulo.master.tableOps.CreateNamespace;
import org.apache.accumulo.master.tableOps.CreateTable;
import org.apache.accumulo.master.tableOps.DeleteNamespace;
import org.apache.accumulo.master.tableOps.DeleteTable;
import org.apache.accumulo.master.tableOps.ExportTable;
import org.apache.accumulo.master.tableOps.ImportTable;
import org.apache.accumulo.master.tableOps.RenameNamespace;
import org.apache.accumulo.master.tableOps.RenameTable;
import org.apache.accumulo.master.tableOps.TableRangeOp;
import org.apache.accumulo.master.tableOps.TraceRepo;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.accumulo.trace.thrift.TInfo;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * 
 */
class FateServiceHandler implements FateService.Iface {

  protected final Master master;
  protected static final Logger log = Master.log;

  public FateServiceHandler(Master master) {
    this.master = master;
  }

  @Override
  public long beginFateOperation(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    return master.fate.startTransaction();
  }

  @Override
  public void executeFateOperation(TInfo tinfo, TCredentials c, long opid, FateOperation op, List<ByteBuffer> arguments, Map<String,String> options,
      boolean autoCleanup) throws ThriftSecurityException, ThriftTableOperationException {
    authenticate(c);

    switch (op) {
      case NAMESPACE_CREATE: {
        String namespace = ByteBufferUtil.toString(arguments.get(0));
        if (!master.security.canCreateNamespace(c, namespace))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        checkNotSystemNamespace(namespace, TableOperation.CREATE);
        checkNamespaceName(namespace, TableOperation.CREATE);
        master.fate.seedTransaction(opid, new TraceRepo<Master>(new CreateNamespace(c.getPrincipal(), namespace, options)), autoCleanup);
        break;
      }
      case NAMESPACE_RENAME: {

        String oldName = ByteBufferUtil.toString(arguments.get(0));
        String newName = ByteBufferUtil.toString(arguments.get(1));
        String namespaceId = checkNamespaceId(oldName, TableOperation.RENAME);

        checkNotSystemNamespace(oldName, TableOperation.RENAME);
        checkNotSystemNamespace(newName, TableOperation.RENAME);
        checkNamespaceName(newName, TableOperation.RENAME);
        if (!master.security.canRenameNamespace(c, namespaceId, oldName, newName))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<Master>(new RenameNamespace(namespaceId, oldName, newName)), autoCleanup);
        break;
      }
      case NAMESPACE_DELETE: {
        String namespace = ByteBufferUtil.toString(arguments.get(0));
        checkNotSystemNamespace(namespace, TableOperation.DELETE);
        String namespaceId = checkNamespaceId(namespace, TableOperation.DELETE);
        if (!master.security.canDeleteNamespace(c, namespaceId))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<Master>(new DeleteNamespace(namespaceId)), autoCleanup);
        break;
      }
      case TABLE_CREATE: {
        String tableName = ByteBufferUtil.toString(arguments.get(0));
        if (!master.security.canCreateTable(c, tableName))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        checkNotMetadataTable(tableName, TableOperation.DELETE);
        checkTableName(tableName, TableOperation.CREATE);

        TimeType timeType = TimeType.valueOf(ByteBufferUtil.toString(arguments.get(1)));

        try {
          master.fate.seedTransaction(opid, new TraceRepo<Master>(new CreateTable(c.getPrincipal(), tableName, timeType, options)), autoCleanup);
        } catch (NamespaceNotFoundException e) {
          throw new ThriftTableOperationException(null, tableName, TableOperation.CREATE, TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }
        break;
      }
      case TABLE_RENAME: {
        String oldTableName = ByteBufferUtil.toString(arguments.get(0));
        String newTableName = ByteBufferUtil.toString(arguments.get(1));

        String tableId = ClientServiceHandler.checkTableId(master.getInstance(), oldTableName, TableOperation.RENAME);
        checkNotMetadataTable(oldTableName, TableOperation.RENAME);
        checkNotMetadataTable(newTableName, TableOperation.RENAME);
        checkTableName(newTableName, TableOperation.RENAME);
        if (!master.security.canRenameTable(c, tableId, oldTableName, newTableName))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        try {
          master.fate.seedTransaction(opid, new TraceRepo<Master>(new RenameTable(tableId, oldTableName, newTableName)), autoCleanup);
        } catch (NamespaceNotFoundException e) {
          throw new ThriftTableOperationException(null, oldTableName, TableOperation.RENAME, TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }

        break;
      }
      case TABLE_CLONE: {
        String srcTableId = ByteBufferUtil.toString(arguments.get(0));
        String tableName = ByteBufferUtil.toString(arguments.get(1));
        checkNotMetadataTable(tableName, TableOperation.CLONE);
        checkTableName(tableName, TableOperation.CLONE);
        if (!master.security.canCloneTable(c, srcTableId, tableName))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        Map<String,String> propertiesToSet = new HashMap<String,String>();
        Set<String> propertiesToExclude = new HashSet<String>();

        for (Entry<String,String> entry : options.entrySet()) {
          if (entry.getKey().startsWith(TableOperationsImpl.CLONE_EXCLUDE_PREFIX)) {
            propertiesToExclude.add(entry.getKey().substring(TableOperationsImpl.CLONE_EXCLUDE_PREFIX.length()));
            continue;
          }

          if (!TablePropUtil.isPropertyValid(entry.getKey(), entry.getValue())) {
            throw new ThriftTableOperationException(null, tableName, TableOperation.CLONE, TableOperationExceptionType.OTHER, "Property or value not valid "
                + entry.getKey() + "=" + entry.getValue());
          }

          propertiesToSet.put(entry.getKey(), entry.getValue());
        }

        master.fate.seedTransaction(opid, new TraceRepo<Master>(new CloneTable(c.getPrincipal(), srcTableId, tableName, propertiesToSet, propertiesToExclude)),
            autoCleanup);

        break;
      }
      case TABLE_DELETE: {
        String tableName = ByteBufferUtil.toString(arguments.get(0));
        final String tableId = ClientServiceHandler.checkTableId(master.getInstance(), tableName, TableOperation.DELETE);
        checkNotMetadataTable(tableName, TableOperation.DELETE);
        if (!master.security.canDeleteTable(c, tableId))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        master.fate.seedTransaction(opid, new TraceRepo<Master>(new DeleteTable(tableId)), autoCleanup);
        break;
      }
      case TABLE_ONLINE: {
        final String tableId = ByteBufferUtil.toString(arguments.get(0));
        checkNotRootID(tableId, TableOperation.ONLINE);

        if (!master.security.canOnlineOfflineTable(c, tableId, op))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<Master>(new ChangeTableState(tableId, TableOperation.ONLINE)), autoCleanup);
        break;
      }
      case TABLE_OFFLINE: {
        final String tableId = ByteBufferUtil.toString(arguments.get(0));
        checkNotRootID(tableId, TableOperation.OFFLINE);

        if (!master.security.canOnlineOfflineTable(c, tableId, op))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<Master>(new ChangeTableState(tableId, TableOperation.OFFLINE)), autoCleanup);
        break;
      }
      case TABLE_MERGE: {
        String tableName = ByteBufferUtil.toString(arguments.get(0));
        Text startRow = ByteBufferUtil.toText(arguments.get(1));
        Text endRow = ByteBufferUtil.toText(arguments.get(2));
        final String tableId = ClientServiceHandler.checkTableId(master.getInstance(), tableName, TableOperation.MERGE);
        Master.log.debug("Creating merge op: " + tableId + " " + startRow + " " + endRow);

        if (!master.security.canMerge(c, tableId))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<Master>(new TableRangeOp(MergeInfo.Operation.MERGE, tableId, startRow, endRow)), autoCleanup);
        break;
      }
      case TABLE_DELETE_RANGE: {
        String tableName = ByteBufferUtil.toString(arguments.get(0));
        Text startRow = ByteBufferUtil.toText(arguments.get(1));
        Text endRow = ByteBufferUtil.toText(arguments.get(2));

        final String tableId = ClientServiceHandler.checkTableId(master.getInstance(), tableName, TableOperation.DELETE_RANGE);
        checkNotMetadataTable(tableName, TableOperation.DELETE_RANGE);

        if (!master.security.canDeleteRange(c, tableId, tableName, startRow, endRow))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<Master>(new TableRangeOp(MergeInfo.Operation.DELETE, tableId, startRow, endRow)), autoCleanup);
        break;
      }
      case TABLE_BULK_IMPORT: {
        String tableName = ByteBufferUtil.toString(arguments.get(0));
        String dir = ByteBufferUtil.toString(arguments.get(1));
        String failDir = ByteBufferUtil.toString(arguments.get(2));
        boolean setTime = Boolean.parseBoolean(ByteBufferUtil.toString(arguments.get(3)));

        final String tableId = ClientServiceHandler.checkTableId(master.getInstance(), tableName, TableOperation.BULK_IMPORT);
        checkNotMetadataTable(tableName, TableOperation.BULK_IMPORT);

        if (!master.security.canBulkImport(c, tableId, tableName, dir, failDir))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<Master>(new BulkImport(tableId, dir, failDir, setTime)), autoCleanup);
        break;
      }
      case TABLE_COMPACT: {
        String tableId = ByteBufferUtil.toString(arguments.get(0));
        byte[] startRow = ByteBufferUtil.toBytes(arguments.get(1));
        byte[] endRow = ByteBufferUtil.toBytes(arguments.get(2));
        List<IteratorSetting> iterators = IteratorUtil.decodeIteratorSettings(ByteBufferUtil.toBytes(arguments.get(3)));

        if (!master.security.canCompact(c, tableId))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<Master>(new CompactRange(tableId, startRow, endRow, iterators)), autoCleanup);
        break;
      }
      case TABLE_CANCEL_COMPACT: {
        String tableId = ByteBufferUtil.toString(arguments.get(0));

        if (!master.security.canCompact(c, tableId))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<Master>(new CancelCompactions(tableId)), autoCleanup);
        break;
      }
      case TABLE_IMPORT: {
        String tableName = ByteBufferUtil.toString(arguments.get(0));
        String exportDir = ByteBufferUtil.toString(arguments.get(1));

        if (!master.security.canImport(c, tableName, exportDir))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        checkNotMetadataTable(tableName, TableOperation.IMPORT);
        checkTableName(tableName, TableOperation.CREATE);

        try {
          master.fate.seedTransaction(opid, new TraceRepo<Master>(new ImportTable(c.getPrincipal(), tableName, exportDir)), autoCleanup);
        } catch (NamespaceNotFoundException e) {
          throw new ThriftTableOperationException(null, tableName, TableOperation.IMPORT, TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }
        break;
      }
      case TABLE_EXPORT: {
        String tableName = ByteBufferUtil.toString(arguments.get(0));
        String exportDir = ByteBufferUtil.toString(arguments.get(1));

        String tableId = ClientServiceHandler.checkTableId(master.getInstance(), tableName, TableOperation.EXPORT);

        if (!master.security.canExport(c, tableId, tableName, exportDir))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        checkNotMetadataTable(tableName, TableOperation.EXPORT);

        master.fate.seedTransaction(opid, new TraceRepo<Master>(new ExportTable(tableName, tableId, exportDir)), autoCleanup);
        break;
      }

      default:
        throw new UnsupportedOperationException();
    }

  }

  @Override
  public String waitForFateOperation(TInfo tinfo, TCredentials credentials, long opid) throws ThriftSecurityException, ThriftTableOperationException {
    authenticate(credentials);

    TStatus status = master.fate.waitForCompletion(opid);
    if (status == TStatus.FAILED) {
      Exception e = master.fate.getException(opid);
      if (e instanceof ThriftTableOperationException)
        throw (ThriftTableOperationException) e;
      else if (e instanceof ThriftSecurityException)
        throw (ThriftSecurityException) e;
      else if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      else
        throw new RuntimeException(e);
    }

    String ret = master.fate.getReturn(opid);
    if (ret == null)
      ret = ""; // thrift does not like returning null
    return ret;
  }

  @Override
  public void finishFateOperation(TInfo tinfo, TCredentials credentials, long opid) throws ThriftSecurityException {
    authenticate(credentials);
    master.fate.delete(opid);
  }

  protected void authenticate(TCredentials c) throws ThriftSecurityException {
    if (!master.security.authenticateUser(c, c))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.BAD_CREDENTIALS);
  }

  private static void checkNotRootID(String tableId, TableOperation operation) throws ThriftTableOperationException {
    if (RootTable.ID.equals(tableId)) {
      String why = "Table name cannot be == " + RootTable.NAME;
      log.warn(why);
      throw new ThriftTableOperationException(tableId, null, operation, TableOperationExceptionType.OTHER, why);
    }
  }

  private static void checkNotMetadataTable(String tableName, TableOperation operation) throws ThriftTableOperationException {
    if (MetadataTable.NAME.equals(tableName) || RootTable.NAME.equals(tableName)) {
      String why = "Table names cannot be == " + RootTable.NAME + " or " + MetadataTable.NAME;
      log.warn(why);
      throw new ThriftTableOperationException(null, tableName, operation, TableOperationExceptionType.OTHER, why);
    }
  }

  private static void checkNotSystemNamespace(String namespace, TableOperation operation) throws ThriftTableOperationException {
    if (Namespaces.ACCUMULO_NAMESPACE.equals(namespace)) {
      String why = "Namespaces cannot be == " + Namespaces.ACCUMULO_NAMESPACE;
      log.warn(why);
      throw new ThriftTableOperationException(null, namespace, operation, TableOperationExceptionType.OTHER, why);
    }
  }

  private void checkTableName(String tableName, TableOperation operation) throws ThriftTableOperationException {
    if (!tableName.matches(Constants.VALID_TABLE_NAME_REGEX)) {
      String why = "Table names must only contain word characters (letters, digits, and underscores): " + tableName;
      log.warn(why);
      throw new ThriftTableOperationException(null, tableName, operation, TableOperationExceptionType.OTHER, why);
    }
    if (Tables.getNameToIdMap(master.getInstance()).containsKey(tableName)) {
      String why = "Table name already exists: " + tableName;
      throw new ThriftTableOperationException(null, tableName, operation, TableOperationExceptionType.EXISTS, why);
    }
  }

  private void checkNamespaceName(String namespace, TableOperation operation) throws ThriftTableOperationException {
    if (!namespace.matches(Constants.VALID_NAMESPACE_REGEX)) {
      String why = "Namespaces must only contain word characters (letters, digits, and underscores): " + namespace;
      log.warn(why);
      throw new ThriftTableOperationException(null, namespace, operation, TableOperationExceptionType.INVALID_NAME, why);
    }
    if (Namespaces.getNameToIdMap(master.getInstance()).containsKey(namespace)) {
      String why = "Namespace already exists: " + namespace;
      throw new ThriftTableOperationException(null, namespace, operation, TableOperationExceptionType.NAMESPACE_EXISTS, why);
    }
  }

  protected String checkNamespaceId(String namespace, TableOperation operation) throws ThriftTableOperationException {
    final String namespaceId = Namespaces.getNameToIdMap(master.getInstance()).get(namespace);
    if (namespaceId == null)
      throw new ThriftTableOperationException(null, namespace, operation, TableOperationExceptionType.NAMESPACE_NOTFOUND, null);
    return namespaceId;
  }

}
