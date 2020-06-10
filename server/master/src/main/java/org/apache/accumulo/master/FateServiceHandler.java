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

import static org.apache.accumulo.core.Constants.MAX_NAMESPACE_LEN;
import static org.apache.accumulo.core.Constants.MAX_TABLE_NAME_LEN;
import static org.apache.accumulo.master.util.TableValidators.CAN_CLONE;
import static org.apache.accumulo.master.util.TableValidators.NOT_METADATA;
import static org.apache.accumulo.master.util.TableValidators.NOT_ROOT_ID;
import static org.apache.accumulo.master.util.TableValidators.NOT_SYSTEM;
import static org.apache.accumulo.master.util.TableValidators.VALID_ID;
import static org.apache.accumulo.master.util.TableValidators.VALID_NAME;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.client.admin.InitialTableState;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.clientImpl.CompactionStrategyConfigUtil;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.master.thrift.FateOperation;
import org.apache.accumulo.core.master.thrift.FateService;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.Validator;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.apache.accumulo.master.tableOps.ChangeTableState;
import org.apache.accumulo.master.tableOps.TraceRepo;
import org.apache.accumulo.master.tableOps.bulkVer2.PrepBulkImport;
import org.apache.accumulo.master.tableOps.clone.CloneTable;
import org.apache.accumulo.master.tableOps.compact.CompactRange;
import org.apache.accumulo.master.tableOps.compact.cancel.CancelCompactions;
import org.apache.accumulo.master.tableOps.create.CreateTable;
import org.apache.accumulo.master.tableOps.delete.DeleteTable;
import org.apache.accumulo.master.tableOps.merge.TableRangeOp;
import org.apache.accumulo.master.tableOps.namespace.create.CreateNamespace;
import org.apache.accumulo.master.tableOps.namespace.delete.DeleteNamespace;
import org.apache.accumulo.master.tableOps.namespace.rename.RenameNamespace;
import org.apache.accumulo.master.tableOps.rename.RenameTable;
import org.apache.accumulo.master.tableOps.tableExport.ExportTable;
import org.apache.accumulo.master.tableOps.tableImport.ImportTable;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;

class FateServiceHandler implements FateService.Iface {

  protected final Master master;
  protected static final Logger log = Master.log;

  public FateServiceHandler(Master master) {
    this.master = master;
  }

  @Override
  public long beginFateOperation(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    authenticate(credentials);
    return master.fate.startTransaction();
  }

  @Override
  public void executeFateOperation(TInfo tinfo, TCredentials c, long opid, FateOperation op,
      List<ByteBuffer> arguments, Map<String,String> options, boolean autoCleanup)
      throws ThriftSecurityException, ThriftTableOperationException {
    authenticate(c);

    switch (op) {
      case NAMESPACE_CREATE: {
        TableOperation tableOp = TableOperation.CREATE;
        validateArgumentCount(arguments, tableOp, 1);
        String namespace = validateNewNamespaceArgument(arguments.get(0), tableOp, null);

        if (!master.security.canCreateNamespace(c))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid,
            new TraceRepo<>(new CreateNamespace(c.getPrincipal(), namespace, options)),
            autoCleanup);
        break;
      }
      case NAMESPACE_RENAME: {
        TableOperation tableOp = TableOperation.RENAME;
        validateArgumentCount(arguments, tableOp, 2);
        String oldName = validateNamespaceArgument(arguments.get(0), tableOp,
            Namespaces.NOT_DEFAULT.and(Namespaces.NOT_ACCUMULO));
        String newName = validateNewNamespaceArgument(arguments.get(1), tableOp, null);

        NamespaceId namespaceId =
            ClientServiceHandler.checkNamespaceId(master.getContext(), oldName, tableOp);
        if (!master.security.canRenameNamespace(c, namespaceId))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid,
            new TraceRepo<>(new RenameNamespace(namespaceId, oldName, newName)), autoCleanup);
        break;
      }
      case NAMESPACE_DELETE: {
        TableOperation tableOp = TableOperation.DELETE;
        validateArgumentCount(arguments, tableOp, 1);
        String namespace = validateNamespaceArgument(arguments.get(0), tableOp,
            Namespaces.NOT_DEFAULT.and(Namespaces.NOT_ACCUMULO));

        NamespaceId namespaceId =
            ClientServiceHandler.checkNamespaceId(master.getContext(), namespace, tableOp);
        if (!master.security.canDeleteNamespace(c, namespaceId))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<>(new DeleteNamespace(namespaceId)),
            autoCleanup);
        break;
      }
      case TABLE_CREATE: {
        TableOperation tableOp = TableOperation.CREATE;
        int SPLIT_OFFSET = 4; // offset where split data begins in arguments list
        if (arguments.size() < SPLIT_OFFSET) {
          throw new ThriftTableOperationException(null, null, tableOp,
              TableOperationExceptionType.OTHER,
              "Expected at least " + SPLIT_OFFSET + " arguments, saw :" + arguments.size());
        }
        String tableName = validateNewTableNameArgument(arguments.get(0), tableOp, NOT_SYSTEM);
        TimeType timeType = TimeType.valueOf(ByteBufferUtil.toString(arguments.get(1)));
        InitialTableState initialTableState =
            InitialTableState.valueOf(ByteBufferUtil.toString(arguments.get(2)));
        int splitCount = Integer.parseInt(ByteBufferUtil.toString(arguments.get(3)));
        validateArgumentCount(arguments, tableOp, SPLIT_OFFSET + splitCount);
        String splitFile = null;
        String splitDirsFile = null;
        if (splitCount > 0) {
          try {
            splitFile = writeSplitsToFile(opid, arguments, splitCount, SPLIT_OFFSET);
            splitDirsFile = createSplitDirsFile(opid);
          } catch (IOException e) {
            throw new ThriftTableOperationException(null, tableName, tableOp,
                TableOperationExceptionType.OTHER,
                "Exception thrown while writing splits to file system");
          }
        }
        NamespaceId namespaceId;

        try {
          namespaceId =
              Namespaces.getNamespaceId(master.getContext(), Tables.qualify(tableName).getFirst());
        } catch (NamespaceNotFoundException e) {
          throw new ThriftTableOperationException(null, tableName, tableOp,
              TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }

        if (!master.security.canCreateTable(c, tableName, namespaceId))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid,
            new TraceRepo<>(new CreateTable(c.getPrincipal(), tableName, timeType, options,
                splitFile, splitCount, splitDirsFile, initialTableState, namespaceId)),
            autoCleanup);

        break;
      }
      case TABLE_RENAME: {
        TableOperation tableOp = TableOperation.RENAME;
        validateArgumentCount(arguments, tableOp, 2);
        final String oldTableName =
            validateTableNameArgument(arguments.get(0), tableOp, NOT_SYSTEM);
        String newTableName =

            validateNewTableNameArgument(arguments.get(1), tableOp, new Validator<>() {

              @Override
              public boolean test(String argument) {
                // verify they are in the same namespace
                String oldNamespace = Tables.qualify(oldTableName).getFirst();
                return oldNamespace.equals(Tables.qualify(argument).getFirst());
              }

              @Override
              public String invalidMessage(String argument) {
                return "Cannot move tables to a new namespace by renaming. The namespace for "
                    + oldTableName + " does not match " + argument;
              }

            });

        TableId tableId =
            ClientServiceHandler.checkTableId(master.getContext(), oldTableName, tableOp);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canRename;
        try {
          canRename =
              master.security.canRenameTable(c, tableId, oldTableName, newTableName, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, oldTableName, TableOperation.RENAME);
          throw e;
        }

        if (!canRename)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        try {
          master.fate.seedTransaction(opid,
              new TraceRepo<>(new RenameTable(namespaceId, tableId, oldTableName, newTableName)),
              autoCleanup);
        } catch (NamespaceNotFoundException e) {
          throw new ThriftTableOperationException(null, oldTableName, tableOp,
              TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }

        break;
      }
      case TABLE_CLONE: {
        TableOperation tableOp = TableOperation.CLONE;
        validateArgumentCount(arguments, tableOp, 3);
        TableId srcTableId = validateTableIdArgument(arguments.get(0), tableOp, CAN_CLONE);
        String tableName = validateNewTableNameArgument(arguments.get(1), tableOp, NOT_SYSTEM);
        boolean keepOffline = false;
        if (arguments.get(2) != null) {
          keepOffline = Boolean.parseBoolean(ByteBufferUtil.toString(arguments.get(2)));
        }

        NamespaceId namespaceId;
        try {
          namespaceId =
              Namespaces.getNamespaceId(master.getContext(), Tables.qualify(tableName).getFirst());
        } catch (NamespaceNotFoundException e) {
          // shouldn't happen, but possible once cloning between namespaces is supported
          throw new ThriftTableOperationException(null, tableName, tableOp,
              TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }

        final boolean canCloneTable;
        try {
          canCloneTable =
              master.security.canCloneTable(c, srcTableId, tableName, namespaceId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, srcTableId, null, TableOperation.CLONE);
          throw e;
        }

        if (!canCloneTable)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        Map<String,String> propertiesToSet = new HashMap<>();
        Set<String> propertiesToExclude = new HashSet<>();

        for (Entry<String,String> entry : options.entrySet()) {
          if (entry.getKey().startsWith(TableOperationsImpl.CLONE_EXCLUDE_PREFIX)) {
            propertiesToExclude
                .add(entry.getKey().substring(TableOperationsImpl.CLONE_EXCLUDE_PREFIX.length()));
            continue;
          }

          if (!TablePropUtil.isPropertyValid(entry.getKey(), entry.getValue())) {
            throw new ThriftTableOperationException(null, tableName, tableOp,
                TableOperationExceptionType.OTHER,
                "Property or value not valid " + entry.getKey() + "=" + entry.getValue());
          }

          propertiesToSet.put(entry.getKey(), entry.getValue());
        }

        master.fate.seedTransaction(opid, new TraceRepo<>(new CloneTable(c.getPrincipal(),
            namespaceId, srcTableId, tableName, propertiesToSet, propertiesToExclude, keepOffline)),
            autoCleanup);

        break;
      }
      case TABLE_DELETE: {
        TableOperation tableOp = TableOperation.DELETE;
        validateArgumentCount(arguments, tableOp, 1);
        String tableName = validateTableNameArgument(arguments.get(0), tableOp, NOT_SYSTEM);

        final TableId tableId =
            ClientServiceHandler.checkTableId(master.getContext(), tableName, tableOp);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canDeleteTable;
        try {
          canDeleteTable = master.security.canDeleteTable(c, tableId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName, TableOperation.DELETE);
          throw e;
        }

        if (!canDeleteTable)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        master.fate.seedTransaction(opid, new TraceRepo<>(new DeleteTable(namespaceId, tableId)),
            autoCleanup);
        break;
      }
      case TABLE_ONLINE: {
        TableOperation tableOp = TableOperation.ONLINE;
        validateArgumentCount(arguments, tableOp, 1);
        final TableId tableId = validateTableIdArgument(arguments.get(0), tableOp, NOT_ROOT_ID);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canOnlineOfflineTable;
        try {
          canOnlineOfflineTable =
              master.security.canOnlineOfflineTable(c, tableId, op, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, null, TableOperation.ONLINE);
          throw e;
        }

        if (!canOnlineOfflineTable)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid,
            new TraceRepo<>(new ChangeTableState(namespaceId, tableId, tableOp)), autoCleanup);
        break;
      }
      case TABLE_OFFLINE: {
        TableOperation tableOp = TableOperation.OFFLINE;
        validateArgumentCount(arguments, tableOp, 1);
        final TableId tableId = validateTableIdArgument(arguments.get(0), tableOp, NOT_ROOT_ID);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canOnlineOfflineTable;
        try {
          canOnlineOfflineTable =
              master.security.canOnlineOfflineTable(c, tableId, op, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, null, TableOperation.OFFLINE);
          throw e;
        }

        if (!canOnlineOfflineTable)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid,
            new TraceRepo<>(new ChangeTableState(namespaceId, tableId, tableOp)), autoCleanup);
        break;
      }
      case TABLE_MERGE: {
        TableOperation tableOp = TableOperation.MERGE;
        validateArgumentCount(arguments, tableOp, 3);
        String tableName = validateTableNameArgument(arguments.get(0), tableOp, null);
        Text startRow = ByteBufferUtil.toText(arguments.get(1));
        Text endRow = ByteBufferUtil.toText(arguments.get(2));

        final TableId tableId =
            ClientServiceHandler.checkTableId(master.getContext(), tableName, tableOp);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canMerge;
        try {
          canMerge = master.security.canMerge(c, tableId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName, TableOperation.MERGE);
          throw e;
        }

        if (!canMerge)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        Master.log.debug("Creating merge op: {} {} {}", tableId, startRow, endRow);
        master.fate.seedTransaction(opid, new TraceRepo<>(
            new TableRangeOp(MergeInfo.Operation.MERGE, namespaceId, tableId, startRow, endRow)),
            autoCleanup);
        break;
      }
      case TABLE_DELETE_RANGE: {
        TableOperation tableOp = TableOperation.DELETE_RANGE;
        validateArgumentCount(arguments, tableOp, 3);
        String tableName = validateTableNameArgument(arguments.get(0), tableOp, NOT_METADATA);
        Text startRow = ByteBufferUtil.toText(arguments.get(1));
        Text endRow = ByteBufferUtil.toText(arguments.get(2));

        final TableId tableId =
            ClientServiceHandler.checkTableId(master.getContext(), tableName, tableOp);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canDeleteRange;
        try {
          canDeleteRange =
              master.security.canDeleteRange(c, tableId, tableName, startRow, endRow, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName, TableOperation.DELETE_RANGE);
          throw e;
        }

        if (!canDeleteRange)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<>(
            new TableRangeOp(MergeInfo.Operation.DELETE, namespaceId, tableId, startRow, endRow)),
            autoCleanup);
        break;
      }
      case TABLE_BULK_IMPORT: {
        TableOperation tableOp = TableOperation.BULK_IMPORT;
        validateArgumentCount(arguments, tableOp, 4);
        String tableName = validateTableNameArgument(arguments.get(0), tableOp, NOT_SYSTEM);
        String dir = ByteBufferUtil.toString(arguments.get(1));
        String failDir = ByteBufferUtil.toString(arguments.get(2));
        boolean setTime = Boolean.parseBoolean(ByteBufferUtil.toString(arguments.get(3)));

        final TableId tableId =
            ClientServiceHandler.checkTableId(master.getContext(), tableName, tableOp);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canBulkImport;
        try {
          canBulkImport =
              master.security.canBulkImport(c, tableId, tableName, dir, failDir, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName, TableOperation.BULK_IMPORT);
          throw e;
        }

        if (!canBulkImport)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.updateBulkImportStatus(dir, BulkImportState.INITIAL);
        master.fate.seedTransaction(opid,
            new TraceRepo<>(new org.apache.accumulo.master.tableOps.bulkVer1.BulkImport(tableId,
                dir, failDir, setTime)),
            autoCleanup);
        break;
      }
      case TABLE_COMPACT: {
        TableOperation tableOp = TableOperation.COMPACT;
        validateArgumentCount(arguments, tableOp, 5);
        TableId tableId = validateTableIdArgument(arguments.get(0), tableOp, null);
        byte[] startRow = ByteBufferUtil.toBytes(arguments.get(1));
        byte[] endRow = ByteBufferUtil.toBytes(arguments.get(2));
        List<IteratorSetting> iterators =
            SystemIteratorUtil.decodeIteratorSettings(ByteBufferUtil.toBytes(arguments.get(3)));
        CompactionStrategyConfig compactionStrategy =
            CompactionStrategyConfigUtil.decode(ByteBufferUtil.toBytes(arguments.get(4)));
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canCompact;
        try {
          canCompact = master.security.canCompact(c, tableId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, null, TableOperation.COMPACT);
          throw e;
        }

        if (!canCompact)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<>(new CompactRange(namespaceId, tableId,
            startRow, endRow, iterators, compactionStrategy)), autoCleanup);
        break;
      }
      case TABLE_CANCEL_COMPACT: {
        TableOperation tableOp = TableOperation.COMPACT_CANCEL;
        validateArgumentCount(arguments, tableOp, 1);
        TableId tableId = validateTableIdArgument(arguments.get(0), tableOp, null);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canCancelCompact;
        try {
          canCancelCompact = master.security.canCompact(c, tableId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, null, TableOperation.COMPACT_CANCEL);
          throw e;
        }

        if (!canCancelCompact)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid,
            new TraceRepo<>(new CancelCompactions(namespaceId, tableId)), autoCleanup);
        break;
      }
      case TABLE_IMPORT: {
        TableOperation tableOp = TableOperation.IMPORT;
        int IMPORT_DIR_OFFSET = 2; // offset where table list begins
        if (arguments.size() < IMPORT_DIR_OFFSET) {
          throw new ThriftTableOperationException(null, null, tableOp,
              TableOperationExceptionType.OTHER,
              "Expected at least " + IMPORT_DIR_OFFSET + "arguments, sar :" + arguments.size());
        }
        String tableName = validateNewTableNameArgument(arguments.get(0), tableOp, NOT_SYSTEM);
        List<ByteBuffer> exportDirArgs = arguments.stream().skip(1).collect(Collectors.toList());
        Set<String> exportDirs = ByteBufferUtil.toStringSet(exportDirArgs);
        NamespaceId namespaceId;
        try {
          namespaceId =
              Namespaces.getNamespaceId(master.getContext(), Tables.qualify(tableName).getFirst());
        } catch (NamespaceNotFoundException e) {
          throw new ThriftTableOperationException(null, tableName, tableOp,
              TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }

        final boolean canImport;
        try {
          canImport = master.security.canImport(c, tableName, exportDirs, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, null, tableName, TableOperation.IMPORT);
          throw e;
        }

        if (!canImport)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid,
            new TraceRepo<>(new ImportTable(c.getPrincipal(), tableName, exportDirs, namespaceId)),
            autoCleanup);
        break;
      }
      case TABLE_EXPORT: {
        TableOperation tableOp = TableOperation.EXPORT;
        validateArgumentCount(arguments, tableOp, 2);
        String tableName = validateTableNameArgument(arguments.get(0), tableOp, NOT_SYSTEM);
        String exportDir = ByteBufferUtil.toString(arguments.get(1));

        TableId tableId =
            ClientServiceHandler.checkTableId(master.getContext(), tableName, tableOp);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canExport;
        try {
          canExport = master.security.canExport(c, tableId, tableName, exportDir, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName, TableOperation.EXPORT);
          throw e;
        }

        if (!canExport)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid,
            new TraceRepo<>(new ExportTable(namespaceId, tableName, tableId, exportDir)),
            autoCleanup);
        break;
      }
      case TABLE_BULK_IMPORT2:
        TableOperation tableOp = TableOperation.BULK_IMPORT;
        validateArgumentCount(arguments, tableOp, 3);
        TableId tableId = validateTableIdArgument(arguments.get(0), tableOp, NOT_ROOT_ID);
        String dir = ByteBufferUtil.toString(arguments.get(1));

        boolean setTime = Boolean.parseBoolean(ByteBufferUtil.toString(arguments.get(2)));

        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canBulkImport;
        try {
          String tableName = Tables.getTableName(master.getContext(), tableId);
          canBulkImport =
              master.security.canBulkImport(c, tableId, tableName, dir, null, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, "", TableOperation.BULK_IMPORT);
          throw e;
        } catch (TableNotFoundException e) {
          throw new ThriftTableOperationException(tableId.canonical(), null,
              TableOperation.BULK_IMPORT, TableOperationExceptionType.NOTFOUND,
              "Table no longer exists");
        }

        if (!canBulkImport)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid,
            new TraceRepo<>(new PrepBulkImport(tableId, dir, setTime)), autoCleanup);
        break;
      default:
        throw new UnsupportedOperationException();
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

  /**
   * Inspects the {@link ThriftSecurityException} and throws a {@link ThriftTableOperationException}
   * if the {@link SecurityErrorCode} on the {@link ThriftSecurityException} was
   * {code}TABLE_DOESNT_EXIST{code}. If the {@link ThriftSecurityException} is thrown because a
   * table doesn't exist anymore, clients will likely see an {@link AccumuloSecurityException}
   * instead of a {@link TableNotFoundException} as expected. If the {@link ThriftSecurityException}
   * has a different {@link SecurityErrorCode}, this method does nothing and expects the caller to
   * properly handle the original exception.
   *
   * @param e
   *          A caught ThriftSecurityException
   * @param tableId
   *          Table ID being operated on, or null
   * @param tableName
   *          Table name being operated on, or null
   * @param op
   *          The TableOperation the Master was attempting to perform
   * @throws ThriftTableOperationException
   *           Thrown if {@code e} was thrown because {@link SecurityErrorCode#TABLE_DOESNT_EXIST}
   */
  private void throwIfTableMissingSecurityException(ThriftSecurityException e, TableId tableId,
      String tableName, TableOperation op) throws ThriftTableOperationException {
    // ACCUMULO-3135 Table can be deleted after we get table ID but before we can check permission
    if (e.isSetCode() && e.getCode() == SecurityErrorCode.TABLE_DOESNT_EXIST) {
      throw new ThriftTableOperationException(tableId.canonical(), tableName, op,
          TableOperationExceptionType.NOTFOUND, "Table no longer exists");
    }
  }

  @Override
  public String waitForFateOperation(TInfo tinfo, TCredentials credentials, long opid)
      throws ThriftSecurityException, ThriftTableOperationException {
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
  public void finishFateOperation(TInfo tinfo, TCredentials credentials, long opid)
      throws ThriftSecurityException {
    authenticate(credentials);
    master.fate.delete(opid);
  }

  protected void authenticate(TCredentials credentials) throws ThriftSecurityException {
    // this is a bit redundant, the credentials of the caller (the first arg) will throw an
    // exception if it fails to authenticate
    // before the second arg is checked (which would return true or false)
    if (!master.security.authenticateUser(credentials, credentials))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.BAD_CREDENTIALS);
  }

  // Verify table name arguments are valid, and match any additional restrictions
  private TableId validateTableIdArgument(ByteBuffer tableIdArg, TableOperation op,
      Validator<TableId> userValidator) throws ThriftTableOperationException {
    TableId tableId = tableIdArg == null ? null : ByteBufferUtil.toTableId(tableIdArg);
    try {
      return VALID_ID.and(userValidator).validate(tableId);
    } catch (IllegalArgumentException e) {
      String why = e.getMessage();
      // Information provided by a client should generate a user-level exception, not a system-level
      // warning.
      log.debug(why);
      throw new ThriftTableOperationException(tableId.canonical(), null, op,
          TableOperationExceptionType.INVALID_NAME, why);
    }
  }

  // Verify existing table's name argument is valid, and match any additional restrictions
  private String validateTableNameArgument(ByteBuffer tableNameArg, TableOperation op,
      Validator<String> userValidator) throws ThriftTableOperationException {
    String tableName = tableNameArg == null ? null : ByteBufferUtil.toString(tableNameArg);
    if ((tableName != null) && (tableName.length() > MAX_TABLE_NAME_LEN)) {
      log.warn("Table names greater than " + MAX_TABLE_NAME_LEN
          + " characters should be renamed to conform to a " + MAX_TABLE_NAME_LEN
          + " character limit. Longer table names are no longer supported and may result in "
          + " unexpected behavior.");
    }
    return _validateArgument(tableName, op, VALID_NAME.and(userValidator));
  }

  // Verify table name arguments are valid, and match any additional restrictions
  private String validateNewTableNameArgument(ByteBuffer tableNameArg, TableOperation op,
      Validator<String> userValidator) throws ThriftTableOperationException {
    String tableName = tableNameArg == null ? null : ByteBufferUtil.toString(tableNameArg);
    if ((tableName != null) && (tableName.length() > MAX_TABLE_NAME_LEN)) {
      throw new ThriftTableOperationException(null, tableName, op,
          TableOperationExceptionType.INVALID_NAME,
          "Table names must be less than or equal to " + MAX_TABLE_NAME_LEN + " characters. " + "'"
              + tableName + "' is " + tableName.length() + " characters long.");
    }
    return _validateArgument(tableName, op, VALID_NAME.and(userValidator));
  }

  private void validateArgumentCount(List<ByteBuffer> arguments, TableOperation op, int expected)
      throws ThriftTableOperationException {
    if (arguments.size() != expected) {
      throw new ThriftTableOperationException(null, null, op, TableOperationExceptionType.OTHER,
          "Unexpected number of arguments : " + expected + " != " + arguments.size());
    }
  }

  // Verify namespace arguments are valid, and match any additional restrictions
  private String validateNamespaceArgument(ByteBuffer namespaceArg, TableOperation op,
      Validator<String> userValidator) throws ThriftTableOperationException {
    String namespace = namespaceArg == null ? null : ByteBufferUtil.toString(namespaceArg);
    if ((namespace != null) && (namespace.length() > MAX_NAMESPACE_LEN)) {
      log.warn("Namespaces greater than " + MAX_NAMESPACE_LEN
          + " characters should be renamed to conform to a " + MAX_NAMESPACE_LEN
          + " character limit. "
          + "Longer namespaces are no longer supported and may result in unexpected behavior.");
    }
    return _validateArgument(namespace, op, Namespaces.VALID_NAME.and(userValidator));
  }

  // Verify namespace arguments are valid, and match any additional restrictions
  private String validateNewNamespaceArgument(ByteBuffer namespaceArg, TableOperation op,
      Validator<String> userValidator) throws ThriftTableOperationException {
    String namespace = namespaceArg == null ? null : ByteBufferUtil.toString(namespaceArg);
    if ((namespace != null) && (namespace.length() > MAX_NAMESPACE_LEN)) {
      throw new ThriftTableOperationException(null, namespace, op,
          TableOperationExceptionType.INVALID_NAME,
          "Namespaces must be less than or equal to " + MAX_NAMESPACE_LEN + " characters. " + "'"
              + namespace + "' is " + namespace.length() + " characters long.");
    }
    return _validateArgument(namespace, op, Namespaces.VALID_NAME.and(userValidator));
  }

  // helper to handle the exception
  private <T> T _validateArgument(T arg, TableOperation op, Validator<T> validator)
      throws ThriftTableOperationException {
    try {
      return validator.validate(arg);
    } catch (IllegalArgumentException e) {
      String why = e.getMessage();
      // Information provided by a client should generate a user-level exception, not a system-level
      // warning.
      log.debug(why);
      throw new ThriftTableOperationException(null, String.valueOf(arg), op,
          TableOperationExceptionType.INVALID_NAME, why);
    }
  }

  /**
   * Create a file on the file system to hold the splits to be created at table creation.
   */
  private String writeSplitsToFile(final long opid, final List<ByteBuffer> arguments,
      final int splitCount, final int splitOffset) throws IOException {
    String opidStr = String.format("%016x", opid);
    String splitsPath = getSplitPath("/tmp/splits-" + opidStr);
    removeAndCreateTempFile(splitsPath);
    try (FSDataOutputStream stream = master.getOutputStream(splitsPath)) {
      writeSplitsToFileSystem(stream, arguments, splitCount, splitOffset);
    } catch (IOException e) {
      log.error("Error in FateServiceHandler while writing splits for opid: " + opidStr + ": "
          + e.getMessage());
      throw e;
    }
    return splitsPath;
  }

  /**
   * Always check for and delete the splits file if it exists to prevent issues in case of server
   * failure and/or FateServiceHandler retries.
   */
  private void removeAndCreateTempFile(String path) throws IOException {
    FileSystem fs = master.getVolumeManager().getDefaultVolume().getFileSystem();
    if (fs.exists(new Path(path)))
      fs.delete(new Path(path), true);
    fs.create(new Path(path));
  }

  /**
   * Check for and delete the temp file if it exists to prevent issues in case of server failure
   * and/or FateServiceHandler retries. Then create/recreate the file.
   */
  private String createSplitDirsFile(final long opid) throws IOException {
    String opidStr = String.format("%016x", opid);
    String splitDirPath = getSplitPath("/tmp/splitDirs-" + opidStr);
    removeAndCreateTempFile(splitDirPath);
    return splitDirPath;
  }

  /**
   * Write the split values to a tmp directory with unique name. Given that it is not known if the
   * supplied splits will be textual or binary, all splits will be encoded to enable proper handling
   * of binary data.
   */
  private void writeSplitsToFileSystem(final FSDataOutputStream stream,
      final List<ByteBuffer> arguments, final int splitCount, final int splitOffset)
      throws IOException {
    for (int i = splitOffset; i < splitCount + splitOffset; i++) {
      byte[] splitBytes = ByteBufferUtil.toBytes(arguments.get(i));
      String encodedSplit = Base64.getEncoder().encodeToString(splitBytes);
      stream.writeBytes(encodedSplit + '\n');
    }
  }

  /**
   * Get full path to location where initial splits are stored on file system.
   */
  private String getSplitPath(String relPath) {
    Volume defaultVolume = master.getVolumeManager().getDefaultVolume();
    String uri = defaultVolume.getFileSystem().getUri().toString();
    String basePath = defaultVolume.getBasePath();
    return uri + basePath + relPath;
  }
}
