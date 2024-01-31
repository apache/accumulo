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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.util.Validators.CAN_CLONE_TABLE;
import static org.apache.accumulo.core.util.Validators.EXISTING_NAMESPACE_NAME;
import static org.apache.accumulo.core.util.Validators.EXISTING_TABLE_NAME;
import static org.apache.accumulo.core.util.Validators.NEW_NAMESPACE_NAME;
import static org.apache.accumulo.core.util.Validators.NEW_TABLE_NAME;
import static org.apache.accumulo.core.util.Validators.NOT_BUILTIN_NAMESPACE;
import static org.apache.accumulo.core.util.Validators.NOT_BUILTIN_TABLE;
import static org.apache.accumulo.core.util.Validators.NOT_METADATA_TABLE;
import static org.apache.accumulo.core.util.Validators.NOT_METADATA_TABLE_ID;
import static org.apache.accumulo.core.util.Validators.NOT_ROOT_TABLE_ID;
import static org.apache.accumulo.core.util.Validators.VALID_TABLE_ID;
import static org.apache.accumulo.core.util.Validators.sameNamespaceAs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.InitialTableState;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.clientImpl.UserCompactionUtils;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.manager.thrift.BulkImportState;
import org.apache.accumulo.core.manager.thrift.FateOperation;
import org.apache.accumulo.core.manager.thrift.FateService;
import org.apache.accumulo.core.manager.thrift.TFateId;
import org.apache.accumulo.core.manager.thrift.TFateInstanceType;
import org.apache.accumulo.core.manager.thrift.ThriftPropertyException;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.FastFormat;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.Validator;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.manager.tableOps.ChangeTableState;
import org.apache.accumulo.manager.tableOps.TraceRepo;
import org.apache.accumulo.manager.tableOps.availability.SetTabletAvailability;
import org.apache.accumulo.manager.tableOps.bulkVer2.PrepBulkImport;
import org.apache.accumulo.manager.tableOps.clone.CloneTable;
import org.apache.accumulo.manager.tableOps.compact.CompactRange;
import org.apache.accumulo.manager.tableOps.compact.cancel.CancelCompactions;
import org.apache.accumulo.manager.tableOps.create.CreateTable;
import org.apache.accumulo.manager.tableOps.delete.PreDeleteTable;
import org.apache.accumulo.manager.tableOps.merge.MergeInfo;
import org.apache.accumulo.manager.tableOps.merge.TableRangeOp;
import org.apache.accumulo.manager.tableOps.namespace.create.CreateNamespace;
import org.apache.accumulo.manager.tableOps.namespace.delete.DeleteNamespace;
import org.apache.accumulo.manager.tableOps.namespace.rename.RenameNamespace;
import org.apache.accumulo.manager.tableOps.rename.RenameTable;
import org.apache.accumulo.manager.tableOps.split.PreSplit;
import org.apache.accumulo.manager.tableOps.tableExport.ExportTable;
import org.apache.accumulo.manager.tableOps.tableImport.ImportTable;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.slf4j.Logger;

class FateServiceHandler implements FateService.Iface {

  private final Manager manager;
  protected static final Logger log = Manager.log;

  public FateServiceHandler(Manager manager) {
    this.manager = manager;
  }

  @Override
  public TFateId beginFateOperation(TInfo tinfo, TCredentials credentials, TFateInstanceType type)
      throws ThriftSecurityException {
    authenticate(credentials);
    return new TFateId(type, manager.fate(FateInstanceType.fromThrift(type)).startTransaction());
  }

  @Override
  public void executeFateOperation(TInfo tinfo, TCredentials c, TFateId opid, FateOperation op,
      List<ByteBuffer> arguments, Map<String,String> options, boolean autoCleanup)
      throws ThriftSecurityException, ThriftTableOperationException, ThriftPropertyException {
    authenticate(c);
    String goalMessage = op.toString() + " ";
    long tid = opid.getTid();
    FateInstanceType type = FateInstanceType.fromThrift(opid.getType());

    switch (op) {
      case NAMESPACE_CREATE: {
        TableOperation tableOp = TableOperation.CREATE;
        validateArgumentCount(arguments, tableOp, 1);
        String namespace = validateName(arguments.get(0), tableOp, NEW_NAMESPACE_NAME);

        if (!manager.security.canCreateNamespace(c)) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        goalMessage += "Create " + namespace + " namespace.";
        manager.fate(type).seedTransaction(op.toString(), tid,
            new TraceRepo<>(new CreateNamespace(c.getPrincipal(), namespace, options)), autoCleanup,
            goalMessage);
        break;
      }
      case NAMESPACE_RENAME: {
        TableOperation tableOp = TableOperation.RENAME;
        validateArgumentCount(arguments, tableOp, 2);
        String oldName = validateName(arguments.get(0), tableOp,
            EXISTING_NAMESPACE_NAME.and(NOT_BUILTIN_NAMESPACE));
        String newName = validateName(arguments.get(1), tableOp, NEW_NAMESPACE_NAME);

        NamespaceId namespaceId =
            ClientServiceHandler.checkNamespaceId(manager.getContext(), oldName, tableOp);
        if (!manager.security.canRenameNamespace(c, namespaceId)) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        goalMessage += "Rename " + oldName + " namespace to " + newName;
        manager.fate(type).seedTransaction(op.toString(), tid,
            new TraceRepo<>(new RenameNamespace(namespaceId, oldName, newName)), autoCleanup,
            goalMessage);
        break;
      }
      case NAMESPACE_DELETE: {
        TableOperation tableOp = TableOperation.DELETE;
        validateArgumentCount(arguments, tableOp, 1);
        String namespace = validateName(arguments.get(0), tableOp,
            EXISTING_NAMESPACE_NAME.and(NOT_BUILTIN_NAMESPACE));

        NamespaceId namespaceId =
            ClientServiceHandler.checkNamespaceId(manager.getContext(), namespace, tableOp);
        if (!manager.security.canDeleteNamespace(c, namespaceId)) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        goalMessage += "Delete namespace Id: " + namespaceId;
        manager.fate(type).seedTransaction(op.toString(), tid,
            new TraceRepo<>(new DeleteNamespace(namespaceId)), autoCleanup, goalMessage);
        break;
      }
      case TABLE_CREATE: {
        TableOperation tableOp = TableOperation.CREATE;
        int SPLIT_OFFSET = 5; // offset where split data begins in arguments list
        if (arguments.size() < SPLIT_OFFSET) {
          throw new ThriftTableOperationException(null, null, tableOp,
              TableOperationExceptionType.OTHER,
              "Expected at least " + SPLIT_OFFSET + " arguments, saw :" + arguments.size());
        }
        String tableName =
            validateName(arguments.get(0), tableOp, NEW_TABLE_NAME.and(NOT_BUILTIN_TABLE));
        TimeType timeType = TimeType.valueOf(ByteBufferUtil.toString(arguments.get(1)));
        InitialTableState initialTableState =
            InitialTableState.valueOf(ByteBufferUtil.toString(arguments.get(2)));
        TabletAvailability initialTabletAvailability =
            TabletAvailability.valueOf(ByteBufferUtil.toString(arguments.get(3)));
        int splitCount = Integer.parseInt(ByteBufferUtil.toString(arguments.get(4)));
        validateArgumentCount(arguments, tableOp, SPLIT_OFFSET + splitCount);
        Path splitsPath = null;
        Path splitsDirsPath = null;
        if (splitCount > 0) {
          try {
            Path tmpDir = mkTempDir(opid);
            splitsPath = new Path(tmpDir, "splits");
            splitsDirsPath = new Path(tmpDir, "splitsDirs");
            writeSplitsToFile(splitsPath, arguments, splitCount, SPLIT_OFFSET);
          } catch (IOException e) {
            throw new ThriftTableOperationException(null, tableName, tableOp,
                TableOperationExceptionType.OTHER,
                "Exception thrown while writing splits to file system");
          }
        }
        NamespaceId namespaceId;

        try {
          namespaceId = Namespaces.getNamespaceId(manager.getContext(),
              TableNameUtil.qualify(tableName).getFirst());
        } catch (NamespaceNotFoundException e) {
          throw new ThriftTableOperationException(null, tableName, tableOp,
              TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }

        if (!manager.security.canCreateTable(c, tableName, namespaceId)) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        for (Map.Entry<String,String> entry : options.entrySet()) {
          if (!Property.isValidProperty(entry.getKey(), entry.getValue())) {
            String errorMessage = "Property or value not valid ";
            if (!Property.isValidTablePropertyKey(entry.getKey())) {
              errorMessage = "Invalid Table Property ";
            }
            throw new ThriftPropertyException(entry.getKey(), entry.getValue(),
                errorMessage + entry.getKey() + "=" + entry.getValue());
          }
        }

        goalMessage += "Create table " + tableName + " " + initialTableState + " with " + splitCount
            + " splits and initial tabletAvailability of " + initialTabletAvailability;

        manager.fate(type).seedTransaction(op.toString(), tid,
            new TraceRepo<>(new CreateTable(c.getPrincipal(), tableName, timeType, options,
                splitsPath, splitCount, splitsDirsPath, initialTableState,
                initialTabletAvailability, namespaceId)),
            autoCleanup, goalMessage);

        break;
      }
      case TABLE_RENAME: {
        TableOperation tableOp = TableOperation.RENAME;
        validateArgumentCount(arguments, tableOp, 2);
        String oldTableName =
            validateName(arguments.get(0), tableOp, EXISTING_TABLE_NAME.and(NOT_BUILTIN_TABLE));
        String newTableName = validateName(arguments.get(1), tableOp,
            NEW_TABLE_NAME.and(sameNamespaceAs(oldTableName)));

        TableId tableId =
            ClientServiceHandler.checkTableId(manager.getContext(), oldTableName, tableOp);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canRename;
        try {
          canRename =
              manager.security.canRenameTable(c, tableId, oldTableName, newTableName, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, oldTableName, TableOperation.RENAME);
          throw e;
        }

        if (!canRename) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        goalMessage += "Rename table " + oldTableName + "(" + tableId + ") to " + oldTableName;

        try {
          manager.fate(type).seedTransaction(op.toString(), tid,
              new TraceRepo<>(new RenameTable(namespaceId, tableId, oldTableName, newTableName)),
              autoCleanup, goalMessage);
        } catch (NamespaceNotFoundException e) {
          throw new ThriftTableOperationException(null, oldTableName, tableOp,
              TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }

        break;
      }
      case TABLE_CLONE: {
        TableOperation tableOp = TableOperation.CLONE;
        validateArgumentCount(arguments, tableOp, 3);
        TableId srcTableId = validateTableIdArgument(arguments.get(0), tableOp, CAN_CLONE_TABLE);
        String tableName =
            validateName(arguments.get(1), tableOp, NEW_TABLE_NAME.and(NOT_BUILTIN_TABLE));
        boolean keepOffline = false;
        if (arguments.get(2) != null) {
          keepOffline = Boolean.parseBoolean(ByteBufferUtil.toString(arguments.get(2)));
        }

        NamespaceId namespaceId;
        try {
          namespaceId = Namespaces.getNamespaceId(manager.getContext(),
              TableNameUtil.qualify(tableName).getFirst());
        } catch (NamespaceNotFoundException e) {
          // shouldn't happen, but possible once cloning between namespaces is supported
          throw new ThriftTableOperationException(null, tableName, tableOp,
              TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }

        final boolean canCloneTable;
        try {
          canCloneTable =
              manager.security.canCloneTable(c, srcTableId, tableName, namespaceId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, srcTableId, null, TableOperation.CLONE);
          throw e;
        }

        if (!canCloneTable) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        Map<String,String> propertiesToSet = new HashMap<>();
        Set<String> propertiesToExclude = new HashSet<>();

        for (Entry<String,String> entry : options.entrySet()) {
          if (entry.getKey().startsWith(TableOperationsImpl.PROPERTY_EXCLUDE_PREFIX)) {
            propertiesToExclude.add(
                entry.getKey().substring(TableOperationsImpl.PROPERTY_EXCLUDE_PREFIX.length()));
            continue;
          }

          if (!Property.isValidProperty(entry.getKey(), entry.getValue())) {
            String errorMessage = "Property or value not valid ";
            if (!Property.isValidTablePropertyKey(entry.getKey())) {
              errorMessage = "Invalid Table Property ";
            }
            throw new ThriftPropertyException(entry.getKey(), entry.getValue(),
                errorMessage + entry.getKey() + "=" + entry.getValue());
          }

          propertiesToSet.put(entry.getKey(), entry.getValue());
        }

        goalMessage += "Clone table " + srcTableId + " to " + tableName;
        if (keepOffline) {
          goalMessage += " and keep offline.";
        }

        manager.fate(type).seedTransaction(
            op.toString(), tid, new TraceRepo<>(new CloneTable(c.getPrincipal(), namespaceId,
                srcTableId, tableName, propertiesToSet, propertiesToExclude, keepOffline)),
            autoCleanup, goalMessage);

        break;
      }
      case TABLE_DELETE: {
        TableOperation tableOp = TableOperation.DELETE;
        validateArgumentCount(arguments, tableOp, 1);
        String tableName =
            validateName(arguments.get(0), tableOp, EXISTING_TABLE_NAME.and(NOT_BUILTIN_TABLE));

        final TableId tableId =
            ClientServiceHandler.checkTableId(manager.getContext(), tableName, tableOp);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canDeleteTable;
        try {
          canDeleteTable = manager.security.canDeleteTable(c, tableId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName, TableOperation.DELETE);
          throw e;
        }

        if (!canDeleteTable) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        goalMessage += "Delete table " + tableName + "(" + tableId + ")";
        manager.fate(type).seedTransaction(op.toString(), tid,
            new TraceRepo<>(new PreDeleteTable(namespaceId, tableId)), autoCleanup, goalMessage);
        break;
      }
      case TABLE_ONLINE: {
        TableOperation tableOp = TableOperation.ONLINE;
        validateArgumentCount(arguments, tableOp, 1);
        final var tableId = validateTableIdArgument(arguments.get(0), tableOp, NOT_ROOT_TABLE_ID);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canOnlineOfflineTable;
        try {
          canOnlineOfflineTable = manager.security.canChangeTableState(c, tableId, op, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, null, TableOperation.ONLINE);
          throw e;
        }

        if (!canOnlineOfflineTable) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        goalMessage += "Online table " + tableId;
        final EnumSet<TableState> expectedCurrStates =
            EnumSet.of(TableState.ONLINE, TableState.OFFLINE);
        manager.fate(type).seedTransaction(op.toString(), tid,
            new TraceRepo<>(
                new ChangeTableState(namespaceId, tableId, tableOp, expectedCurrStates)),
            autoCleanup, goalMessage);
        break;
      }
      case TABLE_OFFLINE: {
        TableOperation tableOp = TableOperation.OFFLINE;
        validateArgumentCount(arguments, tableOp, 1);
        final var tableId = validateTableIdArgument(arguments.get(0), tableOp,
            NOT_ROOT_TABLE_ID.and(NOT_METADATA_TABLE_ID));
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canOnlineOfflineTable;
        try {
          canOnlineOfflineTable = manager.security.canChangeTableState(c, tableId, op, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, null, TableOperation.OFFLINE);
          throw e;
        }

        if (!canOnlineOfflineTable) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        goalMessage += "Offline table " + tableId;
        final EnumSet<TableState> expectedCurrStates =
            EnumSet.of(TableState.ONLINE, TableState.OFFLINE);
        manager.fate(type).seedTransaction(op.toString(), tid,
            new TraceRepo<>(
                new ChangeTableState(namespaceId, tableId, tableOp, expectedCurrStates)),
            autoCleanup, goalMessage);
        break;
      }
      case TABLE_MERGE: {
        TableOperation tableOp = TableOperation.MERGE;
        validateArgumentCount(arguments, tableOp, 3);
        String tableName = validateName(arguments.get(0), tableOp, EXISTING_TABLE_NAME);
        Text startRow = ByteBufferUtil.toText(arguments.get(1));
        Text endRow = ByteBufferUtil.toText(arguments.get(2));

        final TableId tableId =
            ClientServiceHandler.checkTableId(manager.getContext(), tableName, tableOp);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canMerge;
        try {
          canMerge = manager.security.canMerge(c, tableId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName, TableOperation.MERGE);
          throw e;
        }

        if (!canMerge) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        String startRowStr = StringUtils.defaultIfBlank(startRow.toString(), "-inf");
        String endRowStr = StringUtils.defaultIfBlank(startRow.toString(), "+inf");

        Manager.log.debug("Creating merge op: {} from startRow: {} to endRow: {}", tableId,
            startRowStr, endRowStr);
        goalMessage += "Merge table " + tableName + "(" + tableId + ") splits from " + startRowStr
            + " to " + endRowStr;
        manager.fate(type).seedTransaction(op.toString(), tid, new TraceRepo<>(
            new TableRangeOp(MergeInfo.Operation.MERGE, namespaceId, tableId, startRow, endRow)),
            autoCleanup, goalMessage);
        break;
      }
      case TABLE_DELETE_RANGE: {
        TableOperation tableOp = TableOperation.DELETE_RANGE;
        validateArgumentCount(arguments, tableOp, 3);
        String tableName =
            validateName(arguments.get(0), tableOp, EXISTING_TABLE_NAME.and(NOT_METADATA_TABLE));
        Text startRow = ByteBufferUtil.toText(arguments.get(1));
        Text endRow = ByteBufferUtil.toText(arguments.get(2));

        final TableId tableId =
            ClientServiceHandler.checkTableId(manager.getContext(), tableName, tableOp);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canDeleteRange;
        try {
          canDeleteRange =
              manager.security.canDeleteRange(c, tableId, tableName, startRow, endRow, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName, TableOperation.DELETE_RANGE);
          throw e;
        }

        if (!canDeleteRange) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        goalMessage +=
            "Delete table " + tableName + "(" + tableId + ") range " + startRow + " to " + endRow;
        manager.fate(type).seedTransaction(op.toString(), tid, new TraceRepo<>(
            new TableRangeOp(MergeInfo.Operation.DELETE, namespaceId, tableId, startRow, endRow)),
            autoCleanup, goalMessage);
        break;
      }
      case TABLE_COMPACT: {
        TableOperation tableOp = TableOperation.COMPACT;
        validateArgumentCount(arguments, tableOp, 2);
        TableId tableId = validateTableIdArgument(arguments.get(0), tableOp, null);
        CompactionConfig compactionConfig =
            UserCompactionUtils.decodeCompactionConfig(ByteBufferUtil.toBytes(arguments.get(1)));
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canCompact;
        try {
          canCompact = manager.security.canCompact(c, tableId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, null, TableOperation.COMPACT);
          throw e;
        }

        if (!canCompact) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        goalMessage += "Compact table (" + tableId + ") with config " + compactionConfig;
        manager.fate(type).seedTransaction(op.toString(), tid,
            new TraceRepo<>(new CompactRange(namespaceId, tableId, compactionConfig)), autoCleanup,
            goalMessage);
        break;
      }
      case TABLE_CANCEL_COMPACT: {
        TableOperation tableOp = TableOperation.COMPACT_CANCEL;
        validateArgumentCount(arguments, tableOp, 1);
        TableId tableId = validateTableIdArgument(arguments.get(0), tableOp, null);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canCancelCompact;
        try {
          canCancelCompact = manager.security.canCompact(c, tableId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, null, TableOperation.COMPACT_CANCEL);
          throw e;
        }

        if (!canCancelCompact) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        goalMessage += "Cancel compaction of table (" + tableId + ")";
        manager.fate(type).seedTransaction(op.toString(), tid,
            new TraceRepo<>(new CancelCompactions(namespaceId, tableId)), autoCleanup, goalMessage);
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
        String tableName =
            validateName(arguments.get(0), tableOp, NEW_TABLE_NAME.and(NOT_BUILTIN_TABLE));
        boolean keepOffline = Boolean.parseBoolean(ByteBufferUtil.toString(arguments.get(1)));
        boolean keepMappings = Boolean.parseBoolean(ByteBufferUtil.toString(arguments.get(2)));

        List<ByteBuffer> exportDirArgs = arguments.stream().skip(3).collect(Collectors.toList());
        Set<String> exportDirs = ByteBufferUtil.toStringSet(exportDirArgs);
        NamespaceId namespaceId;
        try {
          namespaceId = Namespaces.getNamespaceId(manager.getContext(),
              TableNameUtil.qualify(tableName).getFirst());
        } catch (NamespaceNotFoundException e) {
          throw new ThriftTableOperationException(null, tableName, tableOp,
              TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }

        final boolean canImport;
        try {
          canImport = manager.security.canImport(c, tableName, exportDirs, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, null, tableName, TableOperation.IMPORT);
          throw e;
        }

        if (!canImport) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        goalMessage += "Import table with new name: " + tableName + " from " + exportDirs;
        manager.fate(type)
            .seedTransaction(op.toString(), tid, new TraceRepo<>(new ImportTable(c.getPrincipal(),
                tableName, exportDirs, namespaceId, keepMappings, keepOffline)), autoCleanup,
                goalMessage);
        break;
      }
      case TABLE_EXPORT: {
        TableOperation tableOp = TableOperation.EXPORT;
        validateArgumentCount(arguments, tableOp, 2);
        String tableName =
            validateName(arguments.get(0), tableOp, EXISTING_TABLE_NAME.and(NOT_BUILTIN_TABLE));
        String exportDir = ByteBufferUtil.toString(arguments.get(1));

        TableId tableId =
            ClientServiceHandler.checkTableId(manager.getContext(), tableName, tableOp);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canExport;
        try {
          canExport = manager.security.canExport(c, tableId, tableName, exportDir, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName, TableOperation.EXPORT);
          throw e;
        }

        if (!canExport) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        goalMessage += "Export table " + tableName + "(" + tableId + ") to " + exportDir;
        manager.fate(type).seedTransaction(op.toString(), tid,
            new TraceRepo<>(new ExportTable(namespaceId, tableName, tableId, exportDir)),
            autoCleanup, goalMessage);
        break;
      }
      case TABLE_BULK_IMPORT2: {
        TableOperation tableOp = TableOperation.BULK_IMPORT;
        validateArgumentCount(arguments, tableOp, 3);
        final var tableId = validateTableIdArgument(arguments.get(0), tableOp, NOT_ROOT_TABLE_ID);
        String dir = ByteBufferUtil.toString(arguments.get(1));

        boolean setTime = Boolean.parseBoolean(ByteBufferUtil.toString(arguments.get(2)));

        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canBulkImport;
        String tableName;
        try {
          tableName = manager.getContext().getTableName(tableId);
          canBulkImport =
              manager.security.canBulkImport(c, tableId, tableName, dir, null, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, "", TableOperation.BULK_IMPORT);
          throw e;
        } catch (TableNotFoundException e) {
          throw new ThriftTableOperationException(tableId.canonical(), null,
              TableOperation.BULK_IMPORT, TableOperationExceptionType.NOTFOUND,
              "Table no longer exists");
        }

        if (!canBulkImport) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        manager.updateBulkImportStatus(dir, BulkImportState.INITIAL);

        goalMessage += "Bulk import (v2)  " + dir + " to " + tableName + "(" + tableId + ")";
        manager.fate(type).seedTransaction(op.toString(), tid,
            new TraceRepo<>(new PrepBulkImport(tableId, dir, setTime)), autoCleanup, goalMessage);
        break;
      }
      case TABLE_TABLET_AVAILABILITY: {
        TableOperation tableOp = TableOperation.SET_TABLET_AVAILABILITY;
        validateArgumentCount(arguments, tableOp, 3);
        String tableName = validateName(arguments.get(0), tableOp, NOT_METADATA_TABLE);
        TableId tableId = null;
        try {
          tableId = manager.getContext().getTableId(tableName);
        } catch (TableNotFoundException e) {
          throw new ThriftTableOperationException(null, tableName,
              TableOperation.SET_TABLET_AVAILABILITY, TableOperationExceptionType.NOTFOUND,
              "Table no longer exists");
        }
        final NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canSetAvailability;
        try {
          canSetAvailability = manager.security.canAlterTable(c, tableId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName,
              TableOperation.SET_TABLET_AVAILABILITY);
          throw e;
        }
        if (!canSetAvailability) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        TRange tRange = new TRange();
        try {
          new TDeserializer().deserialize(tRange, ByteBufferUtil.toBytes(arguments.get(1)));
        } catch (TException e) {
          throw new ThriftTableOperationException(tableId.canonical(), tableName,
              TableOperation.SET_TABLET_AVAILABILITY, TableOperationExceptionType.BAD_RANGE,
              e.getMessage());
        }
        TabletAvailability tabletAvailability =
            TabletAvailability.valueOf(ByteBufferUtil.toString(arguments.get(2)));

        goalMessage += "Set availability for table: " + tableName + "(" + tableId + ") range: "
            + tRange + " to: " + tabletAvailability.name();
        manager.fate(type).seedTransaction(op.toString(), tid,
            new TraceRepo<>(
                new SetTabletAvailability(tableId, namespaceId, tRange, tabletAvailability)),
            autoCleanup, goalMessage);
        break;
      }
      case TABLE_SPLIT: {
        TableOperation tableOp = TableOperation.SPLIT;

        // ELASTICITY_TODO this does not check if table is offline for now, that is usually done in
        // FATE operation with a table lock. Deferring that check for now as its possible tablet
        // locks may not be needed.

        int SPLIT_OFFSET = 3; // offset where split data begins in arguments list
        if (arguments.size() < (SPLIT_OFFSET + 1)) {
          throw new ThriftTableOperationException(null, null, tableOp,
              TableOperationExceptionType.OTHER,
              "Expected at least " + (SPLIT_OFFSET + 1) + " arguments, saw :" + arguments.size());
        }

        var tableId = validateTableIdArgument(arguments.get(0), tableOp, NOT_ROOT_TABLE_ID);
        NamespaceId namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        boolean canSplit;

        try {
          canSplit = manager.security.canSplitTablet(c, tableId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, null, TableOperation.SPLIT);
          throw e;
        }

        if (!canSplit) {
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }

        var endRow = ByteBufferUtil.toText(arguments.get(1));
        var prevEndRow = ByteBufferUtil.toText(arguments.get(2));

        endRow = endRow.getLength() == 0 ? null : endRow;
        prevEndRow = prevEndRow.getLength() == 0 ? null : prevEndRow;

        // ELASTICITY_TODO create table stores splits in a file, maybe this operation should do the
        // same
        SortedSet<Text> splits = arguments.subList(SPLIT_OFFSET, arguments.size()).stream()
            .map(ByteBufferUtil::toText).collect(Collectors.toCollection(TreeSet::new));

        KeyExtent extent = new KeyExtent(tableId, endRow, prevEndRow);

        Predicate<Text> outOfBoundsTest =
            split -> !extent.contains(split) || split.equals(extent.endRow());

        if (splits.stream().anyMatch(outOfBoundsTest)) {
          splits.stream().filter(outOfBoundsTest).forEach(split -> log
              .warn("split for {} is out of bounds : {}", extent, TextUtil.truncate(split)));

          throw new ThriftTableOperationException(tableId.canonical(), null, tableOp,
              TableOperationExceptionType.OTHER,
              "Split is outside bounds of tablet or equal to the tablets endrow, see warning in logs for more information.");
        }

        var maxSplitSize = manager.getContext().getTableConfiguration(tableId)
            .getAsBytes(Property.TABLE_MAX_END_ROW_SIZE);

        Predicate<Text> oversizedTest = split -> split.getLength() > maxSplitSize;

        if (splits.stream().anyMatch(oversizedTest)) {
          splits.stream().filter(oversizedTest)
              .forEach(split -> log.warn(
                  "split exceeds max configured split size len:{}  max:{} extent:{} split:{}",
                  split.getLength(), maxSplitSize, extent, TextUtil.truncate(split)));

          throw new ThriftTableOperationException(tableId.canonical(), null, tableOp,
              TableOperationExceptionType.OTHER,
              "Length of requested split exceeds tables configured max, see warning in logs for more information.");
        }

        goalMessage = "Splitting " + extent + " for user into " + (splits.size() + 1) + " tablets";
        manager.fate(type).seedTransaction(op.toString(), tid, new PreSplit(extent, splits),
            autoCleanup, goalMessage);
        break;
      }
      default:
        throw new UnsupportedOperationException();
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

  /**
   * Inspects the {@link ThriftSecurityException} and throws a {@link ThriftTableOperationException}
   * if the {@link SecurityErrorCode} on the {@link ThriftSecurityException} was
   * {code}TABLE_DOESNT_EXIST{code}. If the {@link ThriftSecurityException} is thrown because a
   * table doesn't exist anymore, clients will likely see an {@link AccumuloSecurityException}
   * instead of a {@link TableNotFoundException} as expected. If the {@link ThriftSecurityException}
   * has a different {@link SecurityErrorCode}, this method does nothing and expects the caller to
   * properly handle the original exception.
   *
   * @param e A caught ThriftSecurityException
   * @param tableId Table ID being operated on, or null
   * @param tableName Table name being operated on, or null
   * @param op The TableOperation the Manager was attempting to perform
   * @throws ThriftTableOperationException Thrown if {@code e} was thrown because
   *         {@link SecurityErrorCode#TABLE_DOESNT_EXIST}
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
  public String waitForFateOperation(TInfo tinfo, TCredentials credentials, TFateId opid)
      throws ThriftSecurityException, ThriftTableOperationException {
    authenticate(credentials);

    FateInstanceType type = FateInstanceType.fromThrift(opid.getType());
    TStatus status = manager.fate(type).waitForCompletion(opid.getTid());
    if (status == TStatus.FAILED) {
      Exception e = manager.fate(type).getException(opid.getTid());
      if (e instanceof ThriftTableOperationException) {
        throw (ThriftTableOperationException) e;
      } else if (e instanceof ThriftSecurityException) {
        throw (ThriftSecurityException) e;
      } else if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(e);
      }
    }

    String ret = manager.fate(type).getReturn(opid.getTid());
    if (ret == null) {
      ret = ""; // thrift does not like returning null
    }
    return ret;
  }

  @Override
  public void finishFateOperation(TInfo tinfo, TCredentials credentials, TFateId opid)
      throws ThriftSecurityException {
    authenticate(credentials);
    manager.fate(FateInstanceType.fromThrift(opid.getType())).delete(opid.getTid());
  }

  protected void authenticate(TCredentials credentials) throws ThriftSecurityException {
    // this is a bit redundant, the credentials of the caller (the first arg) will throw an
    // exception if it fails to authenticate
    // before the second arg is checked (which would return true or false)
    if (!manager.security.authenticateUser(credentials, credentials)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.BAD_CREDENTIALS);
    }
  }

  // Verify table name arguments are valid, and match any additional restrictions
  private TableId validateTableIdArgument(ByteBuffer tableIdArg, TableOperation op,
      Validator<TableId> userValidator) throws ThriftTableOperationException {
    TableId tableId = tableIdArg == null ? null : ByteBufferUtil.toTableId(tableIdArg);
    try {
      return VALID_TABLE_ID.and(userValidator).validate(tableId);
    } catch (IllegalArgumentException e) {
      String why = e.getMessage();
      // Information provided by a client should generate a user-level exception, not a system-level
      // warning.
      log.debug(why);
      throw new ThriftTableOperationException((tableId == null ? "null" : tableId.canonical()),
          null, op, TableOperationExceptionType.INVALID_NAME, why);
    }
  }

  private void validateArgumentCount(List<ByteBuffer> arguments, TableOperation op, int expected)
      throws ThriftTableOperationException {
    if (arguments.size() != expected) {
      throw new ThriftTableOperationException(null, null, op, TableOperationExceptionType.OTHER,
          "Unexpected number of arguments : " + expected + " != " + arguments.size());
    }
  }

  // Verify namespace or table name arguments are valid, and match any additional restrictions
  private String validateName(ByteBuffer argument, TableOperation op, Validator<String> validator)
      throws ThriftTableOperationException {
    String arg = argument == null ? null : ByteBufferUtil.toString(argument);
    try {
      return validator.validate(arg);
    } catch (IllegalArgumentException e) {
      String why = e.getMessage();
      // Information provided by a client should generate a user-level exception,
      // not a system-level warning, so use debug here.
      log.debug(why);
      throw new ThriftTableOperationException(null, String.valueOf(arg), op,
          TableOperationExceptionType.INVALID_NAME, why);
    }
  }

  /**
   * Create a file on the file system to hold the splits to be created at table creation.
   */
  private void writeSplitsToFile(Path splitsPath, final List<ByteBuffer> arguments,
      final int splitCount, final int splitOffset) throws IOException {
    FileSystem fs = splitsPath.getFileSystem(manager.getContext().getHadoopConf());
    try (FSDataOutputStream stream = fs.create(splitsPath)) {
      // base64 encode because splits can contain binary
      for (int i = splitOffset; i < splitCount + splitOffset; i++) {
        byte[] splitBytes = ByteBufferUtil.toBytes(arguments.get(i));
        String encodedSplit = Base64.getEncoder().encodeToString(splitBytes);
        stream.write((encodedSplit + '\n').getBytes(UTF_8));
      }
    } catch (IOException e) {
      log.error("Error in FateServiceHandler while writing splits to {}: {}", splitsPath,
          e.getMessage());
      throw e;
    }
  }

  /**
   * Creates a temporary directory for the given FaTE operation (deleting any existing, to avoid
   * issues in case of server retry).
   *
   * @return the path of the created directory
   */
  public Path mkTempDir(TFateId opid) throws IOException {
    Volume vol = manager.getVolumeManager().getFirst();
    Path p = vol
        .prefixChild("/tmp/fate-" + opid.getType() + "-" + FastFormat.toHexString(opid.getTid()));
    FileSystem fs = vol.getFileSystem();
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    fs.mkdirs(p);
    return p;
  }

  @Override
  public boolean cancelFateOperation(TInfo tinfo, TCredentials credentials, TFateId opid)
      throws ThriftSecurityException, ThriftNotActiveServiceException {

    if (!manager.security.canPerformSystemActions(credentials)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    return manager.fate(FateInstanceType.fromThrift(opid.getType())).cancel(opid.getTid());
  }
}
