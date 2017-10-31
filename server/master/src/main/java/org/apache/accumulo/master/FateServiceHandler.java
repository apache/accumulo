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

import static org.apache.accumulo.master.util.TableValidators.NOT_METADATA;
import static org.apache.accumulo.master.util.TableValidators.NOT_ROOT_ID;
import static org.apache.accumulo.master.util.TableValidators.NOT_SYSTEM;
import static org.apache.accumulo.master.util.TableValidators.VALID_ID;
import static org.apache.accumulo.master.util.TableValidators.VALID_NAME;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.impl.CompactionStrategyConfigUtil;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.TableOperationsImpl;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.master.thrift.FateOperation;
import org.apache.accumulo.core.master.thrift.FateService;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.Validator;
import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
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
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;

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
        TableOperation tableOp = TableOperation.CREATE;
        String namespace = validateNamespaceArgument(arguments.get(0), tableOp, null);

        if (!master.security.canCreateNamespace(c))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<>(new CreateNamespace(c.getPrincipal(), namespace, options)), autoCleanup);
        break;
      }
      case NAMESPACE_RENAME: {
        TableOperation tableOp = TableOperation.RENAME;
        String oldName = validateNamespaceArgument(arguments.get(0), tableOp, Namespaces.NOT_DEFAULT.and(Namespaces.NOT_ACCUMULO));
        String newName = validateNamespaceArgument(arguments.get(1), tableOp, null);

        Namespace.ID namespaceId = ClientServiceHandler.checkNamespaceId(master.getInstance(), oldName, tableOp);
        if (!master.security.canRenameNamespace(c, namespaceId, oldName, newName))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<>(new RenameNamespace(namespaceId, oldName, newName)), autoCleanup);
        break;
      }
      case NAMESPACE_DELETE: {
        TableOperation tableOp = TableOperation.DELETE;
        String namespace = validateNamespaceArgument(arguments.get(0), tableOp, Namespaces.NOT_DEFAULT.and(Namespaces.NOT_ACCUMULO));

        Namespace.ID namespaceId = ClientServiceHandler.checkNamespaceId(master.getInstance(), namespace, tableOp);
        if (!master.security.canDeleteNamespace(c, namespaceId))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<>(new DeleteNamespace(namespaceId)), autoCleanup);
        break;
      }
      case TABLE_CREATE: {
        TableOperation tableOp = TableOperation.CREATE;
        String tableName = validateTableNameArgument(arguments.get(0), tableOp, NOT_SYSTEM);
        TimeType timeType = TimeType.valueOf(ByteBufferUtil.toString(arguments.get(1)));

        Namespace.ID namespaceId;

        try {
          namespaceId = Namespaces.getNamespaceId(master.getInstance(), Tables.qualify(tableName).getFirst());
        } catch (NamespaceNotFoundException e) {
          throw new ThriftTableOperationException(null, tableName, tableOp, TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }

        if (!master.security.canCreateTable(c, tableName, namespaceId))
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<>(new CreateTable(c.getPrincipal(), tableName, timeType, options, namespaceId)), autoCleanup);

        break;
      }
      case TABLE_RENAME: {
        TableOperation tableOp = TableOperation.RENAME;
        final String oldTableName = validateTableNameArgument(arguments.get(0), tableOp, NOT_SYSTEM);
        String newTableName = validateTableNameArgument(arguments.get(1), tableOp, new Validator<String>() {

          @Override
          public boolean test(String argument) {
            // verify they are in the same namespace
            String oldNamespace = Tables.qualify(oldTableName).getFirst();
            return oldNamespace.equals(Tables.qualify(argument).getFirst());
          }

          @Override
          public String invalidMessage(String argument) {
            return "Cannot move tables to a new namespace by renaming. The namespace for " + oldTableName + " does not match " + argument;
          }

        });

        Table.ID tableId = ClientServiceHandler.checkTableId(master.getInstance(), oldTableName, tableOp);
        Namespace.ID namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canRename;
        try {
          canRename = master.security.canRenameTable(c, tableId, oldTableName, newTableName, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, oldTableName, TableOperation.RENAME);
          throw e;
        }

        if (!canRename)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        try {
          master.fate.seedTransaction(opid, new TraceRepo<>(new RenameTable(namespaceId, tableId, oldTableName, newTableName)), autoCleanup);
        } catch (NamespaceNotFoundException e) {
          throw new ThriftTableOperationException(null, oldTableName, tableOp, TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }

        break;
      }
      case TABLE_CLONE: {
        TableOperation tableOp = TableOperation.CLONE;
        Table.ID srcTableId = validateTableIdArgument(arguments.get(0), tableOp, NOT_ROOT_ID);
        String tableName = validateTableNameArgument(arguments.get(1), tableOp, NOT_SYSTEM);
        Namespace.ID namespaceId;
        try {
          namespaceId = Namespaces.getNamespaceId(master.getInstance(), Tables.qualify(tableName).getFirst());
        } catch (NamespaceNotFoundException e) {
          // shouldn't happen, but possible once cloning between namespaces is supported
          throw new ThriftTableOperationException(null, tableName, tableOp, TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }

        final boolean canCloneTable;
        try {
          canCloneTable = master.security.canCloneTable(c, srcTableId, tableName, namespaceId, namespaceId);
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
            propertiesToExclude.add(entry.getKey().substring(TableOperationsImpl.CLONE_EXCLUDE_PREFIX.length()));
            continue;
          }

          if (!TablePropUtil.isPropertyValid(entry.getKey(), entry.getValue())) {
            throw new ThriftTableOperationException(null, tableName, tableOp, TableOperationExceptionType.OTHER, "Property or value not valid "
                + entry.getKey() + "=" + entry.getValue());
          }

          propertiesToSet.put(entry.getKey(), entry.getValue());
        }

        master.fate.seedTransaction(opid, new TraceRepo<>(new CloneTable(c.getPrincipal(), namespaceId, srcTableId, tableName, propertiesToSet,
            propertiesToExclude)), autoCleanup);

        break;
      }
      case TABLE_DELETE: {
        TableOperation tableOp = TableOperation.DELETE;
        String tableName = validateTableNameArgument(arguments.get(0), tableOp, NOT_SYSTEM);

        final Table.ID tableId = ClientServiceHandler.checkTableId(master.getInstance(), tableName, tableOp);
        Namespace.ID namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canDeleteTable;
        try {
          canDeleteTable = master.security.canDeleteTable(c, tableId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName, TableOperation.DELETE);
          throw e;
        }

        if (!canDeleteTable)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        master.fate.seedTransaction(opid, new TraceRepo<>(new DeleteTable(namespaceId, tableId)), autoCleanup);
        break;
      }
      case TABLE_ONLINE: {
        TableOperation tableOp = TableOperation.ONLINE;
        final Table.ID tableId = validateTableIdArgument(arguments.get(0), tableOp, NOT_ROOT_ID);
        Namespace.ID namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canOnlineOfflineTable;
        try {
          canOnlineOfflineTable = master.security.canOnlineOfflineTable(c, tableId, op, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, null, TableOperation.ONLINE);
          throw e;
        }

        if (!canOnlineOfflineTable)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<>(new ChangeTableState(namespaceId, tableId, tableOp)), autoCleanup);
        break;
      }
      case TABLE_OFFLINE: {
        TableOperation tableOp = TableOperation.OFFLINE;
        final Table.ID tableId = validateTableIdArgument(arguments.get(0), tableOp, NOT_ROOT_ID);
        Namespace.ID namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canOnlineOfflineTable;
        try {
          canOnlineOfflineTable = master.security.canOnlineOfflineTable(c, tableId, op, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, null, TableOperation.OFFLINE);
          throw e;
        }

        if (!canOnlineOfflineTable)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<>(new ChangeTableState(namespaceId, tableId, tableOp)), autoCleanup);
        break;
      }
      case TABLE_MERGE: {
        TableOperation tableOp = TableOperation.MERGE;
        String tableName = validateTableNameArgument(arguments.get(0), tableOp, null);
        Text startRow = ByteBufferUtil.toText(arguments.get(1));
        Text endRow = ByteBufferUtil.toText(arguments.get(2));

        final Table.ID tableId = ClientServiceHandler.checkTableId(master.getInstance(), tableName, tableOp);
        Namespace.ID namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

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
        master.fate.seedTransaction(opid, new TraceRepo<>(new TableRangeOp(MergeInfo.Operation.MERGE, namespaceId, tableId, startRow, endRow)), autoCleanup);
        break;
      }
      case TABLE_DELETE_RANGE: {
        TableOperation tableOp = TableOperation.DELETE_RANGE;
        String tableName = validateTableNameArgument(arguments.get(0), tableOp, NOT_METADATA);
        Text startRow = ByteBufferUtil.toText(arguments.get(1));
        Text endRow = ByteBufferUtil.toText(arguments.get(2));

        final Table.ID tableId = ClientServiceHandler.checkTableId(master.getInstance(), tableName, tableOp);
        Namespace.ID namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canDeleteRange;
        try {
          canDeleteRange = master.security.canDeleteRange(c, tableId, tableName, startRow, endRow, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName, TableOperation.DELETE_RANGE);
          throw e;
        }

        if (!canDeleteRange)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<>(new TableRangeOp(MergeInfo.Operation.DELETE, namespaceId, tableId, startRow, endRow)), autoCleanup);
        break;
      }
      case TABLE_BULK_IMPORT: {
        TableOperation tableOp = TableOperation.BULK_IMPORT;
        String tableName = validateTableNameArgument(arguments.get(0), tableOp, NOT_SYSTEM);
        String dir = ByteBufferUtil.toString(arguments.get(1));
        String failDir = ByteBufferUtil.toString(arguments.get(2));
        boolean setTime = Boolean.parseBoolean(ByteBufferUtil.toString(arguments.get(3)));

        final Table.ID tableId = ClientServiceHandler.checkTableId(master.getInstance(), tableName, tableOp);
        Namespace.ID namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canBulkImport;
        try {
          canBulkImport = master.security.canBulkImport(c, tableId, tableName, dir, failDir, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName, TableOperation.BULK_IMPORT);
          throw e;
        }

        if (!canBulkImport)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.updateBulkImportStatus(dir, BulkImportState.INITIAL);
        master.fate.seedTransaction(opid, new TraceRepo<>(new BulkImport(tableId, dir, failDir, setTime)), autoCleanup);
        break;
      }
      case TABLE_COMPACT: {
        TableOperation tableOp = TableOperation.COMPACT;
        Table.ID tableId = validateTableIdArgument(arguments.get(0), tableOp, null);
        byte[] startRow = ByteBufferUtil.toBytes(arguments.get(1));
        byte[] endRow = ByteBufferUtil.toBytes(arguments.get(2));
        List<IteratorSetting> iterators = IteratorUtil.decodeIteratorSettings(ByteBufferUtil.toBytes(arguments.get(3)));
        CompactionStrategyConfig compactionStrategy = CompactionStrategyConfigUtil.decode(ByteBufferUtil.toBytes(arguments.get(4)));
        Namespace.ID namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canCompact;
        try {
          canCompact = master.security.canCompact(c, tableId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, null, TableOperation.COMPACT);
          throw e;
        }

        if (!canCompact)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate
            .seedTransaction(opid, new TraceRepo<>(new CompactRange(namespaceId, tableId, startRow, endRow, iterators, compactionStrategy)), autoCleanup);
        break;
      }
      case TABLE_CANCEL_COMPACT: {
        TableOperation tableOp = TableOperation.COMPACT_CANCEL;
        Table.ID tableId = validateTableIdArgument(arguments.get(0), tableOp, null);
        Namespace.ID namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canCancelCompact;
        try {
          canCancelCompact = master.security.canCompact(c, tableId, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, null, TableOperation.COMPACT_CANCEL);
          throw e;
        }

        if (!canCancelCompact)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<>(new CancelCompactions(namespaceId, tableId)), autoCleanup);
        break;
      }
      case TABLE_IMPORT: {
        TableOperation tableOp = TableOperation.IMPORT;
        String tableName = validateTableNameArgument(arguments.get(0), tableOp, NOT_SYSTEM);
        String exportDir = ByteBufferUtil.toString(arguments.get(1));
        Namespace.ID namespaceId;
        try {
          namespaceId = Namespaces.getNamespaceId(master.getInstance(), Tables.qualify(tableName).getFirst());
        } catch (NamespaceNotFoundException e) {
          throw new ThriftTableOperationException(null, tableName, tableOp, TableOperationExceptionType.NAMESPACE_NOTFOUND, "");
        }

        final boolean canImport;
        try {
          canImport = master.security.canImport(c, tableName, exportDir, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, null, tableName, TableOperation.IMPORT);
          throw e;
        }

        if (!canImport)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<>(new ImportTable(c.getPrincipal(), tableName, exportDir, namespaceId)), autoCleanup);
        break;
      }
      case TABLE_EXPORT: {
        TableOperation tableOp = TableOperation.EXPORT;
        String tableName = validateTableNameArgument(arguments.get(0), tableOp, NOT_SYSTEM);
        String exportDir = ByteBufferUtil.toString(arguments.get(1));

        Table.ID tableId = ClientServiceHandler.checkTableId(master.getInstance(), tableName, tableOp);
        Namespace.ID namespaceId = getNamespaceIdFromTableId(tableOp, tableId);

        final boolean canExport;
        try {
          canExport = master.security.canExport(c, tableId, tableName, exportDir, namespaceId);
        } catch (ThriftSecurityException e) {
          throwIfTableMissingSecurityException(e, tableId, tableName, TableOperation.EXPORT);
          throw e;
        }

        if (!canExport)
          throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

        master.fate.seedTransaction(opid, new TraceRepo<>(new ExportTable(namespaceId, tableName, tableId, exportDir)), autoCleanup);
        break;
      }
      default:
        throw new UnsupportedOperationException();
    }
  }

  private Namespace.ID getNamespaceIdFromTableId(TableOperation tableOp, Table.ID tableId) throws ThriftTableOperationException {
    Namespace.ID namespaceId;
    try {
      namespaceId = Tables.getNamespaceId(master.getInstance(), tableId);
    } catch (TableNotFoundException e) {
      throw new ThriftTableOperationException(tableId.canonicalID(), null, tableOp, TableOperationExceptionType.NOTFOUND, e.getMessage());
    }
    return namespaceId;
  }

  /**
   * Inspects the {@link ThriftSecurityException} and throws a {@link ThriftTableOperationException} if the {@link SecurityErrorCode} on the
   * {@link ThriftSecurityException} was {code}TABLE_DOESNT_EXIST{code}. If the {@link ThriftSecurityException} is thrown because a table doesn't exist anymore,
   * clients will likely see an {@link AccumuloSecurityException} instead of a {@link TableNotFoundException} as expected. If the
   * {@link ThriftSecurityException} has a different {@link SecurityErrorCode}, this method does nothing and expects the caller to properly handle the original
   * exception.
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
  private void throwIfTableMissingSecurityException(ThriftSecurityException e, Table.ID tableId, String tableName, TableOperation op)
      throws ThriftTableOperationException {
    // ACCUMULO-3135 Table can be deleted after we get table ID but before we can check permission
    if (e.isSetCode() && SecurityErrorCode.TABLE_DOESNT_EXIST == e.getCode()) {
      throw new ThriftTableOperationException(tableId.canonicalID(), tableName, op, TableOperationExceptionType.NOTFOUND, "Table no longer exists");
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

  protected void authenticate(TCredentials credentials) throws ThriftSecurityException {
    // this is a bit redundant, the credentials of the caller (the first arg) will throw an exception if it fails to authenticate
    // before the second arg is checked (which would return true or false)
    if (!master.security.authenticateUser(credentials, credentials))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.BAD_CREDENTIALS);
  }

  // Verify table name arguments are valid, and match any additional restrictions
  private Table.ID validateTableIdArgument(ByteBuffer tableIdArg, TableOperation op, Validator<Table.ID> userValidator) throws ThriftTableOperationException {
    Table.ID tableId = tableIdArg == null ? null : ByteBufferUtil.toTableId(tableIdArg);
    try {
      return VALID_ID.and(userValidator).validate(tableId);
    } catch (IllegalArgumentException e) {
      String why = e.getMessage();
      // Information provided by a client should generate a user-level exception, not a system-level warning.
      log.debug(why);
      throw new ThriftTableOperationException(tableId.canonicalID(), null, op, TableOperationExceptionType.INVALID_NAME, why);
    }
  }

  // Verify table name arguments are valid, and match any additional restrictions
  private String validateTableNameArgument(ByteBuffer tableNameArg, TableOperation op, Validator<String> userValidator) throws ThriftTableOperationException {
    String tableName = tableNameArg == null ? null : ByteBufferUtil.toString(tableNameArg);
    return _validateArgument(tableName, op, VALID_NAME.and(userValidator));
  }

  // Verify namespace arguments are valid, and match any additional restrictions
  private String validateNamespaceArgument(ByteBuffer namespaceArg, TableOperation op, Validator<String> userValidator) throws ThriftTableOperationException {
    String namespace = namespaceArg == null ? null : ByteBufferUtil.toString(namespaceArg);
    return _validateArgument(namespace, op, Namespaces.VALID_NAME.and(userValidator));
  }

  // helper to handle the exception
  private <T> T _validateArgument(T arg, TableOperation op, Validator<T> validator) throws ThriftTableOperationException {
    try {
      return validator.validate(arg);
    } catch (IllegalArgumentException e) {
      String why = e.getMessage();
      // Information provided by a client should generate a user-level exception, not a system-level warning.
      log.debug(why);
      throw new ThriftTableOperationException(null, String.valueOf(arg), op, TableOperationExceptionType.INVALID_NAME, why);
    }
  }
}
