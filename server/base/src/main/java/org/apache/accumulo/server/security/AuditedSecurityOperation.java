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
package org.apache.accumulo.server.security;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.Translator;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.TColumn;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.accumulo.core.master.thrift.FateOperation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.security.handler.Authenticator;
import org.apache.accumulo.server.security.handler.Authorizor;
import org.apache.accumulo.server.security.handler.PermissionHandler;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class AuditedSecurityOperation extends SecurityOperation {

  public static final String AUDITLOG = "org.apache.accumulo.audit";
  public static final Logger audit = LoggerFactory.getLogger(AUDITLOG);

  public AuditedSecurityOperation(AccumuloServerContext context, Authorizor author, Authenticator authent, PermissionHandler pm) {
    super(context, author, authent, pm);
  }

  public static synchronized SecurityOperation getInstance(AccumuloServerContext context) {
    return getInstance(context, false);
  }

  public static synchronized SecurityOperation getInstance(AccumuloServerContext context, boolean initialize) {
    if (instance == null) {
      String instanceId = context.getInstance().getInstanceID();
      instance = new AuditedSecurityOperation(context, getAuthorizor(instanceId, initialize), getAuthenticator(instanceId, initialize), getPermHandler(
          instanceId, initialize));
    }
    return instance;
  }

  private String getTableName(Table.ID tableId) {
    try {
      return Tables.getTableName(context.getInstance(), tableId);
    } catch (TableNotFoundException e) {
      return "Unknown Table with ID " + tableId;
    }
  }

  public static StringBuilder getAuthString(List<ByteBuffer> authorizations) {
    StringBuilder auths = new StringBuilder();
    for (ByteBuffer bb : authorizations) {
      auths.append(ByteBufferUtil.toString(bb)).append(",");
    }
    return auths;
  }

  private boolean shouldAudit(TCredentials credentials, Table.ID tableId) {
    return (audit.isInfoEnabled() || audit.isWarnEnabled()) && !tableId.equals(MetadataTable.ID) && shouldAudit(credentials);
  }

  // Is INFO the right level to check? Do we even need that check?
  private boolean shouldAudit(TCredentials credentials) {
    return !context.getCredentials().getToken().getClass().getName().equals(credentials.getTokenClassName());
  }

  /*
   * Three auditing methods try to capture the 4 states we might have here. audit is in response to a thrown exception, the operation failed (perhaps due to
   * insufficient privs, or some other reason) audit(credentials, template, args) is a successful operation audit(credentials, permitted, template, args) is a
   * privileges check that is either permitted or denied. We don't know if the operation went on to be successful or not at this point, we would have to go
   * digging through loads of other code to find it.
   */
  private void audit(TCredentials credentials, ThriftSecurityException ex, String template, Object... args) {
    audit.warn("operation: failed; user: {}; {}; exception: {}", credentials.getPrincipal(), String.format(template, args), ex.toString());
  }

  private void audit(TCredentials credentials, String template, Object... args) {
    if (shouldAudit(credentials)) {
      audit.info("operation: success; user: {}: {}", credentials.getPrincipal(), String.format(template, args));
    }
  }

  private void audit(TCredentials credentials, boolean permitted, String template, Object... args) {
    if (shouldAudit(credentials)) {
      String prefix = permitted ? "permitted" : "denied";
      audit
          .info("operation: {}; user: {}; client: {}; {}", prefix, credentials.getPrincipal(), TServerUtils.clientAddress.get(), String.format(template, args));
    }
  }

  public static final String CAN_SCAN_AUDIT_TEMPLATE = "action: scan; targetTable: %s; authorizations: %s; range: %s; columns: %s; iterators: %s; iteratorOptions: %s;";
  private static final int MAX_ELEMENTS_TO_LOG = 10;

  private static List<String> truncate(Collection<?> list) {
    List<String> result = new ArrayList<>();
    int i = 0;
    for (Object obj : list) {
      if (i++ > MAX_ELEMENTS_TO_LOG) {
        result.add(" and " + (list.size() - MAX_ELEMENTS_TO_LOG) + " more ");
        break;
      }
      result.add(obj.toString());
    }
    return result;
  }

  @Override
  public boolean canScan(TCredentials credentials, Table.ID tableId, Namespace.ID namespaceId, TRange range, List<TColumn> columns, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations) throws ThriftSecurityException {
    if (shouldAudit(credentials, tableId)) {
      Range convertedRange = new Range(range);
      List<String> convertedColumns = truncate(Translator.translate(columns, new Translator.TColumnTranslator()));
      String tableName = getTableName(tableId);

      try {
        boolean canScan = super.canScan(credentials, tableId, namespaceId);
        audit(credentials, canScan, CAN_SCAN_AUDIT_TEMPLATE, tableName, getAuthString(authorizations), convertedRange, convertedColumns, ssiList, ssio);

        return canScan;
      } catch (ThriftSecurityException ex) {
        audit(credentials, ex, CAN_SCAN_AUDIT_TEMPLATE, getAuthString(authorizations), tableId, convertedRange, convertedColumns, ssiList, ssio);
        throw ex;
      }
    } else {
      return super.canScan(credentials, tableId, namespaceId);
    }
  }

  public static final String CAN_SCAN_BATCH_AUDIT_TEMPLATE = "action: scan; targetTable: %s; authorizations: %s; range: %s; columns: %s; iterators: %s; iteratorOptions: %s;";

  @Override
  public boolean canScan(TCredentials credentials, Table.ID tableId, Namespace.ID namespaceId, Map<TKeyExtent,List<TRange>> tbatch, List<TColumn> tcolumns,
      List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations) throws ThriftSecurityException {
    if (shouldAudit(credentials, tableId)) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      Map<KeyExtent,List<Range>> convertedBatch = Translator.translate(tbatch, new Translator.TKeyExtentTranslator(), new Translator.ListTranslator(
          new Translator.TRangeTranslator()));
      Map<KeyExtent,List<String>> truncated = new HashMap<>();
      for (Entry<KeyExtent,List<Range>> entry : convertedBatch.entrySet()) {
        truncated.put(entry.getKey(), truncate(entry.getValue()));
      }
      List<Column> convertedColumns = Translator.translate(tcolumns, new Translator.TColumnTranslator());
      String tableName = getTableName(tableId);

      try {
        boolean canScan = super.canScan(credentials, tableId, namespaceId);
        audit(credentials, canScan, CAN_SCAN_BATCH_AUDIT_TEMPLATE, tableName, getAuthString(authorizations), truncated, convertedColumns, ssiList, ssio);

        return canScan;
      } catch (ThriftSecurityException ex) {
        audit(credentials, ex, CAN_SCAN_BATCH_AUDIT_TEMPLATE, getAuthString(authorizations), tableId, truncated, convertedColumns, ssiList, ssio);
        throw ex;
      }
    } else {
      return super.canScan(credentials, tableId, namespaceId);
    }
  }

  public static final String CHANGE_AUTHORIZATIONS_AUDIT_TEMPLATE = "action: changeAuthorizations; targetUser: %s; authorizations: %s";

  @Override
  public void changeAuthorizations(TCredentials credentials, String user, Authorizations authorizations) throws ThriftSecurityException {
    try {
      super.changeAuthorizations(credentials, user, authorizations);
      audit(credentials, CHANGE_AUTHORIZATIONS_AUDIT_TEMPLATE, user, authorizations);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, CHANGE_AUTHORIZATIONS_AUDIT_TEMPLATE, user, authorizations);
      throw ex;
    }
  }

  public static final String CHANGE_PASSWORD_AUDIT_TEMPLATE = "action: changePassword; targetUser: %s;";

  @Override
  public void changePassword(TCredentials credentials, Credentials newInfo) throws ThriftSecurityException {
    try {
      super.changePassword(credentials, newInfo);
      audit(credentials, CHANGE_PASSWORD_AUDIT_TEMPLATE, newInfo.getPrincipal());
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, CHANGE_PASSWORD_AUDIT_TEMPLATE, newInfo.getPrincipal());
      throw ex;
    }
  }

  public static final String CREATE_USER_AUDIT_TEMPLATE = "action: createUser; targetUser: %s; Authorizations: %s;";

  @Override
  public void createUser(TCredentials credentials, Credentials newUser, Authorizations authorizations) throws ThriftSecurityException {
    try {
      super.createUser(credentials, newUser, authorizations);
      audit(credentials, CREATE_USER_AUDIT_TEMPLATE, newUser.getPrincipal(), authorizations);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, CREATE_USER_AUDIT_TEMPLATE, newUser.getPrincipal(), authorizations);
      throw ex;
    }
  }

  public static final String CAN_CREATE_TABLE_AUDIT_TEMPLATE = "action: createTable; targetTable: %s;";

  @Override
  public boolean canCreateTable(TCredentials c, String tableName, Namespace.ID namespaceId) throws ThriftSecurityException {
    try {
      boolean result = super.canCreateTable(c, tableName, namespaceId);
      audit(c, result, CAN_CREATE_TABLE_AUDIT_TEMPLATE, tableName);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, CAN_CREATE_TABLE_AUDIT_TEMPLATE, tableName);
      throw ex;
    }
  }

  public static final String CAN_DELETE_TABLE_AUDIT_TEMPLATE = "action: deleteTable; targetTable: %s;";

  @Override
  public boolean canDeleteTable(TCredentials c, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    String tableName = getTableName(tableId);
    try {
      boolean result = super.canDeleteTable(c, tableId, namespaceId);
      audit(c, result, CAN_DELETE_TABLE_AUDIT_TEMPLATE, tableName, tableId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, CAN_DELETE_TABLE_AUDIT_TEMPLATE, tableName, tableId);
      throw ex;
    }
  }

  public static final String CAN_RENAME_TABLE_AUDIT_TEMPLATE = "action: renameTable; targetTable: %s; newTableName: %s;";

  @Override
  public boolean canRenameTable(TCredentials c, Table.ID tableId, String oldTableName, String newTableName, Namespace.ID namespaceId)
      throws ThriftSecurityException {
    try {
      boolean result = super.canRenameTable(c, tableId, oldTableName, newTableName, namespaceId);
      audit(c, result, CAN_RENAME_TABLE_AUDIT_TEMPLATE, oldTableName, newTableName);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, CAN_RENAME_TABLE_AUDIT_TEMPLATE, oldTableName, newTableName);
      throw ex;
    }
  }

  public static final String CAN_SPLIT_TABLE_AUDIT_TEMPLATE = "action: splitTable; targetTable: %s; targetNamespace: %s;";

  @Override
  public boolean canSplitTablet(TCredentials credentials, Table.ID table, Namespace.ID namespaceId) throws ThriftSecurityException {
    try {
      boolean result = super.canSplitTablet(credentials, table, namespaceId);
      audit(credentials, result, CAN_SPLIT_TABLE_AUDIT_TEMPLATE, table, namespaceId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "split tablet on table %s denied", table);
      throw ex;
    }
  }

  public static final String CAN_PERFORM_SYSTEM_ACTION_AUDIT_TEMPLATE = "action: performSystemAction; principal: %s;";

  @Override
  public boolean canPerformSystemActions(TCredentials credentials) throws ThriftSecurityException {
    try {
      boolean result = super.canPerformSystemActions(credentials);
      audit(credentials, result, CAN_PERFORM_SYSTEM_ACTION_AUDIT_TEMPLATE, credentials.getPrincipal());
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "system action denied");
      throw ex;
    }
  }

  public static final String CAN_FLUSH_TABLE_AUDIT_TEMPLATE = "action: flushTable; targetTable: %s; targetNamespace: %s;";

  @Override
  public boolean canFlush(TCredentials c, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    try {
      boolean result = super.canFlush(c, tableId, namespaceId);
      audit(c, result, CAN_FLUSH_TABLE_AUDIT_TEMPLATE, tableId, namespaceId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "flush on tableId %s denied", tableId);
      throw ex;
    }
  }

  public static final String CAN_ALTER_TABLE_AUDIT_TEMPLATE = "action: alterTable; targetTable: %s; targetNamespace: %s;";

  @Override
  public boolean canAlterTable(TCredentials c, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    try {
      boolean result = super.canAlterTable(c, tableId, namespaceId);
      audit(c, result, CAN_ALTER_TABLE_AUDIT_TEMPLATE, tableId, namespaceId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "alter table on tableId %s denied", tableId);
      throw ex;
    }
  }

  public static final String CAN_CLONE_TABLE_AUDIT_TEMPLATE = "action: cloneTable; targetTable: %s; newTableName: %s";

  @Override
  public boolean canCloneTable(TCredentials c, Table.ID tableId, String tableName, Namespace.ID destinationNamespaceId, Namespace.ID sourceNamespaceId)
      throws ThriftSecurityException {
    String oldTableName = getTableName(tableId);
    try {
      boolean result = super.canCloneTable(c, tableId, tableName, destinationNamespaceId, sourceNamespaceId);
      audit(c, result, CAN_CLONE_TABLE_AUDIT_TEMPLATE, oldTableName, tableName);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, CAN_CLONE_TABLE_AUDIT_TEMPLATE, oldTableName, tableName);
      throw ex;
    }
  }

  public static final String CAN_DELETE_RANGE_AUDIT_TEMPLATE = "action: deleteData; targetTable: %s; startRange: %s; endRange: %s;";

  @Override
  public boolean canDeleteRange(TCredentials c, Table.ID tableId, String tableName, Text startRow, Text endRow, Namespace.ID namespaceId)
      throws ThriftSecurityException {
    try {
      boolean result = super.canDeleteRange(c, tableId, tableName, startRow, endRow, namespaceId);
      audit(c, result, CAN_DELETE_RANGE_AUDIT_TEMPLATE, tableName, startRow.toString(), endRow.toString());
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, CAN_DELETE_RANGE_AUDIT_TEMPLATE, tableName, startRow.toString(), endRow.toString());
      throw ex;
    }
  }

  public static final String CAN_BULK_IMPORT_AUDIT_TEMPLATE = "action: bulkImport; targetTable: %s; dataDir: %s; failDir: %s;";

  @Override
  public boolean canBulkImport(TCredentials c, Table.ID tableId, String tableName, String dir, String failDir, Namespace.ID namespaceId)
      throws ThriftSecurityException {
    try {
      boolean result = super.canBulkImport(c, tableId, namespaceId);
      audit(c, result, CAN_BULK_IMPORT_AUDIT_TEMPLATE, tableName, dir, failDir);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, CAN_BULK_IMPORT_AUDIT_TEMPLATE, tableName, dir, failDir);
      throw ex;
    }
  }

  public static final String CAN_COMPACT_TABLE_AUDIT_TEMPLATE = "action: compactTable; targetTable: %s; targetNamespace: %s;";

  @Override
  public boolean canCompact(TCredentials c, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    try {
      boolean result = super.canCompact(c, tableId, namespaceId);
      audit(c, result, CAN_COMPACT_TABLE_AUDIT_TEMPLATE, tableId, namespaceId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "compact on tableId %s denied", tableId);
      throw ex;
    }
  }

  public static final String CAN_CHANGE_AUTHORIZATIONS_AUDIT_TEMPLATE = "action: changeAuthorizations; targetUser: %s;";

  @Override
  public boolean canChangeAuthorizations(TCredentials c, String user) throws ThriftSecurityException {
    try {
      boolean result = super.canChangeAuthorizations(c, user);
      audit(c, result, CAN_CHANGE_AUTHORIZATIONS_AUDIT_TEMPLATE, user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "change authorizations on user %s denied", user);
      throw ex;
    }
  }

  public static final String CAN_CHANGE_PASSWORD_AUDIT_TEMPLATE = "action: changePassword; targetUser: %s;";

  @Override
  public boolean canChangePassword(TCredentials c, String user) throws ThriftSecurityException {
    try {
      boolean result = super.canChangePassword(c, user);
      audit(c, result, CAN_CHANGE_PASSWORD_AUDIT_TEMPLATE, user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "change password on user %s denied", user);
      throw ex;
    }
  }

  public static final String CAN_CREATE_USER_AUDIT_TEMPLATE = "action: createUser; targetUser: %s;";

  @Override
  public boolean canCreateUser(TCredentials c, String user) throws ThriftSecurityException {
    try {
      boolean result = super.canCreateUser(c, user);
      audit(c, result, CAN_CREATE_USER_AUDIT_TEMPLATE, user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "create user on user %s denied", user);
      throw ex;
    }
  }

  public static final String CAN_DROP_USER_AUDIT_TEMPLATE = "action: dropUser; targetUser: %s;";

  @Override
  public boolean canDropUser(TCredentials c, String user) throws ThriftSecurityException {
    try {
      boolean result = super.canDropUser(c, user);
      audit(c, result, CAN_DROP_USER_AUDIT_TEMPLATE, user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "drop user on user %s denied", user);

      throw ex;
    }
  }

  public static final String CAN_GRANT_SYSTEM_AUDIT_TEMPLATE = "action: grantSystem; targetUser: %s; targetPermission: %s;";

  @Override
  public boolean canGrantSystem(TCredentials c, String user, SystemPermission sysPerm) throws ThriftSecurityException {
    try {
      boolean result = super.canGrantSystem(c, user, sysPerm);
      audit(c, result, CAN_GRANT_SYSTEM_AUDIT_TEMPLATE, user, sysPerm);
      return result;

    } catch (ThriftSecurityException ex) {
      audit(c, ex, "grant system permission %s for user %s denied", sysPerm, user);

      throw ex;
    }
  }

  public static final String CAN_GRANT_TABLE_AUDIT_TEMPLATE = "action: grantTable; targetUser: %s; targetTable: %s; targetNamespace: %s;";

  @Override
  public boolean canGrantTable(TCredentials c, String user, Table.ID table, Namespace.ID namespaceId) throws ThriftSecurityException {
    try {
      boolean result = super.canGrantTable(c, user, table, namespaceId);
      audit(c, result, CAN_GRANT_TABLE_AUDIT_TEMPLATE, user, table, namespaceId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "grant table on table %s for user %s denied", table, user);
      throw ex;
    }
  }

  public static final String CAN_REVOKE_SYSTEM_AUDIT_TEMPLATE = "action: revokeSystem; targetUser: %s;, targetPermission: %s;";

  @Override
  public boolean canRevokeSystem(TCredentials c, String user, SystemPermission sysPerm) throws ThriftSecurityException {
    try {
      boolean result = super.canRevokeSystem(c, user, sysPerm);
      audit(c, result, CAN_REVOKE_SYSTEM_AUDIT_TEMPLATE, user, sysPerm);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "revoke system permission %s for user %s denied", sysPerm, user);
      throw ex;
    }
  }

  public static final String CAN_REVOKE_TABLE_AUDIT_TEMPLATE = "action: revokeTable; targetUser: %s; targetTable %s; targetNamespace: %s;";

  @Override
  public boolean canRevokeTable(TCredentials c, String user, Table.ID table, Namespace.ID namespaceId) throws ThriftSecurityException {
    try {
      boolean result = super.canRevokeTable(c, user, table, namespaceId);
      audit(c, result, CAN_REVOKE_TABLE_AUDIT_TEMPLATE, user, table, namespaceId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "revoke table on table %s for user %s denied", table, user);
      throw ex;
    }
  }

  public static final String CAN_IMPORT_AUDIT_TEMPLATE = "action: import; targetTable: %s; dataDir: %s;";

  @Override
  public boolean canImport(TCredentials credentials, String tableName, String importDir, Namespace.ID namespaceId) throws ThriftSecurityException {

    try {
      boolean result = super.canImport(credentials, tableName, importDir, namespaceId);
      audit(credentials, result, CAN_IMPORT_AUDIT_TEMPLATE, tableName, importDir);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, CAN_IMPORT_AUDIT_TEMPLATE, tableName, importDir);
      throw ex;
    }
  }

  public static final String CAN_EXPORT_AUDIT_TEMPLATE = "action: export; targetTable: %s; dataDir: %s;";

  @Override
  public boolean canExport(TCredentials credentials, Table.ID tableId, String tableName, String exportDir, Namespace.ID namespaceId)
      throws ThriftSecurityException {

    try {
      boolean result = super.canExport(credentials, tableId, tableName, exportDir, namespaceId);
      audit(credentials, result, CAN_EXPORT_AUDIT_TEMPLATE, tableName, exportDir);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, CAN_EXPORT_AUDIT_TEMPLATE, tableName, exportDir);
      throw ex;
    }
  }

  public static final String DROP_USER_AUDIT_TEMPLATE = "action: dropUser; targetUser: %s;";

  @Override
  public void dropUser(TCredentials credentials, String user) throws ThriftSecurityException {
    try {
      super.dropUser(credentials, user);
      audit(credentials, DROP_USER_AUDIT_TEMPLATE, user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, DROP_USER_AUDIT_TEMPLATE, user);
      throw ex;
    }
  }

  public static final String GRANT_SYSTEM_PERMISSION_AUDIT_TEMPLATE = "action: grantSystemPermission; permission: %s; targetUser: %s;";

  @Override
  public void grantSystemPermission(TCredentials credentials, String user, SystemPermission permission) throws ThriftSecurityException {
    try {
      super.grantSystemPermission(credentials, user, permission);
      audit(credentials, GRANT_SYSTEM_PERMISSION_AUDIT_TEMPLATE, permission, user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, GRANT_SYSTEM_PERMISSION_AUDIT_TEMPLATE, permission, user);
      throw ex;
    }
  }

  public static final String GRANT_TABLE_PERMISSION_AUDIT_TEMPLATE = "action: grantTablePermission; permission: %s; targetTable: %s; targetUser: %s;";

  @Override
  public void grantTablePermission(TCredentials credentials, String user, Table.ID tableId, TablePermission permission, Namespace.ID namespaceId)
      throws ThriftSecurityException {
    String tableName = getTableName(tableId);
    try {
      super.grantTablePermission(credentials, user, tableId, permission, namespaceId);
      audit(credentials, GRANT_TABLE_PERMISSION_AUDIT_TEMPLATE, permission, tableName, user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, GRANT_TABLE_PERMISSION_AUDIT_TEMPLATE, permission, tableName, user);
      throw ex;
    }
  }

  public static final String REVOKE_SYSTEM_PERMISSION_AUDIT_TEMPLATE = "action: revokeSystemPermission; permission: %s; targetUser: %s;";

  @Override
  public void revokeSystemPermission(TCredentials credentials, String user, SystemPermission permission) throws ThriftSecurityException {

    try {
      super.revokeSystemPermission(credentials, user, permission);
      audit(credentials, REVOKE_SYSTEM_PERMISSION_AUDIT_TEMPLATE, permission, user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, REVOKE_SYSTEM_PERMISSION_AUDIT_TEMPLATE, permission, user);
      throw ex;
    }
  }

  public static final String REVOKE_TABLE_PERMISSION_AUDIT_TEMPLATE = "action: revokeTablePermission; permission: %s; targetTable: %s; targetUser: %s;";

  @Override
  public void revokeTablePermission(TCredentials credentials, String user, Table.ID tableId, TablePermission permission, Namespace.ID namespaceId)
      throws ThriftSecurityException {
    String tableName = getTableName(tableId);
    try {
      super.revokeTablePermission(credentials, user, tableId, permission, namespaceId);
      audit(credentials, REVOKE_TABLE_PERMISSION_AUDIT_TEMPLATE, permission, tableName, user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, REVOKE_TABLE_PERMISSION_AUDIT_TEMPLATE, permission, tableName, user);
      throw ex;
    }
  }

  public static final String HAS_SYSTEM_PERMISSION_AUDIT_TEMPLATE = "action: hasSystemPermission; permission: %s; targetUser: %s;";

  @Override
  public boolean hasSystemPermission(TCredentials credentials, String user, SystemPermission permission) throws ThriftSecurityException {
    try {
      boolean result = super.hasSystemPermission(credentials, user, permission);
      audit(credentials, result, HAS_SYSTEM_PERMISSION_AUDIT_TEMPLATE, permission, user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "checking permission %s on %s denied", permission, user);
      throw ex;
    }
  }

  public static final String CAN_ONLINE_OFFLINE_TABLE_AUDIT_TEMPLATE = "action: %s; targetTable: %s;";

  @Override
  public boolean canOnlineOfflineTable(TCredentials credentials, Table.ID tableId, FateOperation op, Namespace.ID namespaceId) throws ThriftSecurityException {
    String tableName = getTableName(tableId);
    String operation = null;
    if (op == FateOperation.TABLE_ONLINE)
      operation = "onlineTable";
    if (op == FateOperation.TABLE_OFFLINE)
      operation = "offlineTable";
    try {
      boolean result = super.canOnlineOfflineTable(credentials, tableId, op, namespaceId);
      audit(credentials, result, CAN_ONLINE_OFFLINE_TABLE_AUDIT_TEMPLATE, operation, tableName, tableId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, CAN_ONLINE_OFFLINE_TABLE_AUDIT_TEMPLATE, operation, tableName, tableId);
      throw ex;
    }
  }

  public static final String CAN_MERGE_TABLE_AUDIT_TEMPLATE = "action: mergeTable; targetTable: %s; targetNamespace: %s;";

  @Override
  public boolean canMerge(TCredentials c, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    try {
      boolean result = super.canMerge(c, tableId, namespaceId);
      audit(c, result, CAN_MERGE_TABLE_AUDIT_TEMPLATE, tableId, namespaceId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "merge table on tableId %s denied", tableId);
      throw ex;
    }
  }

  // The audit log is already logging the principal, so we don't have anything else to audit
  public static final String AUTHENICATE_AUDIT_TEMPLATE = "action: authenticate;";

  @Override
  protected void authenticate(TCredentials credentials) throws ThriftSecurityException {
    try {
      super.authenticate(credentials);
      audit(credentials, true, AUTHENICATE_AUDIT_TEMPLATE);
    } catch (ThriftSecurityException e) {
      audit(credentials, false, AUTHENICATE_AUDIT_TEMPLATE);
      throw e;
    }
  }

  public static final String DELEGATION_TOKEN_AUDIT_TEMPLATE = "requested delegation token";

  @Override
  public boolean canObtainDelegationToken(TCredentials credentials) throws ThriftSecurityException {
    try {
      boolean result = super.canObtainDelegationToken(credentials);
      audit(credentials, result, DELEGATION_TOKEN_AUDIT_TEMPLATE);
      return result;
    } catch (ThriftSecurityException e) {
      audit(credentials, false, DELEGATION_TOKEN_AUDIT_TEMPLATE);
      throw e;
    }
  }
}
