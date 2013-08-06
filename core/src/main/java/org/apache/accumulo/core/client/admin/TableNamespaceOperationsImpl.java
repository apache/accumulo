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
package org.apache.accumulo.core.client.admin;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import jline.internal.Log;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNamespaceExistsException;
import org.apache.accumulo.core.client.TableNamespaceNotEmptyException;
import org.apache.accumulo.core.client.TableNamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.impl.ServerClient;
import org.apache.accumulo.core.client.impl.TableNamespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.TableOperation;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

/**
 * Provides a class for administering table namespaces
 * 
 */
public class TableNamespaceOperationsImpl extends TableNamespaceOperationsHelper {
  private Instance instance;
  private Credentials credentials;

  private static final Logger log = Logger.getLogger(TableOperations.class);

  /**
   * @param instance
   *          the connection information for this instance
   * @param credentials
   *          the username/password for this connection
   */
  public TableNamespaceOperationsImpl(Instance instance, Credentials credentials) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.credentials = credentials;
  }

  /**
   * Retrieve a list of table namespaces in Accumulo.
   * 
   * @return List of table namespaces in accumulo
   */
  @Override
  public SortedSet<String> list() {
    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Fetching list of table namespaces...");
    TreeSet<String> namespaces = new TreeSet<String>(TableNamespaces.getNameToIdMap(instance).keySet());
    opTimer.stop("Fetched " + namespaces.size() + " table namespaces in %DURATION%");
    return namespaces;
  }

  /**
   * A method to check if a table namespace exists in Accumulo.
   * 
   * @param namespace
   *          the name of the table namespace
   * @return true if the table namespace exists
   */
  @Override
  public boolean exists(String namespace) {
    ArgumentChecker.notNull(namespace);

    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Checking if table namespace " + namespace + " exists...");
    boolean exists = TableNamespaces.getNameToIdMap(instance).containsKey(namespace);
    opTimer.stop("Checked existance of " + exists + " in %DURATION%");
    return exists;
  }

  /**
   * Create a table namespace with no special configuration
   * 
   * @param namespace
   *          the name of the table namespace
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNamespaceExistsException
   *           if the table namespace already exists
   */
  @Override
  public void create(String namespace) throws AccumuloException, AccumuloSecurityException, TableNamespaceExistsException {
    create(namespace, true, TimeType.MILLIS);
  }

  /**
   * @param namespace
   *          the name of the table namespace
   * @param limitVersion
   *          Enables/disables the versioning iterator, which will limit the number of Key versions kept.
   */
  @Override
  public void create(String namespace, boolean limitVersion) throws AccumuloException, AccumuloSecurityException, TableNamespaceExistsException {
    create(namespace, limitVersion, TimeType.MILLIS);
  }

  /**
   * @param namespace
   *          the name of the table namespace
   * @param timeType
   *          specifies logical or real-time based time recording for entries in the table
   * @param limitVersion
   *          Enables/disables the versioning iterator, which will limit the number of Key versions kept.
   */
  @Override
  public void create(String namespace, boolean limitVersion, TimeType timeType) throws AccumuloException, AccumuloSecurityException,
      TableNamespaceExistsException {
    ArgumentChecker.notNull(namespace, timeType);

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(namespace.getBytes()), ByteBuffer.wrap(timeType.name().getBytes()));

    Map<String,String> opts = IteratorUtil.generateInitialTableProperties(limitVersion);

    try {
      doTableNamespaceOperation(TableOperation.CREATE, args, opts);
    } catch (TableNamespaceNotFoundException e1) {
      // should not happen
      throw new RuntimeException(e1);
    }
  }

  private long beginTableNamespaceOperation() throws ThriftSecurityException, TException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        return client.beginTableNamespaceOperation(Tracer.traceInfo(), credentials.toThrift(instance));
      } catch (TTransportException tte) {
        log.debug("Failed to call beginTableOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }

  private void executeTableNamespaceOperation(long opid, TableOperation op, List<ByteBuffer> args, Map<String,String> opts, boolean autoCleanUp)
      throws ThriftSecurityException, TException, ThriftTableOperationException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        client.executeTableNamespaceOperation(Tracer.traceInfo(), credentials.toThrift(instance), opid, op, args, opts, autoCleanUp);
        break;
      } catch (TTransportException tte) {
        log.debug("Failed to call executeTableOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }

  private String waitForTableNamespaceOperation(long opid) throws ThriftSecurityException, TException, ThriftTableOperationException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        return client.waitForTableNamespaceOperation(Tracer.traceInfo(), credentials.toThrift(instance), opid);
      } catch (TTransportException tte) {
        log.debug("Failed to call waitForTableOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }

  private void finishTableNamespaceOperation(long opid) throws ThriftSecurityException, TException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        client.finishTableNamespaceOperation(Tracer.traceInfo(), credentials.toThrift(instance), opid);
        break;
      } catch (TTransportException tte) {
        log.debug("Failed to call finishTableOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }

  private String doTableNamespaceOperation(TableOperation op, List<ByteBuffer> args, Map<String,String> opts) throws AccumuloSecurityException,
      TableNamespaceExistsException, TableNamespaceNotFoundException, AccumuloException {
    return doTableNamespaceOperation(op, args, opts, true);
  }

  private String doTableNamespaceOperation(TableOperation op, List<ByteBuffer> args, Map<String,String> opts, boolean wait) throws AccumuloSecurityException,
      TableNamespaceExistsException, TableNamespaceNotFoundException, AccumuloException {
    Long opid = null;

    try {
      opid = beginTableNamespaceOperation();
      executeTableNamespaceOperation(opid, op, args, opts, !wait);
      if (!wait) {
        opid = null;
        return null;
      }
      String ret = waitForTableNamespaceOperation(opid);
      Tables.clearCache(instance);
      return ret;
    } catch (ThriftSecurityException e) {
      String tableName = ByteBufferUtil.toString(args.get(0));
      String tableInfo = Tables.getPrintableTableInfoFromName(instance, tableName);
      throw new AccumuloSecurityException(e.user, e.code, tableInfo, e);
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case EXISTS:
          throw new TableNamespaceExistsException(e);
        case NOTFOUND:
          throw new TableNamespaceNotFoundException(e);
        case OFFLINE:
          throw new TableOfflineException(instance, null);
        case OTHER:
        default:
          throw new AccumuloException(e.description, e);
      }
    } catch (Exception e) {
      throw new AccumuloException(e.getMessage(), e);
    } finally {
      // always finish table op, even when exception
      if (opid != null)
        try {
          finishTableNamespaceOperation(opid);
        } catch (Exception e) {
          log.warn(e.getMessage(), e);
        }
    }
  }

  /**
   * Delete a table namespace if empty
   * 
   * @param namespace
   *          the name of the table namespace
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNamespaceNotFoundException
   *           if the table namespace does not exist
   * @throws TableNamespaceNotEmptyException
   *           if the table namespaces still contains tables
   * @throws TableNotFoundException 
   *           if table not found while deleting
   */
  @Override
  public void delete(String namespace) throws AccumuloException, AccumuloSecurityException, TableNamespaceNotFoundException, TableNamespaceNotEmptyException, TableNotFoundException {
    delete(namespace, false);
  }

  /**
   * Delete a table namespace
   * 
   * @param namespace
   *          the name of the table namespace
   * @param deleteTables
   *          boolean, if true deletes all the tables in the namespace in addition to deleting the namespace.
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNamespaceNotFoundException
   *           if the table namespace does not exist
   * @throws TableNamespaceNotEmptyException
   *           if the table namespaces still contains tables
   * @throws TableNotFoundException 
   *           if table not found while deleting
   */
  @Override
  public void delete(String namespace, boolean deleteTables) throws AccumuloException, AccumuloSecurityException, TableNamespaceNotFoundException,
      TableNamespaceNotEmptyException, TableNotFoundException {
    ArgumentChecker.notNull(namespace);
    String namespaceId = TableNamespaces.getNamespaceId(instance, namespace);

    if (namespaceId.equals(Constants.SYSTEM_TABLE_NAMESPACE_ID) || namespaceId.equals(Constants.DEFAULT_TABLE_NAMESPACE_ID)) {
      String why = "Can't delete the system or default table namespace";
      throw new RuntimeException(why);
    }

    if (TableNamespaces.getTableIds(instance, namespaceId).size() > 0) {
      if (!deleteTables) {
        throw new TableNamespaceNotEmptyException(namespaceId, namespace, null);
      }
      for (String table : TableNamespaces.getTableNames(instance, namespaceId)) {
        getTableOperations().delete(table);
      }
    }

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(namespace.getBytes()));
    Map<String,String> opts = new HashMap<String,String>();

    try {
      doTableNamespaceOperation(TableOperation.DELETE, args, opts);
    } catch (TableNamespaceExistsException e) {
      // should not happen
      throw new RuntimeException(e);
    }

  }

  /**
   * Clone a all the tables in a table namespace to a new table namespace. Optionally copy all their properties as well.
   * 
   * @param srcName
   *          The table namespace to clone
   * @param newName
   *          The new table namespace to clone to
   * @param flush
   *          Whether to flush each table before cloning
   * @param propertiesToSet
   *          Which table namespace properties to set
   * @param propertiesToExclude
   *          Which table namespace properties to exclude
   * @param copyTableProps
   *          Whether to copy each table's properties
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws TableNamespaceNotFoundException
   *           If the old table namespace doesn't exist
   * @throws TableNamespaceExistsException
   *           If the new table namespace already exists
   */
  @Override
  public void clone(String srcName, String newName, boolean flush, Map<String,String> propertiesToSet, Set<String> propertiesToExclude, Boolean copyTableProps)
      throws AccumuloSecurityException, AccumuloException, TableNamespaceNotFoundException, TableNamespaceExistsException {

    ArgumentChecker.notNull(srcName, newName);

    String namespaceId = TableNamespaces.getNamespaceId(instance, srcName);

    if (propertiesToExclude == null)
      propertiesToExclude = Collections.emptySet();

    if (propertiesToSet == null)
      propertiesToSet = Collections.emptyMap();

    if (!Collections.disjoint(propertiesToExclude, propertiesToSet.keySet()))
      throw new IllegalArgumentException("propertiesToSet and propertiesToExclude not disjoint");

    String srcNamespaceId = TableNamespaces.getNamespaceId(instance, srcName);
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(srcNamespaceId.getBytes()), ByteBuffer.wrap(newName.getBytes()));
    Map<String,String> opts = new HashMap<String,String>();
    opts.putAll(propertiesToSet);
    for (String prop : propertiesToExclude)
      opts.put(prop, null);
    doTableNamespaceOperation(TableOperation.CLONE, args, opts);

    for (String tableId : TableNamespaces.getTableIds(instance, namespaceId)) {
      try {
        String tableName = Tables.getTableName(instance, tableId);

        String newTableName = newName + "." + Tables.extractTableName(tableName);
        getTableOperations().clone(tableName, newTableName, flush, null, null);
      } catch (TableNotFoundException e) {
        String why = "Table (" + tableId + ") dissappeared while cloning namespace (" + srcName + ")";
        throw new IllegalStateException(why);
      } catch (TableExistsException e) {
        String why = "Table somehow already existed in the newly created namespace (" + newName + ")";
        throw new IllegalStateException(why);
      }
    }
  }

  /**
   * Rename a table namespace
   * 
   * @param oldNamespaceName
   *          the old table namespace
   * @param newNamespaceName
   *          the new table namespace
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNamespaceNotFoundException
   *           if the old table namespace name does not exist
   * @throws TableNamespaceExistsException
   *           if the new table namespace name already exists
   */
  @Override
  public void rename(String oldNamespaceName, String newNamespaceName) throws AccumuloSecurityException, TableNamespaceNotFoundException, AccumuloException,
      TableNamespaceExistsException {

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(oldNamespaceName.getBytes()), ByteBuffer.wrap(newNamespaceName.getBytes()));
    Map<String,String> opts = new HashMap<String,String>();
    doTableNamespaceOperation(TableOperation.RENAME, args, opts);
  }

  /**
   * Sets a property on a table namespace
   * 
   * @param namespace
   *          the name of the table namespace
   * @param property
   *          the name of a per-table property
   * @param value
   *          the value to set a per-table property to
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  @Override
  public void setProperty(final String namespace, final String property, final String value) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(namespace, property, value);

    MasterClient.execute(instance, new ClientExec<MasterClientService.Client>() {
      @Override
      public void execute(MasterClientService.Client client) throws Exception {
        client.setTableNamespaceProperty(Tracer.traceInfo(), credentials.toThrift(instance), namespace, property, value);
      }
    });
  }

  /**
   * Removes a property from a table namespace
   * 
   * @param namespace
   *          the name of the table namespace
   * @param property
   *          the name of a per-table property
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  @Override
  public void removeProperty(final String namespace, final String property) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(namespace, property);

    MasterClient.execute(instance, new ClientExec<MasterClientService.Client>() {
      @Override
      public void execute(MasterClientService.Client client) throws Exception {
        client.removeTableNamespaceProperty(Tracer.traceInfo(), credentials.toThrift(instance), namespace, property);
      }
    });
  }

  /**
   * Gets properties of a table namespace
   * 
   * @param namespace
   *          the name of the table namespace
   * @return all properties visible by this table namespace (system and per-namespace properties)
   * @throws TableNamespaceNotFoundException
   *           if the table namespace does not exist
   */
  @Override
  public Iterable<Entry<String,String>> getProperties(final String namespace) throws AccumuloException, TableNamespaceNotFoundException {
    ArgumentChecker.notNull(namespace);
    try {
      return ServerClient.executeRaw(instance, new ClientExecReturn<Map<String,String>,ClientService.Client>() {
        @Override
        public Map<String,String> execute(ClientService.Client client) throws Exception {
          return client.getTableNamespaceConfiguration(Tracer.traceInfo(), credentials.toThrift(instance), namespace);
        }
      }).entrySet();
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case NOTFOUND:
          throw new TableNamespaceNotFoundException(e);
        case OTHER:
        default:
          throw new AccumuloException(e.description, e);
      }
    } catch (AccumuloException e) {
      throw e;
    } catch (Exception e) {
      throw new AccumuloException(e);
    }

  }

  /**
   * 
   * @param namespace
   *          the table namespace to take offline
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws TableNamespaceNotFoundException
   */
  @Override
  public void offline(String namespace) throws AccumuloSecurityException, AccumuloException, TableNamespaceNotFoundException {

    ArgumentChecker.notNull(namespace);
    String namespaceId = TableNamespaces.getNamespaceId(instance, namespace);
    try {
      for (String table : TableNamespaces.getTableNames(instance, namespaceId)) {
        getTableOperations().offline(table);
      }
    } catch (TableNotFoundException e) {
      Log.error("Table namespace (" + namespaceId + ") contains reference to table that doesn't exist");
    }
  }

  /**
   * 
   * @param namespace
   *          the table namespace to take online
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws TableNamespaceNotFoundException
   */
  @Override
  public void online(String namespace) throws AccumuloSecurityException, AccumuloException, TableNamespaceNotFoundException {
    ArgumentChecker.notNull(namespace);
    String namespaceId = TableNamespaces.getNamespaceId(instance, namespace);
    try {
      for (String table : TableNamespaces.getTableNames(instance, namespaceId)) {
        getTableOperations().online(table);
      }
    } catch (TableNotFoundException e) {
      Log.warn("Table namespace (" + namespaceId + ") contains a reference to a table that doesn't exist");
    }
  }

  /**
   * Get a mapping of table namespace name to internal table namespace id.
   * 
   * @return the map from table namespace name to internal table namespace id
   */
  @Override
  public Map<String,String> namespaceIdMap() {
    return TableNamespaces.getNameToIdMap(instance);
  }

  @Override
  public List<DiskUsage> getDiskUsage(String namespace) throws AccumuloException, AccumuloSecurityException, TableNamespaceNotFoundException {
    Set<String> tables = new HashSet<String>();
    String namespaceId = TableNamespaces.getNamespaceId(instance, namespace);
    tables.addAll(TableNamespaces.getTableNames(instance, namespaceId));
    List<DiskUsage> du = null;
    try {
      du = getTableOperations().getDiskUsage(tables);
    } catch (TableNotFoundException e) {
      log.warn("Could not find table (" + e.getTableName() + ") reference in namespace (" + namespace + ")");
    }
    return du;
  }

  private TableOperations getTableOperations() throws AccumuloException, AccumuloSecurityException {
    return new TableOperationsImpl(instance, credentials);
  }

  @Override
  public void attachIterator(String namespace, IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloSecurityException, AccumuloException,
      TableNamespaceNotFoundException {
    testClassLoad(namespace, setting.getIteratorClass(), SortedKeyValueIterator.class.getName());
    super.attachIterator(namespace, setting, scopes);
  }

  @Override
  public int addConstraint(String namespace, String constraintClassName) throws AccumuloException, AccumuloSecurityException, TableNamespaceNotFoundException {
    testClassLoad(namespace, constraintClassName, Constraint.class.getName());
    return super.addConstraint(namespace, constraintClassName);
  }

  @Override
  public boolean testClassLoad(final String namespace, final String className, final String asTypeName) throws TableNamespaceNotFoundException,
      AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(namespace, className, asTypeName);

    try {
      return ServerClient.executeRaw(instance, new ClientExecReturn<Boolean,ClientService.Client>() {
        @Override
        public Boolean execute(ClientService.Client client) throws Exception {
          return client.checkTableNamespaceClass(Tracer.traceInfo(), credentials.toThrift(instance), namespace, className, asTypeName);
        }
      });
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case NOTFOUND:
          throw new TableNamespaceNotFoundException(e);
        case OTHER:
        default:
          throw new AccumuloException(e.description, e);
      }
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (AccumuloException e) {
      throw e;
    } catch (Exception e) {
      throw new AccumuloException(e);
    }
  }
}
