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
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.ServerClient;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
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
 * Provides a class for administering namespaces
 * 
 */
public class NamespaceOperationsImpl extends NamespaceOperationsHelper {
  private Instance instance;
  private Credentials credentials;

  private static final Logger log = Logger.getLogger(TableOperations.class);

  /**
   * @param instance
   *          the connection information for this instance
   * @param credentials
   *          the username/password for this connection
   */
  public NamespaceOperationsImpl(Instance instance, Credentials credentials) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.credentials = credentials;
  }

  /**
   * Retrieve a list of namespaces in Accumulo.
   * 
   * @return List of namespaces in accumulo
   */
  @Override
  public SortedSet<String> list() {
    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Fetching list of namespaces...");
    TreeSet<String> namespaces = new TreeSet<String>(Namespaces.getNameToIdMap(instance).keySet());
    opTimer.stop("Fetched " + namespaces.size() + " namespaces in %DURATION%");
    return namespaces;
  }

  /**
   * A method to check if a namespace exists in Accumulo.
   * 
   * @param namespace
   *          the name of the namespace
   * @return true if the namespace exists
   */
  @Override
  public boolean exists(String namespace) {
    ArgumentChecker.notNull(namespace);

    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Checking if namespace " + namespace + " exists...");
    boolean exists = Namespaces.getNameToIdMap(instance).containsKey(namespace);
    opTimer.stop("Checked existance of " + exists + " in %DURATION%");
    return exists;
  }

  /**
   * Create a namespace with no special configuration
   * 
   * @param namespace
   *          the name of the namespace
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceExistsException
   *           if the namespace already exists
   */
  @Override
  public void create(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceExistsException {
    create(namespace, true, TimeType.MILLIS);
  }

  /**
   * @param namespace
   *          the name of the namespace
   * @param limitVersion
   *          Enables/disables the versioning iterator, which will limit the number of Key versions kept.
   */
  @Override
  public void create(String namespace, boolean limitVersion) throws AccumuloException, AccumuloSecurityException, NamespaceExistsException {
    create(namespace, limitVersion, TimeType.MILLIS);
  }

  /**
   * @param namespace
   *          the name of the namespace
   * @param timeType
   *          specifies logical or real-time based time recording for entries in the table
   * @param limitVersion
   *          Enables/disables the versioning iterator, which will limit the number of Key versions kept.
   */
  @Override
  public void create(String namespace, boolean limitVersion, TimeType timeType) throws AccumuloException, AccumuloSecurityException, NamespaceExistsException {
    ArgumentChecker.notNull(namespace, timeType);

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(namespace.getBytes()), ByteBuffer.wrap(timeType.name().getBytes()));

    Map<String,String> opts = IteratorUtil.generateInitialTableProperties(limitVersion);

    try {
      doNamespaceOperation(TableOperation.CREATE, args, opts);
    } catch (NamespaceNotFoundException e1) {
      // should not happen
      throw new RuntimeException(e1);
    }
  }

  private long beginNamespaceOperation() throws ThriftSecurityException, TException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        return client.beginNamespaceOperation(Tracer.traceInfo(), credentials.toThrift(instance));
      } catch (TTransportException tte) {
        log.debug("Failed to call beginNamespaceOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }

  private void executeNamespaceOperation(long opid, TableOperation op, List<ByteBuffer> args, Map<String,String> opts, boolean autoCleanUp)
      throws ThriftSecurityException, TException, ThriftTableOperationException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        client.executeNamespaceOperation(Tracer.traceInfo(), credentials.toThrift(instance), opid, op, args, opts, autoCleanUp);
        break;
      } catch (TTransportException tte) {
        log.debug("Failed to call executeTableOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }

  private String waitForNamespaceOperation(long opid) throws ThriftSecurityException, TException, ThriftTableOperationException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        return client.waitForNamespaceOperation(Tracer.traceInfo(), credentials.toThrift(instance), opid);
      } catch (TTransportException tte) {
        log.debug("Failed to call waitForTableOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }

  private void finishNamespaceOperation(long opid) throws ThriftSecurityException, TException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        client.finishNamespaceOperation(Tracer.traceInfo(), credentials.toThrift(instance), opid);
        break;
      } catch (TTransportException tte) {
        log.debug("Failed to call finishTableOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }

  private String doNamespaceOperation(TableOperation op, List<ByteBuffer> args, Map<String,String> opts) throws AccumuloSecurityException,
      NamespaceExistsException, NamespaceNotFoundException, AccumuloException {
    return doNamespaceOperation(op, args, opts, true);
  }

  private String doNamespaceOperation(TableOperation op, List<ByteBuffer> args, Map<String,String> opts, boolean wait) throws AccumuloSecurityException,
      NamespaceExistsException, NamespaceNotFoundException, AccumuloException {
    Long opid = null;

    try {
      opid = beginNamespaceOperation();
      executeNamespaceOperation(opid, op, args, opts, !wait);
      if (!wait) {
        opid = null;
        return null;
      }
      String ret = waitForNamespaceOperation(opid);
      Tables.clearCache(instance);
      return ret;
    } catch (ThriftSecurityException e) {
      String tableName = ByteBufferUtil.toString(args.get(0));
      String tableInfo = Tables.getPrintableTableInfoFromName(instance, tableName);
      throw new AccumuloSecurityException(e.user, e.code, tableInfo, e);
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case EXISTS:
          throw new NamespaceExistsException(e);
        case NOTFOUND:
          throw new NamespaceNotFoundException(e);
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
          finishNamespaceOperation(opid);
        } catch (Exception e) {
          log.warn(e.getMessage(), e);
        }
    }
  }

  /**
   * Delete a namespace if empty
   * 
   * @param namespace
   *          the name of the namespace
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the namespace does not exist
   * @throws NamespaceNotEmptyException
   *           if the namespaces still contains tables
   * @throws TableNotFoundException
   *           if table not found while deleting
   */
  @Override
  public void delete(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException, NamespaceNotEmptyException,
      TableNotFoundException {
    delete(namespace, false);
  }

  /**
   * Delete a namespace
   * 
   * @param namespace
   *          the name of the namespace
   * @param deleteTables
   *          boolean, if true deletes all the tables in the namespace in addition to deleting the namespace.
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the namespace does not exist
   * @throws NamespaceNotEmptyException
   *           if the namespaces still contains tables
   * @throws TableNotFoundException
   *           if table not found while deleting
   */
  @Override
  public void delete(String namespace, boolean deleteTables) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException,
      NamespaceNotEmptyException, TableNotFoundException {
    ArgumentChecker.notNull(namespace);
    String namespaceId = Namespaces.getNamespaceId(instance, namespace);

    if (namespaceId.equals(Constants.SYSTEM_NAMESPACE_ID) || namespaceId.equals(Constants.DEFAULT_NAMESPACE_ID)) {
      log.debug(credentials.getPrincipal() + " attempted to delete the " + namespaceId + " namespace");
      throw new AccumuloSecurityException(credentials.getPrincipal(), SecurityErrorCode.UNSUPPORTED_OPERATION);
    }

    if (Namespaces.getTableIds(instance, namespaceId).size() > 0) {
      if (!deleteTables) {
        throw new NamespaceNotEmptyException(namespaceId, namespace, null);
      }
      for (String table : Namespaces.getTableNames(instance, namespaceId)) {
        try {
          getTableOperations().delete(table);
        } catch (TableNotFoundException e) {
          log.debug("Table (" + table + ") not found while deleting namespace, probably deleted while we were deleting the rest of the tables");
        }
      }
    }

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(namespace.getBytes()));
    Map<String,String> opts = new HashMap<String,String>();

    try {
      doNamespaceOperation(TableOperation.DELETE, args, opts);
    } catch (NamespaceExistsException e) {
      // should not happen
      throw new RuntimeException(e);
    }

  }

  /**
   * Rename a namespace
   * 
   * @param oldNamespaceName
   *          the old namespace
   * @param newNamespaceName
   *          the new namespace
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws NamespaceNotFoundException
   *           if the old namespace name does not exist
   * @throws NamespaceExistsException
   *           if the new namespace name already exists
   */
  @Override
  public void rename(String oldNamespaceName, String newNamespaceName) throws AccumuloSecurityException, NamespaceNotFoundException, AccumuloException,
      NamespaceExistsException {

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(oldNamespaceName.getBytes()), ByteBuffer.wrap(newNamespaceName.getBytes()));
    Map<String,String> opts = new HashMap<String,String>();
    doNamespaceOperation(TableOperation.RENAME, args, opts);
  }

  /**
   * Sets a property on a namespace which will apply to all tables in the namespace
   * 
   * @param namespace
   *          the name of the namespace
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
        client.setNamespaceProperty(Tracer.traceInfo(), credentials.toThrift(instance), namespace, property, value);
      }
    });
  }

  /**
   * Removes a property from a namespace
   * 
   * @param namespace
   *          the name of the namespace
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
        client.removeNamespaceProperty(Tracer.traceInfo(), credentials.toThrift(instance), namespace, property);
      }
    });
  }

  /**
   * Gets properties of a namespace
   * 
   * @param namespace
   *          the name of the namespace
   * @return all properties visible by this namespace (system and per-namespace properties)
   * @throws NamespaceNotFoundException
   *           if the namespace does not exist
   */
  @Override
  public Iterable<Entry<String,String>> getProperties(final String namespace) throws AccumuloException, NamespaceNotFoundException {
    ArgumentChecker.notNull(namespace);
    try {
      return ServerClient.executeRaw(instance, new ClientExecReturn<Map<String,String>,ClientService.Client>() {
        @Override
        public Map<String,String> execute(ClientService.Client client) throws Exception {
          return client.getNamespaceConfiguration(Tracer.traceInfo(), credentials.toThrift(instance), namespace);
        }
      }).entrySet();
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case NOTFOUND:
          throw new NamespaceNotFoundException(e);
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
   *          the namespace to take offline
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws NamespaceNotFoundException
   *           if the namespace does not exist
   */
  @Override
  public void offline(String namespace) throws AccumuloSecurityException, AccumuloException, NamespaceNotFoundException {

    ArgumentChecker.notNull(namespace);
    String namespaceId = Namespaces.getNamespaceId(instance, namespace);
    try {
      for (String table : Namespaces.getTableNames(instance, namespaceId)) {
        getTableOperations().offline(table);
      }
    } catch (TableNotFoundException e) {
      Log.error("Namespace (" + namespaceId + ") contains reference to table that doesn't exist");
    }
  }

  /**
   * 
   * @param namespace
   *          the namespace to take online
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws NamespaceNotFoundException
   *           if the namespace does not exist
   */
  @Override
  public void online(String namespace) throws AccumuloSecurityException, AccumuloException, NamespaceNotFoundException {
    ArgumentChecker.notNull(namespace);
    String namespaceId = Namespaces.getNamespaceId(instance, namespace);
    try {
      for (String table : Namespaces.getTableNames(instance, namespaceId)) {
        getTableOperations().online(table);
      }
    } catch (TableNotFoundException e) {
      Log.warn("Namespace (" + namespaceId + ") contains a reference to a table that doesn't exist");
    }
  }

  /**
   * Get a mapping of namespace name to internal namespace id.
   * 
   * @return the map from namespace name to internal namespace id
   */
  @Override
  public Map<String,String> namespaceIdMap() {
    return Namespaces.getNameToIdMap(instance);
  }

  @Override
  public List<DiskUsage> getDiskUsage(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    Set<String> tables = new HashSet<String>();
    String namespaceId = Namespaces.getNamespaceId(instance, namespace);
    tables.addAll(Namespaces.getTableNames(instance, namespaceId));
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
      NamespaceNotFoundException {
    testClassLoad(namespace, setting.getIteratorClass(), SortedKeyValueIterator.class.getName());
    super.attachIterator(namespace, setting, scopes);
  }

  @Override
  public int addConstraint(String namespace, String constraintClassName) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    testClassLoad(namespace, constraintClassName, Constraint.class.getName());
    return super.addConstraint(namespace, constraintClassName);
  }

  @Override
  public boolean testClassLoad(final String namespace, final String className, final String asTypeName) throws NamespaceNotFoundException, AccumuloException,
      AccumuloSecurityException {
    ArgumentChecker.notNull(namespace, className, asTypeName);

    try {
      return ServerClient.executeRaw(instance, new ClientExecReturn<Boolean,ClientService.Client>() {
        @Override
        public Boolean execute(ClientService.Client client) throws Exception {
          return client.checkNamespaceClass(Tracer.traceInfo(), credentials.toThrift(instance), namespace, className, asTypeName);
        }
      });
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case NOTFOUND:
          throw new NamespaceNotFoundException(e);
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
