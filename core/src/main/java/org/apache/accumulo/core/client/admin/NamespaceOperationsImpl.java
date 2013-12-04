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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
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

public class NamespaceOperationsImpl extends NamespaceOperationsHelper {
  private Instance instance;
  private Credentials credentials;

  private static final Logger log = Logger.getLogger(TableOperations.class);

  public NamespaceOperationsImpl(Instance instance, Credentials credentials) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.credentials = credentials;
  }

  @Override
  public SortedSet<String> list() {
    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Fetching list of namespaces...");
    TreeSet<String> namespaces = new TreeSet<String>(Namespaces.getNameToIdMap(instance).keySet());
    opTimer.stop("Fetched " + namespaces.size() + " namespaces in %DURATION%");
    return namespaces;
  }

  @Override
  public boolean exists(String namespace) {
    ArgumentChecker.notNull(namespace);

    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Checking if namespace " + namespace + " exists...");
    boolean exists = Namespaces.getNameToIdMap(instance).containsKey(namespace);
    opTimer.stop("Checked existance of " + exists + " in %DURATION%");
    return exists;
  }

  @Override
  public void create(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceExistsException {
    ArgumentChecker.notNull(namespace);

    try {
      doNamespaceOperation(TableOperation.CREATE, Arrays.asList(ByteBuffer.wrap(namespace.getBytes())), Collections.<String,String> emptyMap());
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

  @Override
  public void delete(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException, NamespaceNotEmptyException {
    ArgumentChecker.notNull(namespace);
    String namespaceId = Namespaces.getNamespaceId(instance, namespace);

    if (namespaceId.equals(Constants.ACCUMULO_NAMESPACE_ID) || namespaceId.equals(Constants.DEFAULT_NAMESPACE_ID)) {
      log.debug(credentials.getPrincipal() + " attempted to delete the " + namespaceId + " namespace");
      throw new AccumuloSecurityException(credentials.getPrincipal(), SecurityErrorCode.UNSUPPORTED_OPERATION);
    }

    if (Namespaces.getTableIds(instance, namespaceId).size() > 0) {
      throw new NamespaceNotEmptyException(namespaceId, namespace, null);
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

  @Override
  public void rename(String oldNamespaceName, String newNamespaceName) throws AccumuloSecurityException, NamespaceNotFoundException, AccumuloException,
      NamespaceExistsException {

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(oldNamespaceName.getBytes()), ByteBuffer.wrap(newNamespaceName.getBytes()));
    Map<String,String> opts = new HashMap<String,String>();
    doNamespaceOperation(TableOperation.RENAME, args, opts);
  }

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

  @Override
  public Map<String,String> namespaceIdMap() {
    return Namespaces.getNameToIdMap(instance);
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
