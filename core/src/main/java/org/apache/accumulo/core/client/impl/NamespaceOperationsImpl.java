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
package org.apache.accumulo.core.client.impl;

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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.master.thrift.FateOperation;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class NamespaceOperationsImpl extends NamespaceOperationsHelper {
  private Instance instance;
  private Credentials credentials;
  private TableOperationsImpl tableOps;

  private static final Logger log = Logger.getLogger(TableOperations.class);

  public NamespaceOperationsImpl(Instance instance, Credentials credentials, TableOperationsImpl tableOps) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.credentials = credentials;
    this.tableOps = tableOps;
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
      doNamespaceFateOperation(FateOperation.NAMESPACE_CREATE, Arrays.asList(ByteBuffer.wrap(namespace.getBytes())), Collections.<String,String> emptyMap());
    } catch (NamespaceNotFoundException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  @Override
  public void delete(String namespace) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException, NamespaceNotEmptyException {
    ArgumentChecker.notNull(namespace);
    String namespaceId = Namespaces.getNamespaceId(instance, namespace);

    if (namespaceId.equals(Namespaces.ACCUMULO_NAMESPACE_ID) || namespaceId.equals(Namespaces.DEFAULT_NAMESPACE_ID)) {
      log.debug(credentials.getPrincipal() + " attempted to delete the " + namespaceId + " namespace");
      throw new AccumuloSecurityException(credentials.getPrincipal(), SecurityErrorCode.UNSUPPORTED_OPERATION);
    }

    if (Namespaces.getTableIds(instance, namespaceId).size() > 0) {
      throw new NamespaceNotEmptyException(namespaceId, namespace, null);
    }

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(namespace.getBytes()));
    Map<String,String> opts = new HashMap<String,String>();

    try {
      doNamespaceFateOperation(FateOperation.NAMESPACE_DELETE, args, opts);
    } catch (NamespaceExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }

  }

  @Override
  public void rename(String oldNamespaceName, String newNamespaceName) throws AccumuloSecurityException, NamespaceNotFoundException, AccumuloException,
      NamespaceExistsException {

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(oldNamespaceName.getBytes()), ByteBuffer.wrap(newNamespaceName.getBytes()));
    Map<String,String> opts = new HashMap<String,String>();
    doNamespaceFateOperation(FateOperation.NAMESPACE_RENAME, args, opts);
  }

  @Override
  public void setProperty(final String namespace, final String property, final String value) throws AccumuloException, AccumuloSecurityException,
      NamespaceNotFoundException {
    ArgumentChecker.notNull(namespace, property, value);

    MasterClient.executeNamespace(instance, new ClientExec<MasterClientService.Client>() {
      @Override
      public void execute(MasterClientService.Client client) throws Exception {
        client.setNamespaceProperty(Tracer.traceInfo(), credentials.toThrift(instance), namespace, property, value);
      }
    });
  }

  @Override
  public void removeProperty(final String namespace, final String property) throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    ArgumentChecker.notNull(namespace, property);

    MasterClient.executeNamespace(instance, new ClientExec<MasterClientService.Client>() {
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
        case NAMESPACE_NOTFOUND:
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
        case NAMESPACE_NOTFOUND:
          throw new NamespaceNotFoundException(e);
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

  private String doNamespaceFateOperation(FateOperation op, List<ByteBuffer> args, Map<String,String> opts) throws AccumuloSecurityException,
      AccumuloException, NamespaceExistsException, NamespaceNotFoundException {
    try {
      return tableOps.doFateOperation(op, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    } catch (TableNotFoundException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }
}
