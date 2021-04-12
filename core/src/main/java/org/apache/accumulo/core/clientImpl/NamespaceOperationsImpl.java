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
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.Constants.MAX_NAMESPACE_LEN;
import static org.apache.accumulo.core.Constants.MAX_TABLE_NAME_LEN;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotEmptyException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.manager.thrift.FateOperation;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.accumulo.core.util.OpTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceOperationsImpl extends NamespaceOperationsHelper {
  private final ClientContext context;
  private TableOperationsImpl tableOps;

  private static final Logger log = LoggerFactory.getLogger(TableOperations.class);

  public NamespaceOperationsImpl(ClientContext context, TableOperationsImpl tableOps) {
    checkArgument(context != null, "context is null");
    this.context = context;
    this.tableOps = tableOps;
  }

  @Override
  public SortedSet<String> list() {

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Fetching list of namespaces...", Thread.currentThread().getId());
      timer = new OpTimer().start();
    }

    TreeSet<String> namespaces = new TreeSet<>(Namespaces.getNameToIdMap(context).keySet());

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Fetched {} namespaces in {}", Thread.currentThread().getId(),
          namespaces.size(), String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
    }

    return namespaces;
  }

  @Override
  public boolean exists(String namespace) {
    checkArgument(namespace != null, "namespace is null");

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Checking if namespace {} exists", Thread.currentThread().getId(),
          namespace);
      timer = new OpTimer().start();
    }

    boolean exists = Namespaces.namespaceNameExists(context, namespace);

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Checked existance of {} in {}", Thread.currentThread().getId(), exists,
          String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
    }

    return exists;
  }

  @Override
  public void create(String namespace)
      throws AccumuloException, AccumuloSecurityException, NamespaceExistsException {
    checkArgument(namespace != null, "namespace is null");
    checkArgument(namespace.length() <= MAX_NAMESPACE_LEN,
        "Namespace is longer than " + MAX_NAMESPACE_LEN + " characters");
    try {
      doNamespaceFateOperation(FateOperation.NAMESPACE_CREATE,
          Arrays.asList(ByteBuffer.wrap(namespace.getBytes(UTF_8))), Collections.emptyMap(),
          namespace);
    } catch (NamespaceNotFoundException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  @Override
  public void delete(String namespace) throws AccumuloException, AccumuloSecurityException,
      NamespaceNotFoundException, NamespaceNotEmptyException {
    checkArgument(namespace != null, "namespace is null");
    NamespaceId namespaceId = Namespaces.getNamespaceId(context, namespace);

    if (namespaceId.equals(Namespace.ACCUMULO.id()) || namespaceId.equals(Namespace.DEFAULT.id())) {
      Credentials credentials = context.getCredentials();
      log.debug("{} attempted to delete the {} namespace", credentials.getPrincipal(), namespaceId);
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.UNSUPPORTED_OPERATION);
    }

    if (!Namespaces.getTableIds(context, namespaceId).isEmpty()) {
      throw new NamespaceNotEmptyException(namespaceId.canonical(), namespace, null);
    }

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(namespace.getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<>();

    try {
      doNamespaceFateOperation(FateOperation.NAMESPACE_DELETE, args, opts, namespace);
    } catch (NamespaceExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }

  }

  @Override
  public void rename(String oldNamespaceName, String newNamespaceName)
      throws AccumuloSecurityException, NamespaceNotFoundException, AccumuloException,
      NamespaceExistsException {
    checkArgument(newNamespaceName.length() <= MAX_TABLE_NAME_LEN,
        "Namespace is longer than " + MAX_TABLE_NAME_LEN + " characters");
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(oldNamespaceName.getBytes(UTF_8)),
        ByteBuffer.wrap(newNamespaceName.getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<>();
    doNamespaceFateOperation(FateOperation.NAMESPACE_RENAME, args, opts, oldNamespaceName);
  }

  @Override
  public void setProperty(final String namespace, final String property, final String value)
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    checkArgument(namespace != null, "namespace is null");
    checkArgument(property != null, "property is null");
    checkArgument(value != null, "value is null");

    ManagerClient.executeNamespace(context,
        client -> client.setNamespaceProperty(TraceUtil.traceInfo(), context.rpcCreds(), namespace,
            property, value));
    checkLocalityGroups(namespace, property);
  }

  @Override
  public void removeProperty(final String namespace, final String property)
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    checkArgument(namespace != null, "namespace is null");
    checkArgument(property != null, "property is null");

    ManagerClient.executeNamespace(context, client -> client
        .removeNamespaceProperty(TraceUtil.traceInfo(), context.rpcCreds(), namespace, property));
    checkLocalityGroups(namespace, property);
  }

  @Override
  public Iterable<Entry<String,String>> getProperties(final String namespace)
      throws AccumuloException, NamespaceNotFoundException {
    checkArgument(namespace != null, "namespace is null");
    try {
      return ServerClient.executeRaw(context, client -> client
          .getNamespaceConfiguration(TraceUtil.traceInfo(), context.rpcCreds(), namespace))
          .entrySet();
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
    return Namespaces.getNameToIdMap(context).entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().canonical(), (v1, v2) -> {
          throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2));
        }, TreeMap::new));
  }

  @Override
  public boolean testClassLoad(final String namespace, final String className,
      final String asTypeName)
      throws NamespaceNotFoundException, AccumuloException, AccumuloSecurityException {
    checkArgument(namespace != null, "namespace is null");
    checkArgument(className != null, "className is null");
    checkArgument(asTypeName != null, "asTypeName is null");

    try {
      return ServerClient.executeRaw(context,
          client -> client.checkNamespaceClass(TraceUtil.traceInfo(), context.rpcCreds(), namespace,
              className, asTypeName));
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
  public void attachIterator(String namespace, IteratorSetting setting,
      EnumSet<IteratorScope> scopes)
      throws AccumuloSecurityException, AccumuloException, NamespaceNotFoundException {
    testClassLoad(namespace, setting.getIteratorClass(), SortedKeyValueIterator.class.getName());
    super.attachIterator(namespace, setting, scopes);
  }

  @Override
  public int addConstraint(String namespace, String constraintClassName)
      throws AccumuloException, AccumuloSecurityException, NamespaceNotFoundException {
    testClassLoad(namespace, constraintClassName, Constraint.class.getName());
    return super.addConstraint(namespace, constraintClassName);
  }

  private String doNamespaceFateOperation(FateOperation op, List<ByteBuffer> args,
      Map<String,String> opts, String namespace) throws AccumuloSecurityException,
      AccumuloException, NamespaceExistsException, NamespaceNotFoundException {
    try {
      return tableOps.doFateOperation(op, args, opts, namespace);
    } catch (TableExistsException | TableNotFoundException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  private void checkLocalityGroups(String namespace, String propChanged)
      throws AccumuloException, NamespaceNotFoundException {
    if (LocalityGroupUtil.isLocalityGroupProperty(propChanged)) {
      Iterable<Entry<String,String>> allProps = getProperties(namespace);
      try {
        LocalityGroupUtil.checkLocalityGroups(allProps);
      } catch (LocalityGroupConfigurationError | RuntimeException e) {
        LoggerFactory.getLogger(this.getClass()).warn("Changing '" + propChanged
            + "' for namespace '" + namespace
            + "'resulted in bad locality group config. This may be a transient situation since the"
            + " config spreads over multiple properties. Setting properties in a different order "
            + "may help. Even though this warning was displayed, the property was updated. Please "
            + "check your config to ensure consistency.", e);
      }
    }
  }
}
