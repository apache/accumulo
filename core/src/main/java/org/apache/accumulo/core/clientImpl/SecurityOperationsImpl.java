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
import static org.apache.accumulo.core.client.security.SecurityErrorCode.NAMESPACE_DOESNT_EXIST;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.DelegationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.thrift.ClientService;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.securityImpl.thrift.TDelegationToken;
import org.apache.accumulo.core.securityImpl.thrift.TDelegationTokenConfig;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.ByteBufferUtil;

public class SecurityOperationsImpl implements SecurityOperations {

  private final ClientContext context;

  private void executeVoid(ClientExec<ClientService.Client> exec)
      throws AccumuloException, AccumuloSecurityException {
    try {
      ServerClient.executeRawVoid(context, exec);
    } catch (ThriftTableOperationException ttoe) {
      // recast missing table
      if (ttoe.getType() == TableOperationExceptionType.NOTFOUND)
        throw new AccumuloSecurityException(null, SecurityErrorCode.TABLE_DOESNT_EXIST);
      else if (ttoe.getType() == TableOperationExceptionType.NAMESPACE_NOTFOUND)
        throw new AccumuloSecurityException(null, SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
      else
        throw new AccumuloException(ttoe);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (AccumuloException e) {
      throw e;
    } catch (Exception e) {
      throw new AccumuloException(e);
    }
  }

  private <T> T execute(ClientExecReturn<T,ClientService.Client> exec)
      throws AccumuloException, AccumuloSecurityException {
    try {
      return ServerClient.executeRaw(context, exec);
    } catch (ThriftTableOperationException ttoe) {
      // recast missing table
      if (ttoe.getType() == TableOperationExceptionType.NOTFOUND)
        throw new AccumuloSecurityException(null, SecurityErrorCode.TABLE_DOESNT_EXIST);
      else if (ttoe.getType() == TableOperationExceptionType.NAMESPACE_NOTFOUND)
        throw new AccumuloSecurityException(null, SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
      else
        throw new AccumuloException(ttoe);
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (AccumuloException e) {
      throw e;
    } catch (Exception e) {
      throw new AccumuloException(e);
    }
  }

  public SecurityOperationsImpl(ClientContext context) {
    checkArgument(context != null, "context is null");
    this.context = context;
  }

  @Override
  public void createLocalUser(final String principal, final PasswordToken password)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    if (context.getSaslParams() == null) {
      checkArgument(password != null, "password is null");
    }
    executeVoid(client -> {
      if (context.getSaslParams() == null) {
        client.createLocalUser(TraceUtil.traceInfo(), context.rpcCreds(), principal,
            ByteBuffer.wrap(password.getPassword()));
      } else {
        client.createLocalUser(TraceUtil.traceInfo(), context.rpcCreds(), principal,
            ByteBuffer.wrap(new byte[0]));
      }
    });
  }

  @Override
  public void dropLocalUser(final String principal)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    executeVoid(
        client -> client.dropLocalUser(TraceUtil.traceInfo(), context.rpcCreds(), principal));
  }

  @Override
  public boolean authenticateUser(final String principal, final AuthenticationToken token)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(token != null, "token is null");
    final Credentials toAuth = new Credentials(principal, token);
    return execute(client -> client.authenticateUser(TraceUtil.traceInfo(), context.rpcCreds(),
        toAuth.toThrift(context.getInstanceID())));
  }

  @Override
  public void changeLocalUserPassword(final String principal, final PasswordToken token)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(token != null, "token is null");
    final Credentials toChange = new Credentials(principal, token);
    executeVoid(client -> client.changeLocalUserPassword(TraceUtil.traceInfo(), context.rpcCreds(),
        principal, ByteBuffer.wrap(token.getPassword())));
    if (context.getCredentials().getPrincipal().equals(principal)) {
      context.setCredentials(toChange);
    }
  }

  @Override
  public void changeUserAuthorizations(final String principal, final Authorizations authorizations)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(authorizations != null, "authorizations is null");
    executeVoid(client -> client.changeAuthorizations(TraceUtil.traceInfo(), context.rpcCreds(),
        principal, ByteBufferUtil.toByteBuffers(authorizations.getAuthorizations())));
  }

  @Override
  public Authorizations getUserAuthorizations(final String principal)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    return execute(client -> new Authorizations(
        client.getUserAuthorizations(TraceUtil.traceInfo(), context.rpcCreds(), principal)));
  }

  @Override
  public boolean hasSystemPermission(final String principal, final SystemPermission perm)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(perm != null, "perm is null");
    return execute(client -> client.hasSystemPermission(TraceUtil.traceInfo(), context.rpcCreds(),
        principal, perm.getId()));
  }

  @Override
  public boolean hasTablePermission(final String principal, final String table,
      final TablePermission perm) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(table != null, "table is null");
    checkArgument(perm != null, "perm is null");
    try {
      return execute(client -> client.hasTablePermission(TraceUtil.traceInfo(), context.rpcCreds(),
          principal, table, perm.getId()));
    } catch (AccumuloSecurityException e) {
      if (e.getSecurityErrorCode() == NAMESPACE_DOESNT_EXIST)
        throw new AccumuloSecurityException(null, SecurityErrorCode.TABLE_DOESNT_EXIST, e);
      else
        throw e;
    }
  }

  @Override
  public boolean hasNamespacePermission(final String principal, final String namespace,
      final NamespacePermission permission) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(namespace != null, "namespace is null");
    checkArgument(permission != null, "permission is null");
    return execute(client -> client.hasNamespacePermission(TraceUtil.traceInfo(),
        context.rpcCreds(), principal, namespace, permission.getId()));
  }

  @Override
  public void grantSystemPermission(final String principal, final SystemPermission permission)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(permission != null, "permission is null");
    executeVoid(client -> client.grantSystemPermission(TraceUtil.traceInfo(), context.rpcCreds(),
        principal, permission.getId()));
  }

  @Override
  public void grantTablePermission(final String principal, final String table,
      final TablePermission permission) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(table != null, "table is null");
    checkArgument(permission != null, "permission is null");
    try {
      executeVoid(client -> client.grantTablePermission(TraceUtil.traceInfo(), context.rpcCreds(),
          principal, table, permission.getId()));
    } catch (AccumuloSecurityException e) {
      if (e.getSecurityErrorCode() == NAMESPACE_DOESNT_EXIST)
        throw new AccumuloSecurityException(null, SecurityErrorCode.TABLE_DOESNT_EXIST, e);
      else
        throw e;
    }
  }

  @Override
  public void grantNamespacePermission(final String principal, final String namespace,
      final NamespacePermission permission) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(namespace != null, "namespace is null");
    checkArgument(permission != null, "permission is null");
    executeVoid(client -> client.grantNamespacePermission(TraceUtil.traceInfo(), context.rpcCreds(),
        principal, namespace, permission.getId()));
  }

  @Override
  public void revokeSystemPermission(final String principal, final SystemPermission permission)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(permission != null, "permission is null");
    executeVoid(client -> client.revokeSystemPermission(TraceUtil.traceInfo(), context.rpcCreds(),
        principal, permission.getId()));
  }

  @Override
  public void revokeTablePermission(final String principal, final String table,
      final TablePermission permission) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(table != null, "table is null");
    checkArgument(permission != null, "permission is null");
    try {
      executeVoid(client -> client.revokeTablePermission(TraceUtil.traceInfo(), context.rpcCreds(),
          principal, table, permission.getId()));
    } catch (AccumuloSecurityException e) {
      if (e.getSecurityErrorCode() == NAMESPACE_DOESNT_EXIST)
        throw new AccumuloSecurityException(null, SecurityErrorCode.TABLE_DOESNT_EXIST, e);
      else
        throw e;
    }
  }

  @Override
  public void revokeNamespacePermission(final String principal, final String namespace,
      final NamespacePermission permission) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(namespace != null, "namespace is null");
    checkArgument(permission != null, "permission is null");
    executeVoid(client -> client.revokeNamespacePermission(TraceUtil.traceInfo(),
        context.rpcCreds(), principal, namespace, permission.getId()));
  }

  @Override
  public Set<String> listLocalUsers() throws AccumuloException, AccumuloSecurityException {
    return execute(client -> client.listLocalUsers(TraceUtil.traceInfo(), context.rpcCreds()));
  }

  @Override
  public DelegationToken getDelegationToken(DelegationTokenConfig cfg)
      throws AccumuloException, AccumuloSecurityException {
    final TDelegationTokenConfig tConfig;
    if (cfg != null) {
      tConfig = DelegationTokenConfigSerializer.serialize(cfg);
    } else {
      tConfig = new TDelegationTokenConfig();
    }

    TDelegationToken thriftToken;
    try {
      thriftToken = MasterClient.execute(context,
          client -> client.getDelegationToken(TraceUtil.traceInfo(), context.rpcCreds(), tConfig));
    } catch (TableNotFoundException e) {
      // should never happen
      throw new AssertionError(
          "Received TableNotFoundException on method which should not throw that exception", e);
    }

    AuthenticationTokenIdentifier identifier =
        new AuthenticationTokenIdentifier(thriftToken.getIdentifier());

    // Get the password out of the thrift delegation token
    return new DelegationTokenImpl(thriftToken.getPassword(), identifier);
  }

}
