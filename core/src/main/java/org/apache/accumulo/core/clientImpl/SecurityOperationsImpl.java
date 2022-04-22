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
import static org.apache.accumulo.core.util.Validators.EXISTING_NAMESPACE_NAME;

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
import org.apache.accumulo.core.clientImpl.thrift.ClientService.Client;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.rpc.ThriftClientTypes;
import org.apache.accumulo.core.rpc.ThriftClientTypes.ThriftClientType.Exec;
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

  private <R> R exec(Exec<R,ClientService.Client> exec)
      throws AccumuloException, AccumuloSecurityException {
    try {
      return ThriftClientTypes.CLIENT.executeOnTServer(context, client -> {
        return exec.execute(client);
      });
    } catch (AccumuloException e) {
      Throwable t = e.getCause();
      if (t instanceof ThriftTableOperationException) {
        ThriftTableOperationException ttoe = (ThriftTableOperationException) t;
        // recast missing table
        if (ttoe.getType() == TableOperationExceptionType.NOTFOUND)
          throw new AccumuloSecurityException(null, SecurityErrorCode.TABLE_DOESNT_EXIST);
        else if (ttoe.getType() == TableOperationExceptionType.NAMESPACE_NOTFOUND)
          throw new AccumuloSecurityException(null, SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
        else
          throw e;
      }
      throw e;
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
    exec(new ThriftClientTypes.ThriftClientType.Exec<Void,ClientService.Client>() {
      @Override
      public Void execute(Client client) throws Exception {
        if (context.getSaslParams() == null) {
          client.createLocalUser(TraceUtil.traceInfo(), context.rpcCreds(), principal,
              ByteBuffer.wrap(password.getPassword()));
        } else {
          client.createLocalUser(TraceUtil.traceInfo(), context.rpcCreds(), principal,
              ByteBuffer.wrap(new byte[0]));
        }
        return null;
      }
    });
  }

  @Override
  public void dropLocalUser(final String principal)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");

    exec(new ThriftClientTypes.ThriftClientType.Exec<Void,ClientService.Client>() {
      @Override
      public Void execute(Client client) throws Exception {
        client.dropLocalUser(TraceUtil.traceInfo(), context.rpcCreds(), principal);
        return null;
      }
    });
  }

  @Override
  public boolean authenticateUser(final String principal, final AuthenticationToken token)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(token != null, "token is null");

    final Credentials toAuth = new Credentials(principal, token);
    return exec(new ThriftClientTypes.ThriftClientType.Exec<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(Client client) throws Exception {
        return client.authenticateUser(TraceUtil.traceInfo(), context.rpcCreds(),
            toAuth.toThrift(context.getInstanceID()));
      }
    });
  }

  @Override
  public void changeLocalUserPassword(final String principal, final PasswordToken token)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(token != null, "token is null");

    final Credentials toChange = new Credentials(principal, token);
    exec(new ThriftClientTypes.ThriftClientType.Exec<Void,ClientService.Client>() {
      @Override
      public Void execute(Client client) throws Exception {
        client.changeLocalUserPassword(TraceUtil.traceInfo(), context.rpcCreds(), principal,
            ByteBuffer.wrap(token.getPassword()));
        return null;
      }
    });
    if (context.getCredentials().getPrincipal().equals(principal)) {
      context.setCredentials(toChange);
    }
  }

  @Override
  public void changeUserAuthorizations(final String principal, final Authorizations authorizations)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(authorizations != null, "authorizations is null");

    exec(new ThriftClientTypes.ThriftClientType.Exec<Void,ClientService.Client>() {
      @Override
      public Void execute(Client client) throws Exception {
        client.changeAuthorizations(TraceUtil.traceInfo(), context.rpcCreds(), principal,
            ByteBufferUtil.toByteBuffers(authorizations.getAuthorizations()));
        return null;
      }
    });
  }

  @Override
  public Authorizations getUserAuthorizations(final String principal)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");

    return exec(new ThriftClientTypes.ThriftClientType.Exec<Authorizations,ClientService.Client>() {
      @Override
      public Authorizations execute(Client client) throws Exception {
        return new Authorizations(
            client.getUserAuthorizations(TraceUtil.traceInfo(), context.rpcCreds(), principal));
      }
    });
  }

  @Override
  public boolean hasSystemPermission(final String principal, final SystemPermission perm)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(perm != null, "perm is null");

    return exec(new ThriftClientTypes.ThriftClientType.Exec<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(Client client) throws Exception {
        return client.hasSystemPermission(TraceUtil.traceInfo(), context.rpcCreds(), principal,
            perm.getId());
      }
    });
  }

  @Override
  public boolean hasTablePermission(final String principal, final String table,
      final TablePermission perm) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(table != null, "table is null");
    checkArgument(perm != null, "perm is null");

    try {
      return exec(new ThriftClientTypes.ThriftClientType.Exec<Boolean,ClientService.Client>() {
        @Override
        public Boolean execute(Client client) throws Exception {
          return client.hasTablePermission(TraceUtil.traceInfo(), context.rpcCreds(), principal,
              table, perm.getId());
        }
      });
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
    EXISTING_NAMESPACE_NAME.validate(namespace);
    checkArgument(permission != null, "permission is null");

    return exec(new ThriftClientTypes.ThriftClientType.Exec<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(Client client) throws Exception {
        return client.hasNamespacePermission(TraceUtil.traceInfo(), context.rpcCreds(), principal,
            namespace, permission.getId());
      }
    });
  }

  @Override
  public void grantSystemPermission(final String principal, final SystemPermission permission)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(permission != null, "permission is null");

    exec(new ThriftClientTypes.ThriftClientType.Exec<Void,ClientService.Client>() {
      @Override
      public Void execute(Client client) throws Exception {
        client.grantSystemPermission(TraceUtil.traceInfo(), context.rpcCreds(), principal,
            permission.getId());
        return null;
      }
    });
  }

  @Override
  public void grantTablePermission(final String principal, final String table,
      final TablePermission permission) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(table != null, "table is null");
    checkArgument(permission != null, "permission is null");

    try {
      exec(new ThriftClientTypes.ThriftClientType.Exec<Void,ClientService.Client>() {
        @Override
        public Void execute(Client client) throws Exception {
          client.grantTablePermission(TraceUtil.traceInfo(), context.rpcCreds(), principal, table,
              permission.getId());
          return null;
        }
      });
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
    EXISTING_NAMESPACE_NAME.validate(namespace);
    checkArgument(permission != null, "permission is null");

    exec(new ThriftClientTypes.ThriftClientType.Exec<Void,ClientService.Client>() {
      @Override
      public Void execute(Client client) throws Exception {
        client.grantNamespacePermission(TraceUtil.traceInfo(), context.rpcCreds(), principal,
            namespace, permission.getId());
        return null;
      }

    });
  }

  @Override
  public void revokeSystemPermission(final String principal, final SystemPermission permission)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(permission != null, "permission is null");

    exec(new ThriftClientTypes.ThriftClientType.Exec<Void,ClientService.Client>() {
      @Override
      public Void execute(Client client) throws Exception {
        client.revokeSystemPermission(TraceUtil.traceInfo(), context.rpcCreds(), principal,
            permission.getId());
        return null;
      }
    });
  }

  @Override
  public void revokeTablePermission(final String principal, final String table,
      final TablePermission permission) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(table != null, "table is null");
    checkArgument(permission != null, "permission is null");

    try {
      exec(new ThriftClientTypes.ThriftClientType.Exec<Void,ClientService.Client>() {
        @Override
        public Void execute(Client client) throws Exception {
          client.revokeTablePermission(TraceUtil.traceInfo(), context.rpcCreds(), principal, table,
              permission.getId());
          return null;
        }
      });
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
    EXISTING_NAMESPACE_NAME.validate(namespace);
    checkArgument(permission != null, "permission is null");

    exec(new ThriftClientTypes.ThriftClientType.Exec<Void,ClientService.Client>() {
      @Override
      public Void execute(Client client) throws Exception {
        client.revokeNamespacePermission(TraceUtil.traceInfo(), context.rpcCreds(), principal,
            namespace, permission.getId());
        return null;
      }
    });
  }

  @Override
  public Set<String> listLocalUsers() throws AccumuloException, AccumuloSecurityException {
    return exec(new ThriftClientTypes.ThriftClientType.Exec<Set<String>,ClientService.Client>() {
      @Override
      public Set<String> execute(Client client) throws Exception {
        return client.listLocalUsers(TraceUtil.traceInfo(), context.rpcCreds());
      }
    });
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
      thriftToken = ThriftClientTypes.MANAGER.executeOnManager(context, client -> {
        return client.getDelegationToken(TraceUtil.traceInfo(), context.rpcCreds(), tConfig);
      });
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
