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

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.DelegationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.master.thrift.MasterClientService.Client;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TDelegationToken;
import org.apache.accumulo.core.security.thrift.TDelegationTokenConfig;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.ByteBufferUtil;

public class SecurityOperationsImpl implements SecurityOperations {

  private final ClientContext context;

  private void executeVoid(ClientExec<ClientService.Client> exec) throws AccumuloException, AccumuloSecurityException {
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

  private <T> T execute(ClientExecReturn<T,ClientService.Client> exec) throws AccumuloException, AccumuloSecurityException {
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

  @Deprecated
  @Override
  public void createUser(String user, byte[] password, final Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    createLocalUser(user, new PasswordToken(password));
    changeUserAuthorizations(user, authorizations);
  }

  @Override
  public void createLocalUser(final String principal, final PasswordToken password) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    if (null == context.getSaslParams()) {
      checkArgument(password != null, "password is null");
    }
    executeVoid(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        if (null == context.getSaslParams()) {
          client.createLocalUser(Tracer.traceInfo(), context.rpcCreds(), principal, ByteBuffer.wrap(password.getPassword()));
        } else {
          client.createLocalUser(Tracer.traceInfo(), context.rpcCreds(), principal, ByteBuffer.wrap(new byte[0]));
        }
      }
    });
  }

  @Deprecated
  @Override
  public void dropUser(final String user) throws AccumuloException, AccumuloSecurityException {
    dropLocalUser(user);
  }

  @Override
  public void dropLocalUser(final String principal) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    executeVoid(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.dropLocalUser(Tracer.traceInfo(), context.rpcCreds(), principal);
      }
    });
  }

  @Deprecated
  @Override
  public boolean authenticateUser(String user, byte[] password) throws AccumuloException, AccumuloSecurityException {
    return authenticateUser(user, new PasswordToken(password));
  }

  @Override
  public boolean authenticateUser(final String principal, final AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(token != null, "token is null");
    final Credentials toAuth = new Credentials(principal, token);
    return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.authenticateUser(Tracer.traceInfo(), context.rpcCreds(), toAuth.toThrift(context.getInstance()));
      }
    });
  }

  @Override
  @Deprecated
  public void changeUserPassword(String user, byte[] password) throws AccumuloException, AccumuloSecurityException {
    changeLocalUserPassword(user, new PasswordToken(password));
  }

  @Override
  public void changeLocalUserPassword(final String principal, final PasswordToken token) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(token != null, "token is null");
    final Credentials toChange = new Credentials(principal, token);
    executeVoid(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.changeLocalUserPassword(Tracer.traceInfo(), context.rpcCreds(), principal, ByteBuffer.wrap(token.getPassword()));
      }
    });
    if (context.getCredentials().getPrincipal().equals(principal)) {
      context.setCredentials(toChange);
    }
  }

  @Override
  public void changeUserAuthorizations(final String principal, final Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(authorizations != null, "authorizations is null");
    executeVoid(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.changeAuthorizations(Tracer.traceInfo(), context.rpcCreds(), principal, ByteBufferUtil.toByteBuffers(authorizations.getAuthorizations()));
      }
    });
  }

  @Override
  public Authorizations getUserAuthorizations(final String principal) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    return execute(new ClientExecReturn<Authorizations,ClientService.Client>() {
      @Override
      public Authorizations execute(ClientService.Client client) throws Exception {
        return new Authorizations(client.getUserAuthorizations(Tracer.traceInfo(), context.rpcCreds(), principal));
      }
    });
  }

  @Override
  public boolean hasSystemPermission(final String principal, final SystemPermission perm) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(perm != null, "perm is null");
    return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.hasSystemPermission(Tracer.traceInfo(), context.rpcCreds(), principal, perm.getId());
      }
    });
  }

  @Override
  public boolean hasTablePermission(final String principal, final String table, final TablePermission perm) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(table != null, "table is null");
    checkArgument(perm != null, "perm is null");
    try {
      return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
        @Override
        public Boolean execute(ClientService.Client client) throws Exception {
          return client.hasTablePermission(Tracer.traceInfo(), context.rpcCreds(), principal, table, perm.getId());
        }
      });
    } catch (AccumuloSecurityException e) {
      if (e.getSecurityErrorCode() == org.apache.accumulo.core.client.security.SecurityErrorCode.NAMESPACE_DOESNT_EXIST)
        throw new AccumuloSecurityException(null, SecurityErrorCode.TABLE_DOESNT_EXIST, e);
      else
        throw e;
    }
  }

  @Override
  public boolean hasNamespacePermission(final String principal, final String namespace, final NamespacePermission permission) throws AccumuloException,
      AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(namespace != null, "namespace is null");
    checkArgument(permission != null, "permission is null");
    return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.hasNamespacePermission(Tracer.traceInfo(), context.rpcCreds(), principal, namespace, permission.getId());
      }
    });
  }

  @Override
  public void grantSystemPermission(final String principal, final SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(permission != null, "permission is null");
    executeVoid(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.grantSystemPermission(Tracer.traceInfo(), context.rpcCreds(), principal, permission.getId());
      }
    });
  }

  @Override
  public void grantTablePermission(final String principal, final String table, final TablePermission permission) throws AccumuloException,
      AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(table != null, "table is null");
    checkArgument(permission != null, "permission is null");
    try {
      executeVoid(new ClientExec<ClientService.Client>() {
        @Override
        public void execute(ClientService.Client client) throws Exception {
          client.grantTablePermission(Tracer.traceInfo(), context.rpcCreds(), principal, table, permission.getId());
        }
      });
    } catch (AccumuloSecurityException e) {
      if (e.getSecurityErrorCode() == org.apache.accumulo.core.client.security.SecurityErrorCode.NAMESPACE_DOESNT_EXIST)
        throw new AccumuloSecurityException(null, SecurityErrorCode.TABLE_DOESNT_EXIST, e);
      else
        throw e;
    }
  }

  @Override
  public void grantNamespacePermission(final String principal, final String namespace, final NamespacePermission permission) throws AccumuloException,
      AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(namespace != null, "namespace is null");
    checkArgument(permission != null, "permission is null");
    executeVoid(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.grantNamespacePermission(Tracer.traceInfo(), context.rpcCreds(), principal, namespace, permission.getId());
      }
    });
  }

  @Override
  public void revokeSystemPermission(final String principal, final SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(permission != null, "permission is null");
    executeVoid(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.revokeSystemPermission(Tracer.traceInfo(), context.rpcCreds(), principal, permission.getId());
      }
    });
  }

  @Override
  public void revokeTablePermission(final String principal, final String table, final TablePermission permission) throws AccumuloException,
      AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(table != null, "table is null");
    checkArgument(permission != null, "permission is null");
    try {
      executeVoid(new ClientExec<ClientService.Client>() {
        @Override
        public void execute(ClientService.Client client) throws Exception {
          client.revokeTablePermission(Tracer.traceInfo(), context.rpcCreds(), principal, table, permission.getId());
        }
      });
    } catch (AccumuloSecurityException e) {
      if (e.getSecurityErrorCode() == org.apache.accumulo.core.client.security.SecurityErrorCode.NAMESPACE_DOESNT_EXIST)
        throw new AccumuloSecurityException(null, SecurityErrorCode.TABLE_DOESNT_EXIST, e);
      else
        throw e;
    }
  }

  @Override
  public void revokeNamespacePermission(final String principal, final String namespace, final NamespacePermission permission) throws AccumuloException,
      AccumuloSecurityException {
    checkArgument(principal != null, "principal is null");
    checkArgument(namespace != null, "namespace is null");
    checkArgument(permission != null, "permission is null");
    executeVoid(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.revokeNamespacePermission(Tracer.traceInfo(), context.rpcCreds(), principal, namespace, permission.getId());
      }
    });
  }

  @Deprecated
  @Override
  public Set<String> listUsers() throws AccumuloException, AccumuloSecurityException {
    return listLocalUsers();
  }

  @Override
  public Set<String> listLocalUsers() throws AccumuloException, AccumuloSecurityException {
    return execute(new ClientExecReturn<Set<String>,ClientService.Client>() {
      @Override
      public Set<String> execute(ClientService.Client client) throws Exception {
        return client.listLocalUsers(Tracer.traceInfo(), context.rpcCreds());
      }
    });
  }

  @Override
  public DelegationToken getDelegationToken(DelegationTokenConfig cfg) throws AccumuloException, AccumuloSecurityException {
    final TDelegationTokenConfig tConfig;
    if (null != cfg) {
      tConfig = DelegationTokenConfigSerializer.serialize(cfg);
    } else {
      tConfig = new TDelegationTokenConfig();
    }

    TDelegationToken thriftToken;
    try {
      thriftToken = MasterClient.execute(context, new ClientExecReturn<TDelegationToken,Client>() {
        @Override
        public TDelegationToken execute(Client client) throws Exception {
          return client.getDelegationToken(Tracer.traceInfo(), context.rpcCreds(), tConfig);
        }
      });
    } catch (TableNotFoundException e) {
      // should never happen
      throw new AssertionError("Received TableNotFoundException on method which should not throw that exception", e);
    }

    AuthenticationTokenIdentifier identifier = new AuthenticationTokenIdentifier(thriftToken.getIdentifier());

    // Get the password out of the thrift delegation token
    return new DelegationTokenImpl(thriftToken.getPassword(), identifier);
  }

}
