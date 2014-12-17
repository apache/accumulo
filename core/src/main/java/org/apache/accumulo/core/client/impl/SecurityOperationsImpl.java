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
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.trace.instrument.Tracer;

public class SecurityOperationsImpl implements SecurityOperations {

  private Instance instance;
  private Credentials credentials;

  private void execute(ClientExec<ClientService.Client> exec) throws AccumuloException, AccumuloSecurityException {
    try {
      ServerClient.executeRaw(instance, exec);
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
      return ServerClient.executeRaw(instance, exec);
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

  public SecurityOperationsImpl(Instance instance, Credentials credentials) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.credentials = credentials;
  }

  @Deprecated
  @Override
  public void createUser(String user, byte[] password, final Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    createLocalUser(user, new PasswordToken(password));
    changeUserAuthorizations(user, authorizations);
  }

  @Override
  public void createLocalUser(final String principal, final PasswordToken password) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(principal, password);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.createLocalUser(Tracer.traceInfo(), credentials.toThrift(instance), principal, ByteBuffer.wrap(password.getPassword()));
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
    ArgumentChecker.notNull(principal);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.dropLocalUser(Tracer.traceInfo(), credentials.toThrift(instance), principal);
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
    ArgumentChecker.notNull(principal, token);
    final Credentials toAuth = new Credentials(principal, token);
    return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.authenticateUser(Tracer.traceInfo(), credentials.toThrift(instance), toAuth.toThrift(instance));
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
    ArgumentChecker.notNull(principal, token);
    final Credentials toChange = new Credentials(principal, token);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.changeLocalUserPassword(Tracer.traceInfo(), credentials.toThrift(instance), principal, ByteBuffer.wrap(token.getPassword()));
      }
    });
    if (this.credentials.getPrincipal().equals(principal)) {
      this.credentials = toChange;
    }
  }

  @Override
  public void changeUserAuthorizations(final String principal, final Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(principal, authorizations);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.changeAuthorizations(Tracer.traceInfo(), credentials.toThrift(instance), principal,
            ByteBufferUtil.toByteBuffers(authorizations.getAuthorizations()));
      }
    });
  }

  @Override
  public Authorizations getUserAuthorizations(final String principal) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(principal);
    return execute(new ClientExecReturn<Authorizations,ClientService.Client>() {
      @Override
      public Authorizations execute(ClientService.Client client) throws Exception {
        return new Authorizations(client.getUserAuthorizations(Tracer.traceInfo(), credentials.toThrift(instance), principal));
      }
    });
  }

  @Override
  public boolean hasSystemPermission(final String principal, final SystemPermission perm) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(principal, perm);
    return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.hasSystemPermission(Tracer.traceInfo(), credentials.toThrift(instance), principal, perm.getId());
      }
    });
  }

  @Override
  public boolean hasTablePermission(final String principal, final String table, final TablePermission perm) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(principal, table, perm);
    try {
      return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
        @Override
        public Boolean execute(ClientService.Client client) throws Exception {
          return client.hasTablePermission(Tracer.traceInfo(), credentials.toThrift(instance), principal, table, perm.getId());
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
    ArgumentChecker.notNull(principal, namespace, permission);
    return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.hasNamespacePermission(Tracer.traceInfo(), credentials.toThrift(instance), principal, namespace, permission.getId());
      }
    });
  }

  @Override
  public void grantSystemPermission(final String principal, final SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(principal, permission);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.grantSystemPermission(Tracer.traceInfo(), credentials.toThrift(instance), principal, permission.getId());
      }
    });
  }

  @Override
  public void grantTablePermission(final String principal, final String table, final TablePermission permission) throws AccumuloException,
      AccumuloSecurityException {
    ArgumentChecker.notNull(principal, table, permission);
    try {
      execute(new ClientExec<ClientService.Client>() {
        @Override
        public void execute(ClientService.Client client) throws Exception {
          client.grantTablePermission(Tracer.traceInfo(), credentials.toThrift(instance), principal, table, permission.getId());
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
    ArgumentChecker.notNull(principal, namespace, permission);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.grantNamespacePermission(Tracer.traceInfo(), credentials.toThrift(instance), principal, namespace, permission.getId());
      }
    });
  }

  @Override
  public void revokeSystemPermission(final String principal, final SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(principal, permission);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.revokeSystemPermission(Tracer.traceInfo(), credentials.toThrift(instance), principal, permission.getId());
      }
    });
  }

  @Override
  public void revokeTablePermission(final String principal, final String table, final TablePermission permission) throws AccumuloException,
      AccumuloSecurityException {
    ArgumentChecker.notNull(principal, table, permission);
    try {
      execute(new ClientExec<ClientService.Client>() {
        @Override
        public void execute(ClientService.Client client) throws Exception {
          client.revokeTablePermission(Tracer.traceInfo(), credentials.toThrift(instance), principal, table, permission.getId());
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
    ArgumentChecker.notNull(principal, namespace, permission);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.revokeNamespacePermission(Tracer.traceInfo(), credentials.toThrift(instance), principal, namespace, permission.getId());
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
        return client.listLocalUsers(Tracer.traceInfo(), credentials.toThrift(instance));
      }
    });
  }

}
