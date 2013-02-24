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
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.ServerClient;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.trace.instrument.Tracer;

public class SecurityOperationsImpl implements SecurityOperations {
  
  private Instance instance;
  private TCredentials credentials;
  
  private void execute(ClientExec<ClientService.Client> exec) throws AccumuloException, AccumuloSecurityException {
    try {
      ServerClient.executeRaw(instance, exec);
    } catch (ThriftTableOperationException ttoe) {
      // recast missing table
      if (ttoe.getType() == TableOperationExceptionType.NOTFOUND)
        throw new AccumuloSecurityException(null, SecurityErrorCode.TABLE_DOESNT_EXIST);
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
  
  /**
   * @param instance
   *          the connection information
   * @param credentials
   *          the user credentials to use for security operations
   */
  public SecurityOperationsImpl(Instance instance, TCredentials credentials) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.credentials = credentials;
  }
  
  @Deprecated
  @Override
  public void createUser(final String user, final byte[] password, final Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    createUser(user, new PasswordToken(password));
    changeUserAuthorizations(user, authorizations);
  }
  
  /**
   * Create a user
   * 
   * @param principal
   *          the principal to create
   * @param token
   *          the security token with the information about the user to create
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to create a user
   */
  @Override
  public void createUser(final String principal, final AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    _createUser(principal, token);
  }
  
  // Private method because the token/authorization constructor is something which is essentially new and depreciated.
  private void _createUser(final String principal, final AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(principal, token);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.createLocalUser(Tracer.traceInfo(), credentials, principal, ByteBuffer.wrap(((PasswordToken) token).getPassword()));
      }
    });
  }
  
  /**
   * Delete a user
   * 
   * @param user
   *          the user name to delete
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to delete a user
   */
  @Override
  public void dropUser(final String user) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.dropLocalUser(Tracer.traceInfo(), credentials, user);
      }
    });
  }
  
  /**
   * Verify a username/password combination is valid
   * 
   * @param principal
   *          the name of the user to authenticate
   * @param token
   *          the plaintext password for the user
   * @return true if the user asking is allowed to know and the specified user/password is valid, false otherwise
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to ask
   * @deprecated see {@link #authenticateUser(String, AuthenticationToken)}
   */
  @Deprecated
  @Override
  public boolean authenticateUser(final String principal, final byte[] token) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(token);
    return authenticateUser(principal, new PasswordToken(token));
  }
  
  @Override
  public boolean authenticateUser(final String principal, final AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(principal, token);
    final TCredentials toAuth = CredentialHelper.create(principal, token, instance.getInstanceID());
    return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.authenticateUser(Tracer.traceInfo(), credentials, toAuth);
      }
    });
  }
  
  /**
   * Set the user's password
   * 
   * @param principal
   *          the principal who's password is to be changed
   * @param token
   *          the security token with the information about the user to modify
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to modify a user
   * @deprecated
   */
  @Override
  @Deprecated
  public void changeUserPassword(final String principal, final byte[] token) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(token);
    changeLoginInfo(principal, new PasswordToken(token));
  }
  
  @Override
  public void changeLoginInfo(final String principal, final AuthenticationToken token) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(principal, token);
    final TCredentials toChange = CredentialHelper.create(principal, token, instance.getInstanceID());
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.changeLocalUserPassword(Tracer.traceInfo(), credentials, principal, ByteBuffer.wrap(((PasswordToken) token).getPassword()));
      }
    });
    if (this.credentials.principal.equals(principal)) {
      this.credentials = toChange;
    }
  }
  
  /**
   * Set the user's record-level authorizations
   * 
   * @param user
   *          the name of the user to modify
   * @param authorizations
   *          the authorizations that the user has for scanning
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to modify a user
   */
  @Override
  public void changeUserAuthorizations(final String user, final Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, authorizations);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.changeAuthorizations(Tracer.traceInfo(), credentials, user, ByteBufferUtil.toByteBuffers(authorizations.getAuthorizations()));
      }
    });
  }
  
  /**
   * Retrieves the user's authorizations for scanning
   * 
   * @param user
   *          the name of the user to query
   * @return the set of authorizations the user has available for scanning
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to query a user
   */
  @Override
  public Authorizations getUserAuthorizations(final String user) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user);
    return execute(new ClientExecReturn<Authorizations,ClientService.Client>() {
      @Override
      public Authorizations execute(ClientService.Client client) throws Exception {
        return new Authorizations(client.getUserAuthorizations(Tracer.traceInfo(), credentials, user));
      }
    });
  }
  
  /**
   * Verify the user has a particular system permission
   * 
   * @param user
   *          the name of the user to query
   * @param perm
   *          the system permission to check for
   * @return true if user has that permission; false otherwise
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to query a user
   */
  @Override
  public boolean hasSystemPermission(final String user, final SystemPermission perm) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, perm);
    return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.hasSystemPermission(Tracer.traceInfo(), credentials, user, perm.getId());
      }
    });
  }
  
  /**
   * Verify the user has a particular table permission
   * 
   * @param user
   *          the name of the user to query
   * @param table
   *          the name of the table to query about
   * @param perm
   *          the table permission to check for
   * @return true if user has that permission; false otherwise
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to query a user
   */
  @Override
  public boolean hasTablePermission(final String user, final String table, final TablePermission perm) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, table, perm);
    return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.hasTablePermission(Tracer.traceInfo(), credentials, user, table, perm.getId());
      }
    });
  }
  
  /**
   * Grant a user a system permission
   * 
   * @param user
   *          the name of the user to modify
   * @param permission
   *          the system permission to grant to the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to grant a user permissions
   */
  @Override
  public void grantSystemPermission(final String user, final SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, permission);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.grantSystemPermission(Tracer.traceInfo(), credentials, user, permission.getId());
      }
    });
  }
  
  /**
   * Grant a user a specific permission for a specific table
   * 
   * @param user
   *          the name of the user to modify
   * @param table
   *          the name of the table to modify for the user
   * @param permission
   *          the table permission to grant to the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to grant a user permissions
   */
  @Override
  public void grantTablePermission(final String user, final String table, final TablePermission permission) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, table, permission);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.grantTablePermission(Tracer.traceInfo(), credentials, user, table, permission.getId());
      }
    });
  }
  
  /**
   * Revoke a system permission from a user
   * 
   * @param user
   *          the name of the user to modify
   * @param permission
   *          the system permission to revoke for the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to revoke a user's permissions
   */
  @Override
  public void revokeSystemPermission(final String user, final SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, permission);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.revokeSystemPermission(Tracer.traceInfo(), credentials, user, permission.getId());
      }
    });
  }
  
  /**
   * Revoke a table permission for a specific user on a specific table
   * 
   * @param user
   *          the name of the user to modify
   * @param table
   *          the name of the table to modify for the user
   * @param permission
   *          the table permission to revoke for the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to revoke a user's permissions
   */
  @Override
  public void revokeTablePermission(final String user, final String table, final TablePermission permission) throws AccumuloException,
      AccumuloSecurityException {
    ArgumentChecker.notNull(user, table, permission);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.revokeTablePermission(Tracer.traceInfo(), credentials, user, table, permission.getId());
      }
    });
  }
  
  /**
   * Return a list of users in accumulo
   * 
   * @return a set of user names
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to query users
   */
  @Override
  public Set<String> listUsers() throws AccumuloException, AccumuloSecurityException {
    return execute(new ClientExecReturn<Set<String>,ClientService.Client>() {
      @Override
      public Set<String> execute(ClientService.Client client) throws Exception {
        return client.listLocalUsers(Tracer.traceInfo(), credentials);
      }
    });
  }
  
}
