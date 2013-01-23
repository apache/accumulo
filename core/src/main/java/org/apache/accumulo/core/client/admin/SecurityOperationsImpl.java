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

import java.util.Set;

import org.apache.accumulo.cloudtrace.instrument.Tracer;
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
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.security.tokens.AccumuloToken;
import org.apache.accumulo.core.security.tokens.InstanceTokenWrapper;
import org.apache.accumulo.core.security.tokens.PasswordUpdatable;
import org.apache.accumulo.core.security.tokens.TokenHelper;
import org.apache.accumulo.core.security.tokens.UserPassToken;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ByteBufferUtil;

public class SecurityOperationsImpl implements SecurityOperations {
  
  private Instance instance;
  private InstanceTokenWrapper token;
  
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
  public SecurityOperationsImpl(Instance instance, InstanceTokenWrapper credentials) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.token = credentials;
  }
  
  /**
   * Create a user
   * 
   * @param user
   *          the name of the user to create
   * @param password
   *          the plaintext password for the user
   * @param authorizations
   *          the authorizations that the user has for scanning
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to create a user
   * @deprecated Use {@link #createUser(AccumuloToken)} instead
   */
  public void createUser(final String user, final byte[] password, final Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, password, authorizations);
    createUser(new UserPassToken(user, password), authorizations);
  }
  
  /**
   * Create a user
   * 
   * @param user
   *          the name of the user to create
   * @param password
   *          the plaintext password for the user
   * @param authorizations
   *          the authorizations that the user has for scanning
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to create a user
   * @deprecated Use {@link #createUser(AccumuloToken)} instead
   */
  public void createUser(final AccumuloToken<?,?> newToken, final Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(newToken, authorizations);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.createUser(Tracer.traceInfo(), token.toThrift(), TokenHelper.wrapper(newToken),
            ByteBufferUtil.toByteBuffers(authorizations.getAuthorizations()));
      }
    });
  }
  
  /**
   * Create a user
   * 
   * @param user
   *          the name of the user to create
   * @param password
   *          the plaintext password for the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to create a user
   */
  public void createUser(final AccumuloToken<?,?> newToken) throws AccumuloException, AccumuloSecurityException {
    createUser(newToken, new Authorizations());
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
  public void dropUser(final String user) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.dropUser(Tracer.traceInfo(), token.toThrift(), user);
      }
    });
  }
  
  /**
   * Verify a username/password combination is valid
   * 
   * @param user
   *          the name of the user to authenticate
   * @param password
   *          the plaintext password for the user
   * @return true if the user asking is allowed to know and the specified user/password is valid, false otherwise
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to ask
   * @deprecated since 1.5, use {@link #authenticateUser(AccumuloToken)}
   */
  public boolean authenticateUser(final String user, final byte[] password) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, password);
    return authenticateUser(new UserPassToken(user, password));
  }
  
  /**
   * Verify a username/password combination is valid
   * 
   * @param token
   *          the AccumuloToken of the principal to authenticate
   * @return true if the user asking is allowed to know and the specified AccumuloToken is valid, false otherwise
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to ask
   */
  public boolean authenticateUser(final AccumuloToken<?,?> token2) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(token2);
    return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.authenticateUser(Tracer.traceInfo(), token.toThrift(), TokenHelper.wrapper(token2));
      }
    });
  }
  
  /**
   * Set the user's password
   * 
   * @param user
   *          the name of the user to modify
   * @param password
   *          the plaintext password for the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to modify a user
   */
  public void changeUserPassword(final AccumuloToken<?,?> newToken) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(newToken);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.changePassword(Tracer.traceInfo(), token.toThrift(), TokenHelper.wrapper(newToken));
      }
    });
    if (!(this.token.getToken() instanceof PasswordUpdatable) || !(newToken instanceof PasswordUpdatable))
      throw new AccumuloException("The AccumuloToken type cannot be dynamically adjusted. Please create a new token and reconnect");
    if (this.token.getPrincipal().equals(newToken.getPrincipal())) {
      PasswordUpdatable upt = (PasswordUpdatable) this.token.getToken();
      upt.updatePassword((PasswordUpdatable) newToken);
      token.toThrift().token = TokenHelper.wrapper(this.token.getToken());
    }
  }
  
  /**
   * Set the user's password
   * 
   * @param user
   *          the name of the user to modify
   * @param password
   *          the plaintext password for the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to modify a user
   */
  public void changeUserPassword(final String user, final byte[] password) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, password);
    changeUserPassword(new UserPassToken(user, password));
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
  public void changeUserAuthorizations(final String user, final Authorizations authorizations) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, authorizations);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.changeAuthorizations(Tracer.traceInfo(), token.toThrift(), user, ByteBufferUtil.toByteBuffers(authorizations.getAuthorizations()));
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
  public Authorizations getUserAuthorizations(final String user) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user);
    return execute(new ClientExecReturn<Authorizations,ClientService.Client>() {
      @Override
      public Authorizations execute(ClientService.Client client) throws Exception {
        return new Authorizations(client.getUserAuthorizations(Tracer.traceInfo(), token.toThrift(), user));
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
  public boolean hasSystemPermission(final String user, final SystemPermission perm) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, perm);
    return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.hasSystemPermission(Tracer.traceInfo(), token.toThrift(), user, perm.getId());
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
  public boolean hasTablePermission(final String user, final String table, final TablePermission perm) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, table, perm);
    return execute(new ClientExecReturn<Boolean,ClientService.Client>() {
      @Override
      public Boolean execute(ClientService.Client client) throws Exception {
        return client.hasTablePermission(Tracer.traceInfo(), token.toThrift(), user, table, perm.getId());
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
  public void grantSystemPermission(final String user, final SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, permission);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.grantSystemPermission(Tracer.traceInfo(), token.toThrift(), user, permission.getId());
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
  public void grantTablePermission(final String user, final String table, final TablePermission permission) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, table, permission);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.grantTablePermission(Tracer.traceInfo(), token.toThrift(), user, table, permission.getId());
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
  public void revokeSystemPermission(final String user, final SystemPermission permission) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(user, permission);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.revokeSystemPermission(Tracer.traceInfo(), token.toThrift(), user, permission.getId());
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
  public void revokeTablePermission(final String user, final String table, final TablePermission permission) throws AccumuloException,
      AccumuloSecurityException {
    ArgumentChecker.notNull(user, table, permission);
    execute(new ClientExec<ClientService.Client>() {
      @Override
      public void execute(ClientService.Client client) throws Exception {
        client.revokeTablePermission(Tracer.traceInfo(), token.toThrift(), user, table, permission.getId());
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
  public Set<String> listUsers() throws AccumuloException, AccumuloSecurityException {
    return execute(new ClientExecReturn<Set<String>,ClientService.Client>() {
      @Override
      public Set<String> execute(ClientService.Client client) throws Exception {
        return client.listUsers(Tracer.traceInfo(), token.toThrift());
      }
    });
  }
  
}
