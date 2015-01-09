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
package org.apache.accumulo.test.randomwalk.security;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.handler.Authenticator;
import org.apache.accumulo.server.security.handler.Authorizor;
import org.apache.accumulo.server.security.handler.PermissionHandler;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

/**
 *
 */
public class WalkingSecurity extends SecurityOperation implements Authorizor, Authenticator, PermissionHandler {
  State state = null;
  protected final static Logger log = Logger.getLogger(WalkingSecurity.class);

  private static final String tableName = "SecurityTableName";
  private static final String namespaceName = "SecurityNamespaceName";
  private static final String userName = "UserName";

  private static final String userPass = "UserPass";
  private static final String userExists = "UserExists";
  private static final String tableExists = "TableExists";
  private static final String namespaceExists = "NamespaceExists";

  private static final String connector = "UserConnection";

  private static final String authsMap = "authorizationsCountMap";
  private static final String lastKey = "lastMutationKey";
  private static final String filesystem = "securityFileSystem";

  private static WalkingSecurity instance = null;

  public WalkingSecurity(Authorizor author, Authenticator authent, PermissionHandler pm, String instanceId) {
    super(author, authent, pm, instanceId);
  }

  public WalkingSecurity(State state2) {
    super(state2.getInstance().getInstanceID());
    this.state = state2;
    authorizor = this;
    authenticator = this;
    permHandle = this;
  }

  public static WalkingSecurity get(State state) {
    if (instance == null || instance.state != state) {
      instance = new WalkingSecurity(state);
      state.set(tableExists, Boolean.toString(false));
      state.set(namespaceExists, Boolean.toString(false));
      state.set(authsMap, new HashMap<String,Integer>());
    }

    return instance;
  }

  @Override
  public void initialize(String instanceId, boolean initialize) {
    throw new UnsupportedOperationException("nope");
  }

  @Override
  public boolean validSecurityHandlers(Authenticator one, PermissionHandler two) {
    return this.getClass().equals(one.getClass()) && this.getClass().equals(two.getClass());
  }

  @Override
  public boolean validSecurityHandlers(Authenticator one, Authorizor two) {
    return this.getClass().equals(one.getClass()) && this.getClass().equals(two.getClass());
  }

  @Override
  public boolean validSecurityHandlers(Authorizor one, PermissionHandler two) {
    return this.getClass().equals(one.getClass()) && this.getClass().equals(two.getClass());
  }

  @Override
  public void initializeSecurity(TCredentials rootuser, String token) throws ThriftSecurityException {
    throw new UnsupportedOperationException("nope");
  }

  @Override
  public void changeAuthorizations(String user, Authorizations authorizations) throws AccumuloSecurityException {
    state.set(user + "_auths", authorizations);
    state.set("Auths-" + user + '-' + "time", System.currentTimeMillis());
  }

  @Override
  public Authorizations getCachedUserAuthorizations(String user) throws AccumuloSecurityException {
    return (Authorizations) state.get(user + "_auths");
  }

  public boolean ambiguousAuthorizations(String userName) {
    Long setTime = state.getLong("Auths-" + userName + '-' + "time");
    if (setTime == null)
      throw new RuntimeException("WTF? Auths-" + userName + '-' + "time is null");
    if (System.currentTimeMillis() < (setTime + 1000))
      return true;
    return false;
  }

  @Override
  public void initUser(String user) throws AccumuloSecurityException {
    changeAuthorizations(user, new Authorizations());
  }

  @Override
  public Set<String> listUsers() throws AccumuloSecurityException {
    Set<String> userList = new TreeSet<String>();
    for (String user : new String[] {getSysUserName(), getTabUserName()}) {
      if (userExists(user))
        userList.add(user);
    }
    return userList;
  }

  @Override
  public boolean authenticateUser(String principal, AuthenticationToken token) {
    PasswordToken pass = (PasswordToken) state.get(principal + userPass);
    boolean ret = pass.equals(token);
    return ret;
  }

  @Override
  public void createUser(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    state.set(principal + userExists, Boolean.toString(true));
    changePassword(principal, token);
    cleanUser(principal);
  }

  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    state.set(user + userExists, Boolean.toString(false));
    cleanUser(user);
    if (user.equals(getTabUserName()))
      state.set("table" + connector, null);
  }

  @Override
  public void changePassword(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    state.set(principal + userPass, token);
    state.set(principal + userPass + "time", System.currentTimeMillis());
  }

  @Override
  public boolean userExists(String user) {
    return Boolean.parseBoolean(state.getString(user + userExists));
  }

  @Override
  public boolean hasSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    boolean res = Boolean.parseBoolean(state.getString("Sys-" + user + '-' + permission.name()));
    return res;
  }

  @Override
  public boolean hasCachedSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    return hasSystemPermission(user, permission);
  }

  @Override
  public boolean hasTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    return Boolean.parseBoolean(state.getString("Tab-" + user + '-' + permission.name()));
  }

  @Override
  public boolean hasCachedTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    return hasTablePermission(user, table, permission);
  }

  @Override
  public boolean hasNamespacePermission(String user, String namespace, NamespacePermission permission) throws AccumuloSecurityException,
      NamespaceNotFoundException {
    return Boolean.parseBoolean(state.getString("Nsp-" + user + '-' + permission.name()));
  }

  @Override
  public boolean hasCachedNamespacePermission(String user, String namespace, NamespacePermission permission) throws AccumuloSecurityException,
      NamespaceNotFoundException {
    return hasNamespacePermission(user, namespace, permission);
  }

  @Override
  public void grantSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    setSysPerm(state, user, permission, true);
  }

  @Override
  public void revokeSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    setSysPerm(state, user, permission, false);
  }

  @Override
  public void grantTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    setTabPerm(state, user, permission, table, true);
  }

  private static void setSysPerm(State state, String userName, SystemPermission tp, boolean value) {
    log.debug((value ? "Gave" : "Took") + " the system permission " + tp.name() + (value ? " to" : " from") + " user " + userName);
    state.set("Sys-" + userName + '-' + tp.name(), Boolean.toString(value));
  }

  private void setTabPerm(State state, String userName, TablePermission tp, String table, boolean value) {
    if (table.equals(userName))
      throw new RuntimeException("This is also fucked up");
    log.debug((value ? "Gave" : "Took") + " the table permission " + tp.name() + (value ? " to" : " from") + " user " + userName);
    state.set("Tab-" + userName + '-' + tp.name(), Boolean.toString(value));
    if (tp.equals(TablePermission.READ) || tp.equals(TablePermission.WRITE))
      state.set("Tab-" + userName + '-' + tp.name() + '-' + "time", System.currentTimeMillis());
  }

  @Override
  public void revokeTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    setTabPerm(state, user, permission, table, false);
  }

  @Override
  public void grantNamespacePermission(String user, String namespace, NamespacePermission permission) throws AccumuloSecurityException,
      NamespaceNotFoundException {
    setNspPerm(state, user, permission, namespace, true);
  }

  private void setNspPerm(State state, String userName, NamespacePermission tnp, String namespace, boolean value) {
    if (namespace.equals(userName))
      throw new RuntimeException("I don't even know");
    log.debug((value ? "Gave" : "Took") + " the table permission " + tnp.name() + (value ? " to" : " from") + " user " + userName);
    state.set("Nsp-" + userName + '-' + tnp.name(), Boolean.toString(value));
    if (tnp.equals(NamespacePermission.READ) || tnp.equals(NamespacePermission.WRITE))
      state.set("Nsp-" + userName + '-' + tnp.name() + '-' + "time", System.currentTimeMillis());
  }

  @Override
  public void revokeNamespacePermission(String user, String namespace, NamespacePermission permission) throws AccumuloSecurityException,
      NamespaceNotFoundException {
    setNspPerm(state, user, permission, namespace, false);
  }

  @Override
  public void cleanTablePermissions(String table) throws AccumuloSecurityException, TableNotFoundException {
    for (String user : new String[] {getSysUserName(), getTabUserName()}) {
      for (TablePermission tp : TablePermission.values()) {
        revokeTablePermission(user, table, tp);
      }
    }
    state.set(tableExists, Boolean.toString(false));
  }

  @Override
  public void cleanNamespacePermissions(String namespace) throws AccumuloSecurityException, NamespaceNotFoundException {
    for (String user : new String[] {getSysUserName(), getNspUserName()}) {
      for (NamespacePermission tnp : NamespacePermission.values()) {
        revokeNamespacePermission(user, namespace, tnp);
      }
    }
    state.set(namespaceExists, Boolean.toString(false));
  }

  @Override
  public void cleanUser(String user) throws AccumuloSecurityException {
    if (getTableExists())
      for (TablePermission tp : TablePermission.values())
        try {
          revokeTablePermission(user, getTableName(), tp);
        } catch (TableNotFoundException e) {}
    for (SystemPermission sp : SystemPermission.values())
      revokeSystemPermission(user, sp);
  }

  public String getTabUserName() {
    return state.getString("table" + userName);
  }

  public String getSysUserName() {
    return state.getString("system" + userName);
  }

  public String getNspUserName() {
    return state.getString("namespace" + userName);
  }

  public void setTabUserName(String name) {
    state.set("table" + userName, name);
    state.set(name + userExists, Boolean.toString(false));
  }

  public void setNspUserName(String name) {
    state.set("namespace" + userName, name);
    state.set(name + userExists, Boolean.toString(false));
  }

  public void setSysUserName(String name) {
    state.set("system" + userName, name);
  }

  public String getTableName() {
    return state.getString(tableName);
  }

  public String getNamespaceName() {
    return state.getString(namespaceName);
  }

  public boolean getTableExists() {
    return Boolean.parseBoolean(state.getString(tableExists));
  }

  public boolean getNamespaceExists() {
    return Boolean.parseBoolean(state.getString(namespaceExists));
  }

  public TCredentials getSysCredentials() {
    return new Credentials(getSysUserName(), getSysToken()).toThrift(this.state.getInstance());
  }

  public TCredentials getTabCredentials() {
    return new Credentials(getTabUserName(), getTabToken()).toThrift(this.state.getInstance());
  }

  public AuthenticationToken getSysToken() {
    return new PasswordToken(getSysPassword());
  }

  public AuthenticationToken getTabToken() {
    return new PasswordToken(getTabPassword());
  }

  public byte[] getUserPassword(String user) {
    Object obj = state.get(user + userPass);
    if (obj instanceof PasswordToken) {
      return ((PasswordToken) obj).getPassword();
    }
    return null;
  }

  public byte[] getSysPassword() {
    Object obj = state.get(getSysUserName() + userPass);
    if (obj instanceof PasswordToken) {
      return ((PasswordToken) obj).getPassword();
    }
    return null;
  }

  public byte[] getTabPassword() {
    Object obj = state.get(getTabUserName() + userPass);
    if (obj instanceof PasswordToken) {
      return ((PasswordToken) obj).getPassword();
    }
    return null;
  }

  public boolean userPassTransient(String user) {
    return System.currentTimeMillis() - state.getLong(user + userPass + "time") < 1000;
  }

  public void setTableName(String tName) {
    state.set(tableName, tName);
  }

  public void setNamespaceName(String nsName) {
    state.set(namespaceName, nsName);
  }

  @Override
  public void initTable(String table) throws AccumuloSecurityException {
    state.set(tableExists, Boolean.toString(true));
    state.set(tableName, table);
  }

  public String[] getAuthsArray() {
    return new String[] {"Fishsticks", "PotatoSkins", "Ribs", "Asparagus", "Paper", "Towels", "Lint", "Brush", "Celery"};
  }

  public boolean inAmbiguousZone(String userName, TablePermission tp) {
    if (tp.equals(TablePermission.READ) || tp.equals(TablePermission.WRITE)) {
      Long setTime = state.getLong("Tab-" + userName + '-' + tp.name() + '-' + "time");
      if (setTime == null)
        throw new RuntimeException("WTF? Tab-" + userName + '-' + tp.name() + '-' + "time is null");
      if (System.currentTimeMillis() < (setTime + 1000))
        return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public Map<String,Integer> getAuthsMap() {
    return (Map<String,Integer>) state.get(authsMap);
  }

  public String getLastKey() {
    return state.getString(lastKey);
  }

  public void increaseAuthMap(String s, int increment) {
    Integer curVal = getAuthsMap().get(s);
    if (curVal == null) {
      curVal = Integer.valueOf(0);
      getAuthsMap().put(s, curVal);
    }
    curVal += increment;
  }

  public FileSystem getFs() {
    FileSystem fs = null;
    try {
      fs = (FileSystem) state.get(filesystem);
    } catch (RuntimeException re) {}

    if (fs == null) {
      try {
        fs = FileSystem.get(CachedConfiguration.getInstance());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      state.set(filesystem, fs);
    }
    return fs;
  }

  @Override
  public boolean canAskAboutUser(TCredentials credentials, String user) throws ThriftSecurityException {
    try {
      return super.canAskAboutUser(credentials, user);
    } catch (ThriftSecurityException tse) {
      if (tse.getCode().equals(SecurityErrorCode.PERMISSION_DENIED))
        return false;
      throw tse;
    }
  }

  @Override
  public boolean validTokenClass(String tokenClass) {
    return tokenClass.equals(PasswordToken.class.getName());
  }

  public static void clearInstance() {
    instance = null;
  }

  @Override
  public Set<Class<? extends AuthenticationToken>> getSupportedTokenTypes() {
    Set<Class<? extends AuthenticationToken>> cs = new HashSet<Class<? extends AuthenticationToken>>();
    cs.add(PasswordToken.class);
    return cs;
  }

  @Override
  public boolean isValidAuthorizations(String user, List<ByteBuffer> auths) throws AccumuloSecurityException {
    Collection<ByteBuffer> userauths = getCachedUserAuthorizations(user).getAuthorizationsBB();
    for (ByteBuffer auth : auths)
      if (!userauths.contains(auth))
        return false;
    return true;
  }

}
