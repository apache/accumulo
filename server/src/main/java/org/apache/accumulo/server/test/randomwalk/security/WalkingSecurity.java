/**
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
package org.apache.accumulo.server.test.randomwalk.security;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.handler.Authenticator;
import org.apache.accumulo.server.security.handler.Authorizor;
import org.apache.accumulo.server.security.handler.PermissionHandler;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

/**
 * 
 */
public class WalkingSecurity extends SecurityOperation implements Authorizor, Authenticator, PermissionHandler {
  State state;
  protected final static Logger log = Logger.getLogger(WalkingSecurity.class);
  
  private static final String tableName = "secTableName";
  private static final String userName = "UserName";
  
  private static final String userPass = "UserPass";
  private static final String userExists = "UserExists";
  private static final String tableExists = "TableExists";
  
  private static final String connector = "UserConn";
  
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
    if (instance != null && instance.state != state) {
      instance = new WalkingSecurity(state);
      state.set(tableExists, Boolean.toString(false));
      state.set(authsMap, new HashMap<String,Integer>());
    }

    return instance;
  }

  @Override
  public void initialize(String instanceId) {
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
  public void initializeSecurity(String rootuser) throws AccumuloSecurityException {
    throw new UnsupportedOperationException("nope");
  }
  
  @Override
  public void changeAuthorizations(String user, Authorizations authorizations) throws AccumuloSecurityException {
    state.set(user + "_auths", authorizations);
  }
  
  @Override
  public Authorizations getCachedUserAuthorizations(String user) throws AccumuloSecurityException {
    return (Authorizations) state.get(user + "_auths");
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
  public boolean authenticateUser(String user, ByteBuffer password, String instanceId) {
    return Arrays.equals((byte[]) state.get(user + userPass), password.array());
  }

  @Override
  public void createUser(String user, byte[] pass) throws AccumuloSecurityException {
    state.set(user + userExists, Boolean.toString(true));
    changePassword(user, pass);
  }
  
  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    state.set(user + userExists, Boolean.toString(false));
    for (SystemPermission sp : SystemPermission.values()) {
      revokeSystemPermission(user, sp);
    }
    if (getTableExists())
      try{
        for (TablePermission tp : TablePermission.values())
          revokeTablePermission(user, getTableName(), tp);
      } catch (TableNotFoundException tnfe) {
        log.error("This really shouldn't happen", tnfe);
      }
  }

  @Override
  public void changePassword(String user, byte[] pass) throws AccumuloSecurityException {
    state.set(user + userPass, pass);
    state.set(user + userPass + "time", System.currentTimeMillis());
  }

  @Override
  public boolean userExists(String user) {
    return Boolean.parseBoolean(state.getString(user + userExists));
  }
  
  @Override
  public boolean hasSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    return Boolean.parseBoolean(state.getString("Sys" + userName + permission.name()));
  }

  @Override
  public boolean hasCachedSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException {
    return hasSystemPermission(user, permission);
  }
  
  @Override
  public boolean hasTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    return Boolean.parseBoolean(state.getString("Tab" + table + userName + permission.name()));
  }
  
  @Override
  public boolean hasCachedTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    return hasTablePermission(user, table, permission);
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
    state.set("Sys" + userName + tp.name(), Boolean.toString(value));
  }
  
  private void setTabPerm(State state, String userName, TablePermission tp, String table, boolean value) {
    log.debug((value ? "Gave" : "Took") + " the table permission " + tp.name() + (value ? " to" : " from") + " user " + userName);
    state.set("Tab" + table + userName + tp.name(), Boolean.toString(value));
    if (tp.equals(TablePermission.READ) || tp.equals(TablePermission.WRITE))
      state.set("Tab" + table + userName + tp.name() + "time", System.currentTimeMillis());
  }

  @Override
  public void revokeTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException {
    setTabPerm(state, user, permission, table, false);
  }
  
  @Override
  public void cleanTablePermissions(String table) throws AccumuloSecurityException, TableNotFoundException {
    for (String user : new String[] {getSysUserName(), getTabUserName()}) {
      for (TablePermission tp : TablePermission.values()) {
        revokeTablePermission(user, null, tp);
      }
    }
    state.set(tableExists, Boolean.toString(false));
  }
  
  @Override
  public void cleanUser(String user) throws AccumuloSecurityException {
    for (TablePermission tp : TablePermission.values())
      try {
        revokeTablePermission(user, null, tp);
      } catch (TableNotFoundException e) {}
    for (SystemPermission sp : SystemPermission.values())
      revokeSystemPermission(user, sp);
  }
  
  @Override
  public void initializeSecurity(AuthInfo credentials, String rootuser, byte[] rootpass) throws AccumuloSecurityException {
    throw new UnsupportedOperationException("nope");
  }
  
  public String getTabUserName() {
    return state.getString("table" + userName);
  }
  
  public String getSysUserName() {
    return state.getString("system" + userName);
  }

  public void setTabUserName(String name) {
    state.set("table" + userName, name);
  }
  
  public void setSysUserName(String name) {
    state.set("system" + userName, name);
  }

  public Connector getSystemConnector() throws AccumuloException, AccumuloSecurityException {
    Connector toRet = (Connector) state.get("system" + connector);
    if (toRet == null) {
      toRet = state.getInstance().getConnector(getSysAuthInfo());
      state.set("system" + connector, toRet);
    }
    return toRet;
  }

  public Connector getTableConnector() throws AccumuloException, AccumuloSecurityException {
    Connector toRet = (Connector) state.get("table" + connector);
    if (toRet == null) {
      toRet = state.getInstance().getConnector(getTabAuthInfo());
      state.set("table" + connector, toRet);
    }
    return toRet;
  }

  public String getTableName() {
    return state.getString(tableName);
  }

  public boolean getTableExists() {
    return Boolean.parseBoolean(state.getString(tableExists));
  }
  
  public AuthInfo getSysAuthInfo() {
    return new AuthInfo(getSysUserName(), ByteBuffer.wrap(getSysPassword()), state.getInstance().getInstanceID());
  }
  
  public AuthInfo getTabAuthInfo() {
    return new AuthInfo(getTabUserName(), ByteBuffer.wrap(getTabPassword()), state.getInstance().getInstanceID());
  }

  public byte[] getUserPassword(String user) {
    return (byte[]) state.get(user + userPass);
  }

  public byte[] getSysPassword() {
    return (byte[]) state.get(getSysUserName() + userPass);
  }
  
  public byte[] getTabPassword() {
    return (byte[]) state.get(getTabUserName() + userPass);
  }

  public boolean userPassTransient(String user) {
    return System.currentTimeMillis() - state.getInteger(user + userPass + "time") < 1000;
  }
  
  public void setTableName(String tName) {
    state.set(tableName, tName);
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
      Long setTime = (Long) state.get("Tab" + userName + tp.name() + "time");
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
      curVal = new Integer(0);
      getAuthsMap().put(s, curVal);
    }
    curVal += increment;
  }

  public FileSystem getFs() {
    FileSystem fs = null;
    try {
      fs = (FileSystem) state.get(filesystem);
    } catch (RuntimeException re) {}
    ;
    if (fs == null) {
      try {
        fs = FileSystem.get(CachedConfiguration.getInstance());
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      state.set(filesystem, fs);
    }
    return fs;
  }
}
