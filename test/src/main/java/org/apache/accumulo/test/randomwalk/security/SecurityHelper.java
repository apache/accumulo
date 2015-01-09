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

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

public class SecurityHelper {
  protected final static Logger log = Logger.getLogger(SecurityHelper.class);

  private static final String tableName = "secTableName";
  private static final String masterName = "sysUserName";
  private static final String tUserName = "tabUserName";

  private static final String masterPass = "sysUserPass";
  private static final String tUserPass = "tabUserPass";

  private static final String tUserExists = "tabUserExists";
  private static final String tableExists = "secTableExists";

  private static final String masterConn = "sysUserConn";

  private static final String authsMap = "authorizationsCountMap";
  private static final String lastKey = "lastMutationKey";
  private static final String filesystem = "securityFileSystem";

  public static String getTableName(State state) {
    return state.getString(tableName);
  }

  public static void setTableName(State state, String tName) {
    state.set(tableName, tName);
  }

  public static String getSysUserName(State state) {
    return state.getString(masterName);
  }

  public static void setSysUserName(State state, String sysUserName) {
    state.set(masterName, sysUserName);
  }

  public static String getTabUserName(State state) {
    return state.getString(tUserName);
  }

  public static void setTabUserName(State state, String tabUserName) {
    state.set(tUserName, tabUserName);
  }

  public static byte[] getSysUserPass(State state) {
    return (byte[]) state.get(masterPass);
  }

  public static void setSysUserPass(State state, byte[] sysUserPass) {
    log.debug("Setting system user pass to " + new String(sysUserPass, UTF_8));
    state.set(masterPass, sysUserPass);
    state.set(masterPass + "time", System.currentTimeMillis());

  }

  public static boolean sysUserPassTransient(State state) {
    return System.currentTimeMillis() - state.getLong(masterPass + "time") < 1000;
  }

  public static byte[] getTabUserPass(State state) {
    return (byte[]) state.get(tUserPass);
  }

  public static void setTabUserPass(State state, byte[] tabUserPass) {
    log.debug("Setting table user pass to " + new String(tabUserPass, UTF_8));
    state.set(tUserPass, tabUserPass);
    state.set(tUserPass + "time", System.currentTimeMillis());
  }

  public static boolean tabUserPassTransient(State state) {
    return System.currentTimeMillis() - state.getLong(tUserPass + "time") < 1000;
  }

  public static boolean getTabUserExists(State state) {
    return Boolean.parseBoolean(state.getString(tUserExists));
  }

  public static void setTabUserExists(State state, boolean exists) {
    state.set(tUserExists, Boolean.toString(exists));
  }

  public static boolean getTableExists(State state) {
    return Boolean.parseBoolean(state.getString(tableExists));
  }

  public static void setTableExists(State state, boolean exists) {
    state.set(tableExists, Boolean.toString(exists));
  }

  public static Connector getSystemConnector(State state) {
    return (Connector) state.get(masterConn);
  }

  public static void setSystemConnector(State state, Connector conn) {
    state.set(masterConn, conn);
  }

  public static boolean getTabPerm(State state, String userName, TablePermission tp) {
    return Boolean.parseBoolean(state.getString("Tab" + userName + tp.name()));
  }

  public static void setTabPerm(State state, String userName, TablePermission tp, boolean value) {
    log.debug((value ? "Gave" : "Took") + " the table permission " + tp.name() + (value ? " to" : " from") + " user " + userName);
    state.set("Tab" + userName + tp.name(), Boolean.toString(value));
    if (tp.equals(TablePermission.READ) || tp.equals(TablePermission.WRITE))
      state.set("Tab" + userName + tp.name() + "time", System.currentTimeMillis());

  }

  public static boolean getSysPerm(State state, String userName, SystemPermission tp) {
    return Boolean.parseBoolean(state.getString("Sys" + userName + tp.name()));
  }

  public static void setSysPerm(State state, String userName, SystemPermission tp, boolean value) {
    log.debug((value ? "Gave" : "Took") + " the system permission " + tp.name() + (value ? " to" : " from") + " user " + userName);
    state.set("Sys" + userName + tp.name(), Boolean.toString(value));
  }

  public static Authorizations getUserAuths(State state, String target) {
    return (Authorizations) state.get(target + "_auths");
  }

  public static void setUserAuths(State state, String target, Authorizations auths) {
    state.set(target + "_auths", auths);
  }

  @SuppressWarnings("unchecked")
  public static Map<String,Integer> getAuthsMap(State state) {
    return (Map<String,Integer>) state.get(authsMap);
  }

  public static void setAuthsMap(State state, Map<String,Integer> map) {
    state.set(authsMap, map);
  }

  public static String[] getAuthsArray() {
    return new String[] {"Fishsticks", "PotatoSkins", "Ribs", "Asparagus", "Paper", "Towels", "Lint", "Brush", "Celery"};
  }

  public static String getLastKey(State state) {
    return state.getString(lastKey);
  }

  public static void setLastKey(State state, String key) {
    state.set(lastKey, key);
  }

  public static void increaseAuthMap(State state, String s, int increment) {
    Integer curVal = getAuthsMap(state).get(s);
    if (curVal == null) {
      curVal = Integer.valueOf(0);
      getAuthsMap(state).put(s, curVal);
    }
    curVal += increment;
  }

  public static FileSystem getFs(State state) {
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

  public static boolean inAmbiguousZone(State state, String userName, TablePermission tp) {
    if (tp.equals(TablePermission.READ) || tp.equals(TablePermission.WRITE)) {
      Long setTime = (Long) state.get("Tab" + userName + tp.name() + "time");
      if (System.currentTimeMillis() < (setTime + 1000))
        return true;
    }
    return false;
  }

}
