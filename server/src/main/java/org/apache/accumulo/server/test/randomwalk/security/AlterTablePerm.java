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
package org.apache.accumulo.server.test.randomwalk.security;

import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;

public class AlterTablePerm extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    alter(state, props);
  }
  
  public static void alter(State state, Properties props) throws Exception {
    Connector conn;
    
    String action = props.getProperty("task", "toggle");
    String perm = props.getProperty("perm", "random");
    String sourceUser = props.getProperty("source", "system");
    String targetUser = props.getProperty("target", "table");
    boolean tabExists = SecurityHelper.getTableExists(state);
    
    String target;
    if ("table".equals(targetUser))
      target = SecurityHelper.getTabUserName(state);
    else
      target = SecurityHelper.getSysUserName(state);
    
    boolean exists = SecurityHelper.getTabUserExists(state);
    boolean tableExists = SecurityHelper.getTableExists(state);
    
    TablePermission tabPerm;
    if (perm.equals("random")) {
      Random r = new Random();
      int i = r.nextInt(TablePermission.values().length);
      tabPerm = TablePermission.values()[i];
    } else
      tabPerm = TablePermission.valueOf(perm);
    
    boolean hasPerm = SecurityHelper.getTabPerm(state, target, tabPerm);
    boolean canGive;
    if ("system".equals(sourceUser)) {
      conn = SecurityHelper.getSystemConnector(state);
      canGive = SecurityHelper.getSysPerm(state, SecurityHelper.getSysUserName(state), SystemPermission.ALTER_USER)
          || SecurityHelper.getTabPerm(state, SecurityHelper.getSysUserName(state), TablePermission.GRANT);
    } else if ("table".equals(sourceUser)) {
      conn = state.getInstance().getConnector(SecurityHelper.getTabUserName(state), SecurityHelper.getTabUserPass(state));
      canGive = SecurityHelper.getTabPerm(state, SecurityHelper.getTabUserName(state), TablePermission.GRANT);
    } else {
      conn = state.getConnector();
      canGive = true;
    }
    
    // toggle
    if (!"take".equals(action) && !"give".equals(action)) {
      try {
        boolean res;
        if (hasPerm != (res = state.getConnector().securityOperations().hasTablePermission(target, SecurityHelper.getTableName(state), tabPerm)))
          throw new AccumuloException("Test framework and accumulo are out of sync for user " + conn.whoami() + " for perm " + tabPerm.name()
              + " with local vs. accumulo being " + hasPerm + " " + res);
        
        if (hasPerm)
          action = "take";
        else
          action = "give";
      } catch (AccumuloSecurityException ae) {
        switch (ae.getErrorCode()) {
          case USER_DOESNT_EXIST:
            if (exists)
              throw new AccumuloException("Framework and Accumulo are out of sync, we think user exists", ae);
            else
              return;
          case TABLE_DOESNT_EXIST:
            if (tabExists)
              throw new AccumuloException(conn.whoami(), ae);
            else
              return;
          default:
            throw ae;
        }
      }
    }
    
    if ("take".equals(action)) {
      try {
        conn.securityOperations().revokeTablePermission(target, SecurityHelper.getTableName(state), tabPerm);
      } catch (AccumuloSecurityException ae) {
        switch (ae.getErrorCode()) {
          case GRANT_INVALID:
            if (tabPerm.equals(SystemPermission.GRANT))
              return;
          case PERMISSION_DENIED:
            if (canGive)
              throw new AccumuloException("Test user failed to give permission when it should have worked", ae);
            return;
          case USER_DOESNT_EXIST:
            if (exists)
              throw new AccumuloException("Table user doesn't exist and they SHOULD.", ae);
            return;
          case TABLE_DOESNT_EXIST:
            if (tableExists)
              throw new AccumuloException("Table doesn't exist but it should", ae);
            return;
          default:
            throw new AccumuloException("Got unexpected exception", ae);
        }
      }
      SecurityHelper.setTabPerm(state, target, tabPerm, false);
    } else if ("give".equals(action)) {
      try {
        conn.securityOperations().grantTablePermission(target, SecurityHelper.getTableName(state), tabPerm);
      } catch (AccumuloSecurityException ae) {
        switch (ae.getErrorCode()) {
          case GRANT_INVALID:
            if (tabPerm.equals(SystemPermission.GRANT))
              return;
            throw new AccumuloException("Got a grant invalid on non-System.GRANT option", ae);
          case PERMISSION_DENIED:
            if (canGive)
              throw new AccumuloException("Test user failed to give permission when it should have worked", ae);
            return;
          case USER_DOESNT_EXIST:
            if (exists)
              throw new AccumuloException("Table user doesn't exist and they SHOULD.", ae);
            return;
          case TABLE_DOESNT_EXIST:
            if (tableExists)
              throw new AccumuloException("Table doesn't exist but it should", ae);
            return;
          default:
            throw new AccumuloException("Got unexpected exception", ae);
        }
      }
      SecurityHelper.setTabPerm(state, target, tabPerm, true);
    }
    
    if (!exists)
      throw new AccumuloException("User shouldn't have existed, but apparantly does");
    if (!tableExists)
      throw new AccumuloException("Table shouldn't have existed, but apparantly does");
    if (!canGive)
      throw new AccumuloException("Source user shouldn't have been able to grant privilege");
    
  }
  
}
