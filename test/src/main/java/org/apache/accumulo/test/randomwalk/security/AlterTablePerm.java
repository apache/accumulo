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

import java.util.Properties;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class AlterTablePerm extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {
    alter(state, props);
  }

  public static void alter(State state, Properties props) throws Exception {
    String action = props.getProperty("task", "toggle");
    String perm = props.getProperty("perm", "random");
    String sourceUserProp = props.getProperty("source", "system");
    String targetUser = props.getProperty("target", "table");
    boolean tabExists = WalkingSecurity.get(state).getTableExists();

    String target;
    if ("table".equals(targetUser))
      target = WalkingSecurity.get(state).getTabUserName();
    else
      target = WalkingSecurity.get(state).getSysUserName();

    boolean exists = WalkingSecurity.get(state).userExists(target);
    boolean tableExists = WalkingSecurity.get(state).getTableExists();

    TablePermission tabPerm;
    if (perm.equals("random")) {
      Random r = new Random();
      int i = r.nextInt(TablePermission.values().length);
      tabPerm = TablePermission.values()[i];
    } else
      tabPerm = TablePermission.valueOf(perm);
    String tableName = WalkingSecurity.get(state).getTableName();
    boolean hasPerm = WalkingSecurity.get(state).hasTablePermission(target, tableName, tabPerm);
    boolean canGive;
    String sourceUser;
    AuthenticationToken sourceToken;
    if ("system".equals(sourceUserProp)) {
      sourceUser = WalkingSecurity.get(state).getSysUserName();
      sourceToken = WalkingSecurity.get(state).getSysToken();
    } else if ("table".equals(sourceUserProp)) {
      sourceUser = WalkingSecurity.get(state).getTabUserName();
      sourceToken = WalkingSecurity.get(state).getTabToken();
    } else {
      sourceUser = state.getUserName();
      sourceToken = state.getToken();
    }
    Connector conn = state.getInstance().getConnector(sourceUser, sourceToken);

    canGive = WalkingSecurity.get(state).canGrantTable(new Credentials(sourceUser, sourceToken).toThrift(state.getInstance()), target,
        WalkingSecurity.get(state).getTableName(), WalkingSecurity.get(state).getNamespaceName());

    // toggle
    if (!"take".equals(action) && !"give".equals(action)) {
      try {
        boolean res;
        if (hasPerm != (res = state.getConnector().securityOperations().hasTablePermission(target, tableName, tabPerm)))
          throw new AccumuloException("Test framework and accumulo are out of sync for user " + conn.whoami() + " for perm " + tabPerm.name()
              + " with local vs. accumulo being " + hasPerm + " " + res);

        if (hasPerm)
          action = "take";
        else
          action = "give";
      } catch (AccumuloSecurityException ae) {
        switch (ae.getSecurityErrorCode()) {
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

    boolean trans = WalkingSecurity.get(state).userPassTransient(conn.whoami());
    if ("take".equals(action)) {
      try {
        conn.securityOperations().revokeTablePermission(target, tableName, tabPerm);
      } catch (AccumuloSecurityException ae) {
        switch (ae.getSecurityErrorCode()) {
          case GRANT_INVALID:
            throw new AccumuloException("Got a grant invalid on non-System.GRANT option", ae);
          case PERMISSION_DENIED:
            if (canGive)
              throw new AccumuloException(conn.whoami() + " failed to revoke permission to " + target + " when it should have worked", ae);
            return;
          case USER_DOESNT_EXIST:
            if (exists)
              throw new AccumuloException("Table user doesn't exist and they SHOULD.", ae);
            return;
          case TABLE_DOESNT_EXIST:
            if (tableExists)
              throw new AccumuloException("Table doesn't exist but it should", ae);
            return;
          case BAD_CREDENTIALS:
            if (!trans)
              throw new AccumuloException("Bad credentials for user " + conn.whoami());
            return;
          default:
            throw new AccumuloException("Got unexpected exception", ae);
        }
      }
      WalkingSecurity.get(state).revokeTablePermission(target, tableName, tabPerm);
    } else if ("give".equals(action)) {
      try {
        conn.securityOperations().grantTablePermission(target, tableName, tabPerm);
      } catch (AccumuloSecurityException ae) {
        switch (ae.getSecurityErrorCode()) {
          case GRANT_INVALID:
            throw new AccumuloException("Got a grant invalid on non-System.GRANT option", ae);
          case PERMISSION_DENIED:
            if (canGive)
              throw new AccumuloException(conn.whoami() + " failed to give permission to " + target + " when it should have worked", ae);
            return;
          case USER_DOESNT_EXIST:
            if (exists)
              throw new AccumuloException("Table user doesn't exist and they SHOULD.", ae);
            return;
          case TABLE_DOESNT_EXIST:
            if (tableExists)
              throw new AccumuloException("Table doesn't exist but it should", ae);
            return;
          case BAD_CREDENTIALS:
            if (!trans)
              throw new AccumuloException("Bad credentials for user " + conn.whoami());
            return;
          default:
            throw new AccumuloException("Got unexpected exception", ae);
        }
      }
      WalkingSecurity.get(state).grantTablePermission(target, tableName, tabPerm);
    }

    if (!exists)
      throw new AccumuloException("User shouldn't have existed, but apparantly does");
    if (!tableExists)
      throw new AccumuloException("Table shouldn't have existed, but apparantly does");
    if (!canGive)
      throw new AccumuloException(conn.whoami() + " shouldn't have been able to grant privilege");

  }

}
