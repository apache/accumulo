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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.accumulo.server.test.randomwalk.Test;

public class DropTable extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    dropTable(state, props);
  }
  
  public static void dropTable(State state, Properties props) throws Exception {
    String sourceUser = props.getProperty("source", "system");
    Connector conn;
    String username;
    if (sourceUser.equals("table")) {
      username = SecurityHelper.getTabUserName(state);
      conn = state.getInstance().getConnector(username, SecurityHelper.getTabUserPass(state));
    } else {
      username = SecurityHelper.getSysUserName(state);
      conn = SecurityHelper.getSystemConnector(state);
    }
    
    String tableName = SecurityHelper.getTableName(state);
    
    boolean exists = SecurityHelper.getTableExists(state);
    boolean hasPermission = false;
    if (SecurityHelper.getSysPerm(state, username, SystemPermission.DROP_TABLE) || SecurityHelper.getTabPerm(state, username, TablePermission.DROP_TABLE))
      hasPermission = true;
    
    try {
      conn.tableOperations().delete(tableName);
    } catch (AccumuloSecurityException ae) {
      if (ae.getErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
        if (hasPermission)
          throw new AccumuloException("Got a security exception when I should have had permission.", ae);
        else {
          // Drop anyway for sake of state
          state.getConnector().tableOperations().delete(tableName);
          SecurityHelper.setTableExists(state, false);
          for (String user : new String[] {SecurityHelper.getSysUserName(state), SecurityHelper.getTabUserName(state)})
            for (TablePermission tp : TablePermission.values())
              SecurityHelper.setTabPerm(state, user, tp, false);
          return;
        }
      }
      throw new AccumuloException("Got unexpected ae error code", ae);
    } catch (TableNotFoundException tnfe) {
      if (exists)
        throw new TableExistsException(null, tableName, "Got a TableNotFOundException but it should have existed", tnfe);
      else
        return;
    }
    SecurityHelper.setTableExists(state, false);
    for (String user : new String[] {SecurityHelper.getSysUserName(state), SecurityHelper.getTabUserName(state)})
      for (TablePermission tp : TablePermission.values())
        SecurityHelper.setTabPerm(state, user, tp, false);
    if (!hasPermission)
      throw new AccumuloException("Didn't get Security Exception when we should have");
  }
}
