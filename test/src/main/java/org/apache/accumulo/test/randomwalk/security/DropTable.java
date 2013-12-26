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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.log4j.Logger;

public class DropTable extends Test {
  private static final Logger log = Logger.getLogger(DropTable.class);
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    dropTable(state, props);
  }
  
  public static void dropTable(State state, Properties props) throws Exception {
    String sourceUser = props.getProperty("source", "system");
    String principal;
    AuthenticationToken token;
    if (sourceUser.equals("table")) {
      principal = WalkingSecurity.get(state).getTabUserName();
      token = WalkingSecurity.get(state).getTabToken();
    } else {
      principal = WalkingSecurity.get(state).getSysUserName();
      token = WalkingSecurity.get(state).getSysToken();
    }
    Connector conn = state.getInstance().getConnector(principal, token);
    
    String tableName = WalkingSecurity.get(state).getTableName();
    String tableId = conn.tableOperations().tableIdMap().get(tableName);
    
    boolean exists = WalkingSecurity.get(state).getTableExists();
    
    if ((null == tableId && exists) || (null != tableId && !exists)) {
      log.error("For table " + tableName + ": found table ID " + tableId + " and " + (exists ? "expect" : "did not expect") + " it to exist");
      throw new AccumuloException("Test and Accumulo state differ on " + tableName + " existence.");
    }
    
    boolean hasPermission;
    try {
      hasPermission = WalkingSecurity.get(state).canDeleteTable(new Credentials(principal, token).toThrift(state.getInstance()), tableId);
    } catch (Exception e) {
      if (!exists) {
        log.error("Ignoring exception checking permissions on non-existent table", e);
        return;
      }
      
      throw e;
    }
    
    try {
      conn.tableOperations().delete(tableName);
    } catch (AccumuloSecurityException ae) {
      if (ae.getSecurityErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
        if (hasPermission)
          throw new AccumuloException("Got a security exception when I should have had permission.", ae);
        else {
          // Drop anyway for sake of state
          state.getConnector().tableOperations().delete(tableName);
          WalkingSecurity.get(state).cleanTablePermissions(tableName);
          return;
        }
      } else if (ae.getSecurityErrorCode().equals(SecurityErrorCode.BAD_CREDENTIALS)) {
        if (WalkingSecurity.get(state).userPassTransient(conn.whoami()))
          return;
      }
      throw new AccumuloException("Got unexpected ae error code", ae);
    } catch (TableNotFoundException tnfe) {
      if (exists)
        throw new TableExistsException(null, tableName, "Got a TableNotFOundException but it should have existed", tnfe);
      else
        return;
    }
    WalkingSecurity.get(state).cleanTablePermissions(tableName);
    if (!hasPermission)
      throw new AccumuloException("Didn't get Security Exception when we should have");
  }
}
