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
import org.apache.accumulo.core.security.thrift.Credential;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class DropTable extends Test {
  
  @Override
  public void visit(State state, Properties props) throws Exception {
    dropTable(state, props);
  }
  
  public static void dropTable(State state, Properties props) throws Exception {
    String sourceUser = props.getProperty("source", "system");
    Credential auth;
    if (sourceUser.equals("table")) {
      auth = WalkingSecurity.get(state).getTabCredentials();
    } else {
      auth = WalkingSecurity.get(state).getSysCredentials();
    }
    Connector conn = state.getInstance().getConnector(auth);
    
    String tableName = WalkingSecurity.get(state).getTableName();
    
    boolean exists = WalkingSecurity.get(state).getTableExists();
    boolean hasPermission = WalkingSecurity.get(state).canDeleteTable(auth, tableName);
    
    try {
      conn.tableOperations().delete(tableName);
    } catch (AccumuloSecurityException ae) {
      if (ae.getErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
        if (hasPermission)
          throw new AccumuloException("Got a security exception when I should have had permission.", ae);
        else {
          // Drop anyway for sake of state
          state.getConnector().tableOperations().delete(tableName);
          WalkingSecurity.get(state).cleanTablePermissions(tableName);
          return;
        }
      } else if (ae.getErrorCode().equals(SecurityErrorCode.BAD_CREDENTIALS)) {
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
