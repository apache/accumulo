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
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.test.randomwalk.Environment;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class CreateTable extends Test {

  @Override
  public void visit(State state, Environment env, Properties props) throws Exception {
    Connector conn = env.getInstance().getConnector(WalkingSecurity.get(state, env).getSysUserName(), WalkingSecurity.get(state, env).getSysToken());

    String tableName = WalkingSecurity.get(state, env).getTableName();

    boolean exists = WalkingSecurity.get(state, env).getTableExists();
    boolean hasPermission = WalkingSecurity.get(state, env).canCreateTable(WalkingSecurity.get(state, env).getSysCredentials(), null, null);

    try {
      conn.tableOperations().create(tableName);
    } catch (AccumuloSecurityException ae) {
      if (ae.getSecurityErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
        if (hasPermission)
          throw new AccumuloException("Got a security exception when I should have had permission.", ae);
        else {
          // create table anyway for sake of state
          try {
            env.getConnector().tableOperations().create(tableName);
            WalkingSecurity.get(state, env).initTable(tableName);
          } catch (TableExistsException tee) {
            if (exists)
              return;
            else
              throw new AccumuloException("Test and Accumulo are out of sync");
          }
          return;
        }
      } else
        throw new AccumuloException("Got unexpected error", ae);
    } catch (TableExistsException tee) {
      if (!exists)
        throw new TableExistsException(null, tableName, "Got a TableExistsException but it shouldn't have existed", tee);
      else
        return;
    }
    WalkingSecurity.get(state, env).initTable(tableName);
    for (TablePermission tp : TablePermission.values())
      WalkingSecurity.get(state, env).grantTablePermission(conn.whoami(), tableName, tp);
    if (!hasPermission)
      throw new AccumuloException("Didn't get Security Exception when we should have");
  }
}
