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

import java.net.InetAddress;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;

public class AlterTable extends Test {

  @Override
  public void visit(State state, Properties props) throws Exception {
    Connector conn = state.getInstance().getConnector(WalkingSecurity.get(state).getSysUserName(), WalkingSecurity.get(state).getSysToken());

    String tableName = WalkingSecurity.get(state).getTableName();
    String namespaceName = WalkingSecurity.get(state).getNamespaceName();

    boolean exists = WalkingSecurity.get(state).getTableExists();
    boolean hasPermission = WalkingSecurity.get(state).canAlterTable(WalkingSecurity.get(state).getSysCredentials(), tableName, namespaceName);
    String newTableName = String.format("security_%s_%s_%d", InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_"), state.getPid(),
        System.currentTimeMillis());

    renameTable(conn, state, tableName, newTableName, hasPermission, exists);
  }

  public static void renameTable(Connector conn, State state, String oldName, String newName, boolean hasPermission, boolean tableExists)
      throws AccumuloSecurityException, AccumuloException, TableExistsException {
    try {
      conn.tableOperations().rename(oldName, newName);
    } catch (AccumuloSecurityException ae) {
      if (ae.getSecurityErrorCode().equals(SecurityErrorCode.PERMISSION_DENIED)) {
        if (hasPermission)
          throw new AccumuloException("Got a security exception when I should have had permission.", ae);
        else
          return;
      } else if (ae.getSecurityErrorCode().equals(SecurityErrorCode.BAD_CREDENTIALS)) {
        if (WalkingSecurity.get(state).userPassTransient(conn.whoami()))
          return;
      }
      throw new AccumuloException("Got unexpected ae error code", ae);
    } catch (TableNotFoundException tnfe) {
      if (tableExists)
        throw new TableExistsException(null, oldName, "Got a TableNotFoundException but it should exist", tnfe);
      else
        return;
    }
    WalkingSecurity.get(state).setTableName(newName);
    if (!hasPermission)
      throw new AccumuloException("Didn't get Security Exception when we should have");
  }
}
