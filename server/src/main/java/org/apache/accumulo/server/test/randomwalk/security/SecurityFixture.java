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

import java.net.InetAddress;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.server.test.randomwalk.Fixture;
import org.apache.accumulo.server.test.randomwalk.State;

public class SecurityFixture extends Fixture {
  
  @Override
  public void setUp(State state) throws Exception {
    String secTableName, systemUserName, tableUserName;
    Connector sysConn;
    
    Connector conn = state.getConnector();
    Instance instance = state.getInstance();
    
    String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");
    
    systemUserName = String.format("system_%s_%s_%d", hostname, state.getPid(), System.currentTimeMillis());
    tableUserName = String.format("table_%s_%s_%d", hostname, state.getPid(), System.currentTimeMillis());
    secTableName = String.format("security_%s_%s_%d", hostname, state.getPid(), System.currentTimeMillis());
    
    byte[] sysUserPass = "sysUser".getBytes();
    conn.securityOperations().createUser(systemUserName, sysUserPass, new Authorizations());
    sysConn = instance.getConnector(systemUserName, sysUserPass);
    
    WalkingSecurity.get(state).createUser(systemUserName, sysUserPass);
    
    WalkingSecurity.get(state).changePassword(tableUserName, new byte[0]);
    WalkingSecurity.get(state).setSystemConnector(sysConn);
    
    WalkingSecurity.get(state).setTableName(secTableName);
    WalkingSecurity.get(state).setTabUserName(tableUserName);
    
    for (TablePermission tp : TablePermission.values()) {
      WalkingSecurity.get(state).revokeTablePermission(systemUserName, secTableName, tp);
      WalkingSecurity.get(state).revokeTablePermission(tableUserName, secTableName, tp);
    }
    for (SystemPermission sp : SystemPermission.values()) {
      WalkingSecurity.get(state).revokeSystemPermission(systemUserName, sp);
      WalkingSecurity.get(state).revokeSystemPermission(tableUserName, sp);
    }
    WalkingSecurity.get(state).changeAuthorizations(tableUserName, new Authorizations());
  }
  
  @Override
  public void tearDown(State state) throws Exception {
    log.debug("One last validate");
    Validate.validate(state, log);
    Connector conn = state.getConnector();
    
    if (WalkingSecurity.get(state).getTableExists()) {
      String secTableName = WalkingSecurity.get(state).getTableName();
      log.debug("Dropping tables: " + secTableName);
      
      conn.tableOperations().delete(secTableName);
    }
    
    if (WalkingSecurity.get(state).userExists(WalkingSecurity.get(state).getTabUserName())) {
      String tableUserName = WalkingSecurity.get(state).getTabUserName();
      log.debug("Dropping user: " + tableUserName);
      
      conn.securityOperations().dropUser(tableUserName);
    }
    String systemUserName = WalkingSecurity.get(state).getSysUserName();
    log.debug("Dropping user: " + systemUserName);
    conn.securityOperations().dropUser(systemUserName);
    
  }
}
