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
import java.util.HashMap;

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
    
    SecurityHelper.setSystemConnector(state, sysConn);
    SecurityHelper.setSysUserName(state, systemUserName);
    SecurityHelper.setSysUserPass(state, sysUserPass);
    
    SecurityHelper.setTableExists(state, false);
    SecurityHelper.setTableExists(state, false);
    
    SecurityHelper.setTabUserPass(state, new byte[0]);
    
    SecurityHelper.setTableName(state, secTableName);
    SecurityHelper.setTabUserName(state, tableUserName);
    
    for (TablePermission tp : TablePermission.values()) {
      SecurityHelper.setTabPerm(state, systemUserName, tp, false);
      SecurityHelper.setTabPerm(state, tableUserName, tp, false);
    }
    for (SystemPermission sp : SystemPermission.values()) {
      SecurityHelper.setSysPerm(state, systemUserName, sp, false);
      SecurityHelper.setSysPerm(state, tableUserName, sp, false);
    }
    SecurityHelper.setUserAuths(state, tableUserName, new Authorizations());
    SecurityHelper.setAuthsMap(state, new HashMap<String,Integer>());
  }
  
  @Override
  public void tearDown(State state) throws Exception {
    log.debug("One last validate");
    Validate.validate(state, log);
    Connector conn = state.getConnector();
    
    if (SecurityHelper.getTableExists(state)) {
      String secTableName = SecurityHelper.getTableName(state);
      log.debug("Dropping tables: " + secTableName);
      
      conn.tableOperations().delete(secTableName);
    }
    
    if (SecurityHelper.getTabUserExists(state)) {
      String tableUserName = SecurityHelper.getTabUserName(state);
      log.debug("Dropping user: " + tableUserName);
      
      conn.securityOperations().dropUser(tableUserName);
    }
    String systemUserName = SecurityHelper.getSysUserName(state);
    log.debug("Dropping user: " + systemUserName);
    conn.securityOperations().dropUser(systemUserName);
    
  }
}
