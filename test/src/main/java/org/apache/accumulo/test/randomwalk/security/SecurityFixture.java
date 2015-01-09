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
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.test.randomwalk.Fixture;
import org.apache.accumulo.test.randomwalk.State;

public class SecurityFixture extends Fixture {

  @Override
  public void setUp(State state) throws Exception {
    String secTableName, systemUserName, tableUserName, secNamespaceName;
    Connector conn = state.getConnector();

    String hostname = InetAddress.getLocalHost().getHostName().replaceAll("[-.]", "_");

    systemUserName = String.format("system_%s", hostname);
    tableUserName = String.format("table_%s", hostname);
    secTableName = String.format("security_%s", hostname);
    secNamespaceName = String.format("securityNs_%s", hostname);

    if (conn.tableOperations().exists(secTableName))
      conn.tableOperations().delete(secTableName);
    Set<String> users = conn.securityOperations().listLocalUsers();
    if (users.contains(tableUserName))
      conn.securityOperations().dropLocalUser(tableUserName);
    if (users.contains(systemUserName))
      conn.securityOperations().dropLocalUser(systemUserName);

    PasswordToken sysUserPass = new PasswordToken("sysUser");
    conn.securityOperations().createLocalUser(systemUserName, sysUserPass);

    WalkingSecurity.get(state).setTableName(secTableName);
    WalkingSecurity.get(state).setNamespaceName(secNamespaceName);
    state.set("rootUserPass", state.getCredentials().getToken());

    WalkingSecurity.get(state).setSysUserName(systemUserName);
    WalkingSecurity.get(state).createUser(systemUserName, sysUserPass);

    WalkingSecurity.get(state).changePassword(tableUserName, new PasswordToken(new byte[0]));

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

    if (WalkingSecurity.get(state).getNamespaceExists()) {
      String secNamespaceName = WalkingSecurity.get(state).getNamespaceName();
      log.debug("Dropping namespace: " + secNamespaceName);

      conn.namespaceOperations().delete(secNamespaceName);
    }

    if (WalkingSecurity.get(state).userExists(WalkingSecurity.get(state).getTabUserName())) {
      String tableUserName = WalkingSecurity.get(state).getTabUserName();
      log.debug("Dropping user: " + tableUserName);

      conn.securityOperations().dropLocalUser(tableUserName);
    }
    String systemUserName = WalkingSecurity.get(state).getSysUserName();
    log.debug("Dropping user: " + systemUserName);
    conn.securityOperations().dropLocalUser(systemUserName);
    WalkingSecurity.clearInstance();

    // Allow user drops to propagate, in case a new security test starts
    Thread.sleep(2000);
  }
}
