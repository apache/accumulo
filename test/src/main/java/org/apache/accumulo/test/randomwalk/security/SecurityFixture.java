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

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.test.randomwalk.Environment;
import org.apache.accumulo.test.randomwalk.Fixture;
import org.apache.accumulo.test.randomwalk.State;

public class SecurityFixture extends Fixture {

  @Override
  public void setUp(State state, Environment env) throws Exception {
    String secTableName, systemUserName, tableUserName, secNamespaceName;
    // A best-effort sanity check to guard against not password-based auth
    ClientConfiguration clientConf = ClientConfiguration.loadDefault();
    if (clientConf.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
      throw new IllegalStateException("Security module currently cannot support Kerberos/SASL instances");
    }

    Connector conn = env.getConnector();

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

    WalkingSecurity.get(state, env).setTableName(secTableName);
    WalkingSecurity.get(state, env).setNamespaceName(secNamespaceName);
    state.set("rootUserPass", env.getToken());

    WalkingSecurity.get(state, env).setSysUserName(systemUserName);
    WalkingSecurity.get(state, env).createUser(systemUserName, sysUserPass);

    WalkingSecurity.get(state, env).changePassword(tableUserName, new PasswordToken(new byte[0]));

    WalkingSecurity.get(state, env).setTabUserName(tableUserName);

    for (TablePermission tp : TablePermission.values()) {
      WalkingSecurity.get(state, env).revokeTablePermission(systemUserName, secTableName, tp);
      WalkingSecurity.get(state, env).revokeTablePermission(tableUserName, secTableName, tp);
    }
    for (SystemPermission sp : SystemPermission.values()) {
      WalkingSecurity.get(state, env).revokeSystemPermission(systemUserName, sp);
      WalkingSecurity.get(state, env).revokeSystemPermission(tableUserName, sp);
    }
    WalkingSecurity.get(state, env).changeAuthorizations(tableUserName, new Authorizations());
  }

  @Override
  public void tearDown(State state, Environment env) throws Exception {
    log.debug("One last validate");
    Validate.validate(state, env, log);
    Connector conn = env.getConnector();

    if (WalkingSecurity.get(state, env).getTableExists()) {
      String secTableName = WalkingSecurity.get(state, env).getTableName();
      log.debug("Dropping tables: " + secTableName);

      conn.tableOperations().delete(secTableName);
    }

    if (WalkingSecurity.get(state, env).getNamespaceExists()) {
      String secNamespaceName = WalkingSecurity.get(state, env).getNamespaceName();
      log.debug("Dropping namespace: " + secNamespaceName);

      conn.namespaceOperations().delete(secNamespaceName);
    }

    if (WalkingSecurity.get(state, env).userExists(WalkingSecurity.get(state, env).getTabUserName())) {
      String tableUserName = WalkingSecurity.get(state, env).getTabUserName();
      log.debug("Dropping user: " + tableUserName);

      conn.securityOperations().dropLocalUser(tableUserName);
    }
    String systemUserName = WalkingSecurity.get(state, env).getSysUserName();
    log.debug("Dropping user: " + systemUserName);
    conn.securityOperations().dropLocalUser(systemUserName);
    WalkingSecurity.clearInstance();

    // Allow user drops to propagate, in case a new security test starts
    Thread.sleep(2000);
  }
}
