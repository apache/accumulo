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
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.Test;

public class AccumuloClientIT extends AccumuloClusterHarness {

  @Test
  public void testConnectorBuilder() throws Exception {
    AccumuloClient c = getAccumuloClient();
    String instanceName = c.info().getInstanceName();
    String zookeepers = c.info().getZooKeepers();
    final String user = "testuser";
    final String password = "testpassword";
    c.securityOperations().createLocalUser(user, new PasswordToken(password));

    AccumuloClient conn = Accumulo.newClient().forInstance(instanceName, zookeepers)
        .usingPassword(user, password).withZkTimeout(1234).build();

    assertEquals(instanceName, conn.info().getInstanceName());
    assertEquals(zookeepers, conn.info().getZooKeepers());
    assertEquals(user, conn.whoami());
    assertEquals(1234, conn.info().getZooKeepersSessionTimeOut());

    ClientInfo info = Accumulo.newClient().forInstance(instanceName, zookeepers)
        .usingPassword(user, password).info();
    assertEquals(instanceName, info.getInstanceName());
    assertEquals(zookeepers, info.getZooKeepers());
    assertEquals(user, info.getPrincipal());
    assertTrue(info.getAuthenticationToken() instanceof PasswordToken);

    Properties props = new Properties();
    props.put(ClientProperty.INSTANCE_NAME.getKey(), instanceName);
    props.put(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), zookeepers);
    props.put(ClientProperty.AUTH_PRINCIPAL.getKey(), user);
    props.put(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getKey(), "22s");
    ClientProperty.setPassword(props, password);
    conn = Accumulo.newClient().usingProperties(props).build();

    assertEquals(instanceName, conn.info().getInstanceName());
    assertEquals(zookeepers, conn.info().getZooKeepers());
    assertEquals(user, conn.whoami());
    assertEquals(22000, conn.info().getZooKeepersSessionTimeOut());

    final String user2 = "testuser2";
    final String password2 = "testpassword2";
    c.securityOperations().createLocalUser(user2, new PasswordToken(password2));

    AccumuloClient conn2 = Accumulo.newClient().usingClientInfo(conn.info())
        .usingToken(user2, new PasswordToken(password2)).build();
    assertEquals(instanceName, conn2.info().getInstanceName());
    assertEquals(zookeepers, conn2.info().getZooKeepers());
    assertEquals(user2, conn2.whoami());
    info = conn2.info();
    assertEquals(instanceName, info.getInstanceName());
    assertEquals(zookeepers, info.getZooKeepers());
    assertEquals(user2, info.getPrincipal());

    final String user3 = "testuser3";
    final String password3 = "testpassword3";
    c.securityOperations().createLocalUser(user3, new PasswordToken(password3));

    AccumuloClient conn3 = conn.changeUser(user3, new PasswordToken(password3));
    assertEquals(instanceName, conn3.info().getInstanceName());
    assertEquals(zookeepers, conn3.info().getZooKeepers());
    assertEquals(user3, conn3.whoami());
    info = conn3.info();
    assertEquals(instanceName, info.getInstanceName());
    assertEquals(zookeepers, info.getZooKeepers());
    assertEquals(user3, info.getPrincipal());
  }
}
