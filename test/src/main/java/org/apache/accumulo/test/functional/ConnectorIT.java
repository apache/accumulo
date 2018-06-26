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

import java.util.Properties;

import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.Assert;
import org.junit.Test;

public class ConnectorIT extends AccumuloClusterHarness {

  @Test
  public void testConnectorBuilder() throws Exception {
    Connector c = getConnector();
    String instanceName = c.info().getInstanceName();
    String zookeepers = c.info().getZooKeepers();
    final String user = "testuser";
    final String password = "testpassword";
    c.securityOperations().createLocalUser(user, new PasswordToken(password));

    Connector conn = Connector.builder().forInstance(instanceName, zookeepers)
        .usingPassword(user, password).withZkTimeout(1234).build();

    Assert.assertEquals(instanceName, conn.info().getInstanceName());
    Assert.assertEquals(zookeepers, conn.info().getZooKeepers());
    Assert.assertEquals(user, conn.whoami());
    Assert.assertEquals(1234, conn.info().getZooKeepersSessionTimeOut());

    ClientInfo info = Connector.builder().forInstance(instanceName, zookeepers)
        .usingPassword(user, password).info();
    Assert.assertEquals(instanceName, info.getInstanceName());
    Assert.assertEquals(zookeepers, info.getZooKeepers());
    Assert.assertEquals(user, info.getPrincipal());
    Assert.assertTrue(info.getAuthenticationToken() instanceof PasswordToken);

    Properties props = new Properties();
    props.put(ClientProperty.INSTANCE_NAME.getKey(), instanceName);
    props.put(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), zookeepers);
    props.put(ClientProperty.AUTH_PRINCIPAL.getKey(), user);
    props.put(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getKey(), "22s");
    ClientProperty.setPassword(props, password);
    conn = Connector.builder().usingProperties(props).build();

    Assert.assertEquals(instanceName, conn.info().getInstanceName());
    Assert.assertEquals(zookeepers, conn.info().getZooKeepers());
    Assert.assertEquals(user, conn.whoami());
    Assert.assertEquals(22000, conn.info().getZooKeepersSessionTimeOut());

    final String user2 = "testuser2";
    final String password2 = "testpassword2";
    c.securityOperations().createLocalUser(user2, new PasswordToken(password2));

    Connector conn2 = Connector.builder().usingClientInfo(conn.info())
        .usingToken(user2, new PasswordToken(password2)).build();
    Assert.assertEquals(instanceName, conn2.info().getInstanceName());
    Assert.assertEquals(zookeepers, conn2.info().getZooKeepers());
    Assert.assertEquals(user2, conn2.whoami());
    info = conn2.info();
    Assert.assertEquals(instanceName, info.getInstanceName());
    Assert.assertEquals(zookeepers, info.getZooKeepers());
    Assert.assertEquals(user2, info.getPrincipal());

    final String user3 = "testuser3";
    final String password3 = "testpassword3";
    c.securityOperations().createLocalUser(user3, new PasswordToken(password3));

    Connector conn3 = conn.changeUser(user3, new PasswordToken(password3));
    Assert.assertEquals(instanceName, conn3.info().getInstanceName());
    Assert.assertEquals(zookeepers, conn3.info().getZooKeepers());
    Assert.assertEquals(user3, conn3.whoami());
    info = conn3.info();
    Assert.assertEquals(instanceName, info.getInstanceName());
    Assert.assertEquals(zookeepers, info.getZooKeepers());
    Assert.assertEquals(user3, info.getPrincipal());
  }
}
