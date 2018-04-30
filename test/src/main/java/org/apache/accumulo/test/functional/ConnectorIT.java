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

import org.apache.accumulo.core.client.ConnectionInfo;
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
    String instanceName = c.getInstance().getInstanceName();
    String zookeepers = c.getInstance().getZooKeepers();
    final String user = "testuser";
    final String password = "testpassword";
    c.securityOperations().createLocalUser(user, new PasswordToken(password));

    Connector conn = Connector.builder().forInstance(instanceName, zookeepers)
        .usingPassword(user, password).build();

    Assert.assertEquals(instanceName, conn.getInstance().getInstanceName());
    Assert.assertEquals(zookeepers, conn.getInstance().getZooKeepers());
    Assert.assertEquals(user, conn.whoami());

    ConnectionInfo info = Connector.builder().forInstance(instanceName, zookeepers)
        .usingPassword(user, password).info();
    Assert.assertEquals(instanceName, info.getInstanceName());
    Assert.assertEquals(zookeepers, info.getZooKeepers());
    Assert.assertEquals(user, info.getPrincipal());
    Assert.assertTrue(info.getAuthenticationToken() instanceof PasswordToken);

    Properties props = new Properties();
    props.put(ClientProperty.INSTANCE_NAME.getKey(), instanceName);
    props.put(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), zookeepers);
    props.put(ClientProperty.AUTH_USERNAME.getKey(), user);
    props.put(ClientProperty.AUTH_PASSWORD.getKey(), password);
    conn = Connector.builder().usingProperties(props).build();

    Assert.assertEquals(instanceName, conn.getInstance().getInstanceName());
    Assert.assertEquals(zookeepers, conn.getInstance().getZooKeepers());
    Assert.assertEquals(user, conn.whoami());

    final String user2 = "testuser2";
    final String password2 = "testpassword2";
    c.securityOperations().createLocalUser(user2, new PasswordToken(password2));

    Connector conn2 = conn.createConnector(user2, new PasswordToken(password2));
    Assert.assertEquals(instanceName, conn2.getInstance().getInstanceName());
    Assert.assertEquals(zookeepers, conn2.getInstance().getZooKeepers());
    Assert.assertEquals(user2, conn2.whoami());
    info = conn2.info();
    Assert.assertEquals(instanceName, info.getInstanceName());
    Assert.assertEquals(zookeepers, info.getZooKeepers());
    Assert.assertEquals(user2, info.getPrincipal());
  }
}
