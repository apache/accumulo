/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class AccumuloClientIT extends AccumuloClusterHarness {

  @AfterEach
  public void deleteUsers() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      Set<String> users = client.securityOperations().listLocalUsers();
      ClusterUser user1 = getUser(0);
      ClusterUser user2 = getUser(1);
      if (users.contains(user1.getPrincipal())) {
        client.securityOperations().dropLocalUser(user1.getPrincipal());
      }
      if (users.contains(user2.getPrincipal())) {
        client.securityOperations().dropLocalUser(user2.getPrincipal());
      }
    }
  }

  private interface CloseCheck {
    void check() throws Exception;
  }

  private static void expectClosed(CloseCheck cc) {
    var e = assertThrows(IllegalStateException.class, cc::check);
    assertTrue(e.getMessage().toLowerCase().contains("closed"));
  }

  @Test
  public void testAccumuloClientBuilder() throws Exception {
    AccumuloClient c = Accumulo.newClient().from(getClientProps()).build();
    String instanceName = getClientInfo().getInstanceName();
    String zookeepers = getClientInfo().getZooKeepers();

    ClusterUser testuser1 = getUser(0);
    final String user1 = testuser1.getPrincipal();
    final String password1 = testuser1.getPassword();
    c.securityOperations().createLocalUser(user1, new PasswordToken(password1));

    AccumuloClient client = Accumulo.newClient().to(instanceName, zookeepers).as(user1, password1)
        .zkTimeout(1234).build();

    Properties props = client.properties();
    assertFalse(props.containsKey(ClientProperty.AUTH_TOKEN.getKey()));
    ClientInfo info = ClientInfo.from(client.properties());
    assertEquals(instanceName, info.getInstanceName());
    assertEquals(zookeepers, info.getZooKeepers());
    assertEquals(user1, client.whoami());
    assertEquals(1234, info.getZooKeepersSessionTimeOut());

    props =
        Accumulo.newClientProperties().to(instanceName, zookeepers).as(user1, password1).build();
    assertTrue(props.containsKey(ClientProperty.AUTH_TOKEN.getKey()));
    assertEquals(password1, props.get(ClientProperty.AUTH_TOKEN.getKey()));
    assertEquals("password", props.get(ClientProperty.AUTH_TYPE.getKey()));
    assertEquals(instanceName, props.getProperty(ClientProperty.INSTANCE_NAME.getKey()));
    info = ClientInfo.from(props);
    assertEquals(instanceName, info.getInstanceName());
    assertEquals(zookeepers, info.getZooKeepers());
    assertEquals(user1, info.getPrincipal());
    assertTrue(info.getAuthenticationToken() instanceof PasswordToken);

    props = new Properties();
    props.put(ClientProperty.INSTANCE_NAME.getKey(), instanceName);
    props.put(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), zookeepers);
    props.put(ClientProperty.AUTH_PRINCIPAL.getKey(), user1);
    props.put(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getKey(), "22s");
    ClientProperty.setPassword(props, password1);
    client.close();
    client = Accumulo.newClient().from(props).build();

    info = ClientInfo.from(client.properties());
    assertEquals(instanceName, info.getInstanceName());
    assertEquals(zookeepers, info.getZooKeepers());
    assertEquals(user1, client.whoami());
    assertEquals(22000, info.getZooKeepersSessionTimeOut());

    ClusterUser testuser2 = getUser(1);
    final String user2 = testuser2.getPrincipal();
    final String password2 = testuser2.getPassword();
    c.securityOperations().createLocalUser(user2, new PasswordToken(password2));

    AccumuloClient client2 = Accumulo.newClient().from(client.properties())
        .as(user2, new PasswordToken(password2)).build();
    info = ClientInfo.from(client2.properties());
    assertEquals(instanceName, info.getInstanceName());
    assertEquals(zookeepers, info.getZooKeepers());
    assertEquals(user2, client2.whoami());
    assertEquals(user2, info.getPrincipal());

    c.close();
    client.close();
    client2.close();
  }

  @Test
  public void testClose() throws Exception {
    String tableName = getUniqueNames(1)[0];

    Scanner scanner;

    assertEquals(0, SingletonManager.getReservationCount());
    assertEquals(Mode.CLIENT, SingletonManager.getMode());

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      assertEquals(1, SingletonManager.getReservationCount());

      c.tableOperations().create(tableName);

      try (BatchWriter writer = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation("0001");
        m.at().family("f007").qualifier("q4").put("j");
        writer.addMutation(m);
      }

      scanner = c.createScanner(tableName, Authorizations.EMPTY);
    }

    // scanner created from closed client should fail
    expectClosed(() -> scanner.iterator().next());

    assertEquals(0, SingletonManager.getReservationCount());

    AccumuloClient c = Accumulo.newClient().from(getClientProps()).build();
    assertEquals(1, SingletonManager.getReservationCount());

    // ensure client created after everything was closed works
    Scanner scanner2 = c.createScanner(tableName, Authorizations.EMPTY);
    Entry<Key,Value> e = getOnlyElement(scanner2);
    assertEquals("0001", e.getKey().getRowData().toString());
    assertEquals("f007", e.getKey().getColumnFamilyData().toString());
    assertEquals("q4", e.getKey().getColumnQualifierData().toString());
    assertEquals("j", e.getValue().toString());

    // grab table ops before closing, will get an exception if trying to get it after closing
    TableOperations tops = c.tableOperations();

    c.close();

    assertEquals(0, SingletonManager.getReservationCount());

    expectClosed(() -> c.createScanner(tableName, Authorizations.EMPTY));
    expectClosed(() -> c.createConditionalWriter(tableName));
    expectClosed(() -> c.createBatchWriter(tableName));
    expectClosed(c::tableOperations);
    expectClosed(c::instanceOperations);
    expectClosed(c::securityOperations);
    expectClosed(c::namespaceOperations);
    expectClosed(c::properties);
    expectClosed(() -> c.instanceOperations().getInstanceId());

    // check a few table ops to ensure they fail
    expectClosed(() -> tops.create("expectFail"));
    expectClosed(() -> tops.cancelCompaction(tableName));
    expectClosed(() -> tops.listSplits(tableName));
  }
}
