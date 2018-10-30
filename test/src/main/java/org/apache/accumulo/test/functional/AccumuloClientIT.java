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
import static org.junit.Assert.fail;

import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class AccumuloClientIT extends AccumuloClusterHarness {

  private static interface CloseCheck {
    void check() throws Exception;
  }

  private static void expectClosed(CloseCheck cc) throws Exception {
    try {
      cc.check();
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().toLowerCase().contains("closed"));
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testGetConnectorFromAccumuloClient() throws Exception {
    AccumuloClient client = getAccumuloClient();
    org.apache.accumulo.core.client.Connector c = org.apache.accumulo.core.client.Connector
        .from(client);
    assertEquals(client.whoami(), c.whoami());

    // this should cause the connector to stop functioning
    client.close();

    expectClosed(() -> c.tableOperations());
  }

  @Test
  public void testclientectorBuilder() throws Exception {
    AccumuloClient c = getAccumuloClient();
    String instanceName = c.info().getInstanceName();
    String zookeepers = c.info().getZooKeepers();
    final String user = "testuser";
    final String password = "testpassword";
    c.securityOperations().createLocalUser(user, new PasswordToken(password));

    AccumuloClient client = Accumulo.newClient().forInstance(instanceName, zookeepers)
        .usingPassword(user, password).withZkTimeout(1234).build();

    assertEquals(instanceName, client.info().getInstanceName());
    assertEquals(zookeepers, client.info().getZooKeepers());
    assertEquals(user, client.whoami());
    assertEquals(1234, client.info().getZooKeepersSessionTimeOut());

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
    client.close();
    client = Accumulo.newClient().usingProperties(props).build();

    assertEquals(instanceName, client.info().getInstanceName());
    assertEquals(zookeepers, client.info().getZooKeepers());
    assertEquals(user, client.whoami());
    assertEquals(22000, client.info().getZooKeepersSessionTimeOut());

    final String user2 = "testuser2";
    final String password2 = "testpassword2";
    c.securityOperations().createLocalUser(user2, new PasswordToken(password2));

    AccumuloClient client2 = Accumulo.newClient().usingClientInfo(client.info())
        .usingToken(user2, new PasswordToken(password2)).build();
    assertEquals(instanceName, client2.info().getInstanceName());
    assertEquals(zookeepers, client2.info().getZooKeepers());
    assertEquals(user2, client2.whoami());
    info = client2.info();
    assertEquals(instanceName, info.getInstanceName());
    assertEquals(zookeepers, info.getZooKeepers());
    assertEquals(user2, info.getPrincipal());

    final String user3 = "testuser3";
    final String password3 = "testpassword3";
    c.securityOperations().createLocalUser(user3, new PasswordToken(password3));

    AccumuloClient client3 = client.changeUser(user3, new PasswordToken(password3));
    assertEquals(instanceName, client3.info().getInstanceName());
    assertEquals(zookeepers, client3.info().getZooKeepers());
    assertEquals(user3, client3.whoami());
    info = client3.info();
    assertEquals(instanceName, info.getInstanceName());
    assertEquals(zookeepers, info.getZooKeepers());
    assertEquals(user3, info.getPrincipal());

    c.close();
    client.close();
    client2.close();
    client3.close();
  }

  @Test
  public void testClose() throws Exception {
    String tableName = getUniqueNames(1)[0];

    Scanner scanner;

    assertEquals(0, SingletonManager.getReservationCount());
    assertEquals(Mode.CLIENT, SingletonManager.getMode());

    try (AccumuloClient c = Accumulo.newClient().usingClientInfo(getClientInfo()).build()) {
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

    AccumuloClient c = Accumulo.newClient().usingClientInfo(getClientInfo()).build();
    assertEquals(1, SingletonManager.getReservationCount());

    // ensure client created after everything was closed works
    Scanner scanner2 = c.createScanner(tableName, Authorizations.EMPTY);
    Entry<Key,Value> e = Iterables.getOnlyElement(scanner2);
    assertEquals("0001", e.getKey().getRowData().toString());
    assertEquals("f007", e.getKey().getColumnFamilyData().toString());
    assertEquals("q4", e.getKey().getColumnQualifierData().toString());
    assertEquals("j", e.getValue().toString());

    // grab table ops before closing, will get an exception if trying to get it after closing
    TableOperations tops = c.tableOperations();

    c.close();

    assertEquals(0, SingletonManager.getReservationCount());

    expectClosed(() -> c.createScanner(tableName, Authorizations.EMPTY));
    expectClosed(() -> c.createConditionalWriter(tableName, new ConditionalWriterConfig()));
    expectClosed(() -> c.createBatchWriter(tableName));
    expectClosed(() -> c.tableOperations());
    expectClosed(() -> c.instanceOperations());
    expectClosed(() -> c.securityOperations());
    expectClosed(() -> c.namespaceOperations());
    expectClosed(() -> c.info());
    expectClosed(() -> c.getInstanceID());
    expectClosed(() -> c.changeUser("root", new PasswordToken("secret")));

    // check a few table ops to ensure they fail
    expectClosed(() -> tops.create("expectFail"));
    expectClosed(() -> tops.cancelCompaction(tableName));
    expectClosed(() -> tops.listSplits(tableName));

  }
}
