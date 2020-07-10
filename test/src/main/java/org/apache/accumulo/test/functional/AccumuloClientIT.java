/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

public class AccumuloClientIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(AccumuloClientIT.class);

  @After
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
    AccumuloClient client = Accumulo.newClient().from(getClientProps()).build();
    org.apache.accumulo.core.client.Connector c =
        org.apache.accumulo.core.client.Connector.from(client);
    assertEquals(client.whoami(), c.whoami());

    // this should cause the connector to stop functioning
    client.close();

    expectClosed(c::tableOperations);
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

    try (AccumuloClient c = Accumulo.newClient().from(getClientInfo().getProperties()).build()) {
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

    AccumuloClient c = Accumulo.newClient().from(getClientInfo().getProperties()).build();
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
    expectClosed(c::tableOperations);
    expectClosed(c::instanceOperations);
    expectClosed(c::securityOperations);
    expectClosed(c::namespaceOperations);
    expectClosed(c::properties);
    expectClosed(() -> c.instanceOperations().getInstanceID());

    // check a few table ops to ensure they fail
    expectClosed(() -> tops.create("expectFail"));
    expectClosed(() -> tops.cancelCompaction(tableName));
    expectClosed(() -> tops.listSplits(tableName));
  }

  @Test
  public void testAmpleReadTablets() throws Exception {
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      accumuloClient.securityOperations().grantTablePermission(accumuloClient.whoami(),
          MetadataTable.NAME, TablePermission.WRITE);
      BatchWriter bw =
          accumuloClient.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
      ClientContext cc = (ClientContext) accumuloClient;
      // Create a fake METADATA table with these splits
      String[] splits = {"a", "e", "j", "o", "t", "z"};
      // create metadata for a table "t" with the splits above
      TableId tableId = TableId.of("t");
      Text pr = null;
      for (String s : splits) {
        Text split = new Text(s);
        Mutation prevRow = KeyExtent.getPrevRowUpdateMutation(new KeyExtent(tableId, split, pr));
        prevRow.put(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME,
            new Text("123456"), new Value("127.0.0.1:1234"));
        MetadataSchema.TabletsSection.ChoppedColumnFamily.CHOPPED_COLUMN.put(prevRow,
            new Value("junk"));
        bw.addMutation(prevRow);
        pr = split;
      }
      // Add the default tablet
      Mutation defaultTablet = KeyExtent.getPrevRowUpdateMutation(new KeyExtent(tableId, null, pr));
      defaultTablet.put(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME,
          new Text("123456"), new Value("127.0.0.1:1234"));
      bw.addMutation(defaultTablet);
      bw.close();

      Text startRow = new Text("a");
      Text endRow = new Text("z");

      // Call up Ample from the client context using table "t" and build
      TabletsMetadata tablets = cc.getAmple().readTablets().forTable(tableId)
          .overlapping(startRow, endRow).fetch(FILES, LOCATION, LAST, PREV_ROW).build();

      TabletMetadata tabletMetadata0 = Iterables.get(tablets, 0);
      TabletMetadata tabletMetadata1 = Iterables.get(tablets, 1);
      TabletMetadata tabletMetadata2 = Iterables.get(tablets, 2);
      TabletMetadata tabletMetadata3 = Iterables.get(tablets, 3);
      TabletMetadata tabletMetadata4 = Iterables.get(tablets, 4);

      String infoTabletId0 = tabletMetadata0.getTableId().toString();
      String infoExtent0 = tabletMetadata0.getExtent().toString();
      String infoPrevEndRow0 = tabletMetadata0.getPrevEndRow().toString();
      String infoEndRow0 = tabletMetadata0.getEndRow().toString();

      String infoTabletId1 = tabletMetadata1.getTableId().toString();
      String infoExtent1 = tabletMetadata1.getExtent().toString();
      String infoPrevEndRow1 = tabletMetadata1.getPrevEndRow().toString();
      String infoEndRow1 = tabletMetadata1.getEndRow().toString();

      String infoTabletId2 = tabletMetadata2.getTableId().toString();
      String infoExtent2 = tabletMetadata2.getExtent().toString();
      String infoPrevEndRow2 = tabletMetadata2.getPrevEndRow().toString();
      String infoEndRow2 = tabletMetadata2.getEndRow().toString();

      String infoTabletId3 = tabletMetadata3.getTableId().toString();
      String infoExtent3 = tabletMetadata3.getExtent().toString();
      String infoPrevEndRow3 = tabletMetadata3.getPrevEndRow().toString();
      String infoEndRow3 = tabletMetadata3.getEndRow().toString();

      String infoTabletId4 = tabletMetadata4.getTableId().toString();
      String infoExtent4 = tabletMetadata4.getExtent().toString();
      String infoPrevEndRow4 = tabletMetadata4.getPrevEndRow().toString();
      String infoEndRow4 = tabletMetadata4.getEndRow().toString();

      String testInfoTabletId = "t";

      String testInfoKeyExtent0 = "t;e;a";
      String testInfoKeyExtent1 = "t;j;e";
      String testInfoKeyExtent2 = "t;o;j";
      String testInfoKeyExtent3 = "t;t;o";
      String testInfoKeyExtent4 = "t;z;t";

      String testInfoPrevEndRow0 = "a";
      String testInfoPrevEndRow1 = "e";
      String testInfoPrevEndRow2 = "j";
      String testInfoPrevEndRow3 = "o";
      String testInfoPrevEndRow4 = "t";

      String testInfoEndRow0 = "e";
      String testInfoEndRow1 = "j";
      String testInfoEndRow2 = "o";
      String testInfoEndRow3 = "t";
      String testInfoEndRow4 = "z";

      assertEquals(infoTabletId0, testInfoTabletId);
      assertEquals(infoTabletId1, testInfoTabletId);
      assertEquals(infoTabletId2, testInfoTabletId);
      assertEquals(infoTabletId3, testInfoTabletId);
      assertEquals(infoTabletId4, testInfoTabletId);

      assertEquals(infoExtent0, testInfoKeyExtent0);
      assertEquals(infoExtent1, testInfoKeyExtent1);
      assertEquals(infoExtent2, testInfoKeyExtent2);
      assertEquals(infoExtent3, testInfoKeyExtent3);
      assertEquals(infoExtent4, testInfoKeyExtent4);

      assertEquals(infoPrevEndRow0, testInfoPrevEndRow0);
      assertEquals(infoPrevEndRow1, testInfoPrevEndRow1);
      assertEquals(infoPrevEndRow2, testInfoPrevEndRow2);
      assertEquals(infoPrevEndRow3, testInfoPrevEndRow3);
      assertEquals(infoPrevEndRow4, testInfoPrevEndRow4);

      assertEquals(infoEndRow0, testInfoEndRow0);
      assertEquals(infoEndRow1, testInfoEndRow1);
      assertEquals(infoEndRow2, testInfoEndRow2);
      assertEquals(infoEndRow3, testInfoEndRow3);
      assertEquals(infoEndRow4, testInfoEndRow4);
    }
  }
}
