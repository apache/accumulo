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
package org.apache.accumulo.minicluster;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class MiniAccumuloClusterTest extends WithTestNames {

  public static final String ROOT_PASSWORD = "superSecret";
  public static final String ROOT_USER = "root";

  public static File testDir;

  private static MiniAccumuloCluster accumulo;

  @BeforeAll
  public static void setupMiniCluster() throws Exception {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());
    testDir = new File(baseDir, MiniAccumuloClusterTest.class.getName());
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir());

    MiniAccumuloConfig config = new MiniAccumuloConfig(testDir, ROOT_PASSWORD).setJDWPEnabled(true);
    config.setZooKeeperPort(0);
    HashMap<String,String> site = new HashMap<>();
    site.put(Property.TSERV_WORKQ_THREADS.getKey(), "2");
    config.setSiteConfig(site);
    accumulo = new MiniAccumuloCluster(config);
    accumulo.start();
  }

  @Test
  public void checkDFSConstants() {
    // check for unexpected changes in static constants because these will be inlined
    // and we won't otherwise know that they won't work on a particular version
    assertEquals("dfs.namenode.name.dir", DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
    assertEquals("dfs.datanode.data.dir", DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);
    assertEquals("dfs.replication", DFSConfigKeys.DFS_REPLICATION_KEY);
  }

  @SuppressWarnings("deprecation")
  @Test
  @Timeout(30)
  public void test() throws Exception {
    try (AccumuloClient conn = Accumulo.newClient().from(accumulo.getClientProperties())
        .as(ROOT_USER, ROOT_PASSWORD).build()) {

      final String tableName = testName();

      IteratorSetting is = new IteratorSetting(10, SummingCombiner.class);
      SummingCombiner.setEncodingType(is, LongCombiner.Type.STRING);
      SummingCombiner.setColumns(is,
          Collections.singletonList(new IteratorSetting.Column("META", "COUNT")));

      conn.tableOperations().create(tableName, new NewTableConfiguration().attachIterator(is));

      final String principal = "user1";
      final String password = "pass1";
      conn.securityOperations().createLocalUser(principal, new PasswordToken(password));
      conn.securityOperations().changeUserAuthorizations(principal, new Authorizations("A", "B"));
      conn.securityOperations().grantTablePermission(principal, tableName, TablePermission.WRITE);
      conn.securityOperations().grantTablePermission(principal, tableName, TablePermission.READ);

      try (AccumuloClient uconn = Accumulo.newClient().from(accumulo.getClientProperties())
          .as(principal, password).build()) {

        try (BatchWriter bw = uconn.createBatchWriter(tableName, new BatchWriterConfig())) {

          UUID uuid = UUID.randomUUID();

          ColumnVisibility colVisAorB = new ColumnVisibility("A|B");
          Mutation m = new Mutation(uuid.toString());
          m.put("META", "SIZE", colVisAorB, "8");
          m.put("META", "CRC", colVisAorB, "456");
          m.put("META", "COUNT", colVisAorB, "1");
          m.put("DATA", "IMG", new ColumnVisibility("A&B"), "ABCDEFGH");

          bw.addMutation(m);
          bw.flush();

          m = new Mutation(uuid.toString());
          m.put("META", "COUNT", colVisAorB, "1");
          m.put("META", "CRC", colVisAorB, "123");
          bw.addMutation(m);

        }

        int count = 0;
        try (Scanner scanner = uconn.createScanner(tableName, new Authorizations("A"))) {
          for (Entry<Key,Value> entry : scanner) {
            final String actualValue = entry.getValue().toString();
            switch (entry.getKey().getColumnQualifierData().toString()) {
              case "COUNT":
                assertEquals("2", actualValue);
                break;
              case "SIZE":
                assertEquals("8", actualValue);
                break;
              case "CRC":
                assertEquals("123", actualValue);
                break;
              default:
                fail();
                break;
            }
            count++;
          }
        }
        assertEquals(3, count);

        count = 0;
        try (Scanner scanner = uconn.createScanner(tableName, new Authorizations("A", "B"))) {
          for (Entry<Key,Value> entry : scanner) {
            if (entry.getKey().getColumnQualifierData().toString().equals("IMG")) {
              assertEquals("ABCDEFGH", entry.getValue().toString());
            }
            count++;
          }
        }

        assertEquals(4, count);
      }
      conn.tableOperations().delete(tableName);
    }

  }

  @Test
  @Timeout(10)
  public void testDebugPorts() {

    Set<Pair<ServerType,Integer>> debugPorts = accumulo.getDebugPorts();
    assertEquals(5, debugPorts.size());
    for (Pair<ServerType,Integer> debugPort : debugPorts) {
      assertTrue(debugPort.getSecond() > 0);
    }
  }

  @Test
  public void testConfig() {
    // ensure what user passed in is what comes back
    assertEquals(0, accumulo.getConfig().getZooKeeperPort());
    HashMap<String,String> site = new HashMap<>();
    site.put(Property.TSERV_WORKQ_THREADS.getKey(), "2");
    assertEquals(site, accumulo.getConfig().getSiteConfig());
  }

  @Test
  public void testRandomPorts() throws Exception {
    File confDir = new File(testDir, "conf");
    File accumuloProps = new File(confDir, "accumulo.properties");
    var config = new PropertiesConfiguration();
    try (var reader = new FileReader(accumuloProps, UTF_8)) {
      config.read(reader);
    }
    for (Property randomPortProp : new Property[] {Property.TSERV_CLIENTPORT, Property.MONITOR_PORT,
        Property.MANAGER_CLIENTPORT, Property.GC_PORT}) {
      String value = config.getString(randomPortProp.getKey());
      assertNotNull(value, "Found no value for " + randomPortProp);
      assertEquals("0", value);
    }
  }

  @AfterAll
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
  }

}
