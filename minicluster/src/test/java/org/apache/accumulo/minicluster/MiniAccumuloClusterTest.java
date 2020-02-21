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
package org.apache.accumulo.minicluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

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
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not set by user input")
public class MiniAccumuloClusterTest {

  public static File testDir;

  private static MiniAccumuloCluster accumulo;

  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());
    testDir = new File(baseDir, MiniAccumuloClusterTest.class.getName());
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir());

    MiniAccumuloConfig config = new MiniAccumuloConfig(testDir, "superSecret").setJDWPEnabled(true);
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
  @Test(timeout = 30000)
  public void test() throws Exception {
    org.apache.accumulo.core.client.Connector conn = accumulo.getConnector("root", "superSecret");

    conn.tableOperations().create("table1", new NewTableConfiguration());

    conn.securityOperations().createLocalUser("user1", new PasswordToken("pass1"));
    conn.securityOperations().changeUserAuthorizations("user1", new Authorizations("A", "B"));
    conn.securityOperations().grantTablePermission("user1", "table1", TablePermission.WRITE);
    conn.securityOperations().grantTablePermission("user1", "table1", TablePermission.READ);

    IteratorSetting is = new IteratorSetting(10, SummingCombiner.class);
    SummingCombiner.setEncodingType(is, LongCombiner.Type.STRING);
    SummingCombiner.setColumns(is,
        Collections.singletonList(new IteratorSetting.Column("META", "COUNT")));

    conn.tableOperations().attachIterator("table1", is);

    org.apache.accumulo.core.client.Connector uconn = accumulo.getConnector("user1", "pass1");

    BatchWriter bw = uconn.createBatchWriter("table1", new BatchWriterConfig());

    UUID uuid = UUID.randomUUID();

    Mutation m = new Mutation(uuid.toString());
    m.put("META", "SIZE", new ColumnVisibility("A|B"), "8");
    m.put("META", "CRC", new ColumnVisibility("A|B"), "456");
    m.put("META", "COUNT", new ColumnVisibility("A|B"), "1");
    m.put("DATA", "IMG", new ColumnVisibility("A&B"), "ABCDEFGH");

    bw.addMutation(m);
    bw.flush();

    m = new Mutation(uuid.toString());
    m.put("META", "COUNT", new ColumnVisibility("A|B"), "1");
    m.put("META", "CRC", new ColumnVisibility("A|B"), "123");
    bw.addMutation(m);

    bw.close();

    int count = 0;
    Scanner scanner = uconn.createScanner("table1", new Authorizations("A"));
    for (Entry<Key,Value> entry : scanner) {
      if (entry.getKey().getColumnQualifierData().toString().equals("COUNT")) {
        assertEquals("2", entry.getValue().toString());
      } else if (entry.getKey().getColumnQualifierData().toString().equals("SIZE")) {
        assertEquals("8", entry.getValue().toString());
      } else if (entry.getKey().getColumnQualifierData().toString().equals("CRC")) {
        assertEquals("123", entry.getValue().toString());
      } else {
        fail();
      }
      count++;
    }

    assertEquals(3, count);

    count = 0;
    scanner = uconn.createScanner("table1", new Authorizations("A", "B"));
    for (Entry<Key,Value> entry : scanner) {
      if (entry.getKey().getColumnQualifierData().toString().equals("IMG")) {
        assertEquals("ABCDEFGH", entry.getValue().toString());
      }
      count++;
    }

    assertEquals(4, count);

    conn.tableOperations().delete("table1");
  }

  @Rule
  public TemporaryFolder folder =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @SuppressWarnings("deprecation")
  @Test(timeout = 60000)
  public void testPerTableClasspath() throws Exception {

    org.apache.accumulo.core.client.Connector conn = accumulo.getConnector("root", "superSecret");

    conn.tableOperations().create("table2");

    File jarFile = folder.newFile("iterator.jar");
    FileUtils.copyURLToFile(this.getClass().getResource("/FooFilter.jar"), jarFile);

    conn.instanceOperations().setProperty(Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "cx1",
        jarFile.toURI().toString());
    conn.tableOperations().setProperty("table2", Property.TABLE_CLASSPATH.getKey(), "cx1");
    conn.tableOperations().attachIterator("table2",
        new IteratorSetting(100, "foocensor", "org.apache.accumulo.test.FooFilter"));

    BatchWriter bw = conn.createBatchWriter("table2", new BatchWriterConfig());

    Mutation m1 = new Mutation("foo");
    m1.put("cf1", "cq1", "v2");
    m1.put("cf1", "cq2", "v3");

    bw.addMutation(m1);

    Mutation m2 = new Mutation("bar");
    m2.put("cf1", "cq1", "v6");
    m2.put("cf1", "cq2", "v7");

    bw.addMutation(m2);

    bw.close();

    Scanner scanner = conn.createScanner("table2", new Authorizations());

    int count = 0;
    for (Entry<Key,Value> entry : scanner) {
      assertFalse(entry.getKey().getRowData().toString().toLowerCase().contains("foo"));
      count++;
    }

    assertEquals(2, count);

    conn.instanceOperations()
        .removeProperty(Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "cx1");
    conn.tableOperations().delete("table2");
  }

  @Test(timeout = 10000)
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
    FileBasedConfigurationBuilder<PropertiesConfiguration> propsBuilder =
        new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
            .configure(new Parameters().properties().setFile(accumuloProps));
    PropertiesConfiguration conf = propsBuilder.getConfiguration();
    for (Property randomPortProp : new Property[] {Property.TSERV_CLIENTPORT, Property.MONITOR_PORT,
        Property.MASTER_CLIENTPORT, Property.TRACE_PORT, Property.GC_PORT}) {
      String value = conf.getString(randomPortProp.getKey());
      assertNotNull("Found no value for " + randomPortProp, value);
      assertEquals("0", value);
    }
  }

  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
  }

}
