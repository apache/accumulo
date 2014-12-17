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
package org.apache.accumulo.minicluster;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
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
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MiniAccumuloClusterTest {

  public static File testDir;

  private static MiniAccumuloCluster accumulo;

  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    baseDir.mkdirs();
    testDir = new File(baseDir, MiniAccumuloClusterTest.class.getName());
    FileUtils.deleteQuietly(testDir);
    testDir.mkdir();

    MiniAccumuloConfig config = new MiniAccumuloConfig(testDir, "superSecret").setJDWPEnabled(true);
    config.setZooKeeperPort(0);
    HashMap<String,String> site = new HashMap<String,String>();
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

  @Test(timeout = 30000)
  public void test() throws Exception {
    Connector conn = accumulo.getConnector("root", "superSecret");

    conn.tableOperations().create("table1", true);

    conn.securityOperations().createLocalUser("user1", new PasswordToken("pass1"));
    conn.securityOperations().changeUserAuthorizations("user1", new Authorizations("A", "B"));
    conn.securityOperations().grantTablePermission("user1", "table1", TablePermission.WRITE);
    conn.securityOperations().grantTablePermission("user1", "table1", TablePermission.READ);

    IteratorSetting is = new IteratorSetting(10, SummingCombiner.class);
    SummingCombiner.setEncodingType(is, LongCombiner.Type.STRING);
    SummingCombiner.setColumns(is, Collections.singletonList(new IteratorSetting.Column("META", "COUNT")));

    conn.tableOperations().attachIterator("table1", is);

    Connector uconn = accumulo.getConnector("user1", "pass1");

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
        Assert.assertEquals("2", entry.getValue().toString());
      } else if (entry.getKey().getColumnQualifierData().toString().equals("SIZE")) {
        Assert.assertEquals("8", entry.getValue().toString());
      } else if (entry.getKey().getColumnQualifierData().toString().equals("CRC")) {
        Assert.assertEquals("123", entry.getValue().toString());
      } else {
        Assert.assertTrue(false);
      }
      count++;
    }

    Assert.assertEquals(3, count);

    count = 0;
    scanner = uconn.createScanner("table1", new Authorizations("A", "B"));
    for (Entry<Key,Value> entry : scanner) {
      if (entry.getKey().getColumnQualifierData().toString().equals("IMG")) {
        Assert.assertEquals("ABCDEFGH", entry.getValue().toString());
      }
      count++;
    }

    Assert.assertEquals(4, count);

    conn.tableOperations().delete("table1");
  }

  @Rule
  public TemporaryFolder folder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Test(timeout = 60000)
  public void testPerTableClasspath() throws Exception {

    Connector conn = accumulo.getConnector("root", "superSecret");

    conn.tableOperations().create("table2");

    File jarFile = folder.newFile("iterator.jar");
    FileUtils.copyURLToFile(this.getClass().getResource("/FooFilter.jar"), jarFile);

    conn.instanceOperations().setProperty(Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "cx1", jarFile.toURI().toString());
    conn.tableOperations().setProperty("table2", Property.TABLE_CLASSPATH.getKey(), "cx1");
    conn.tableOperations().attachIterator("table2", new IteratorSetting(100, "foocensor", "org.apache.accumulo.test.FooFilter"));

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
      Assert.assertFalse(entry.getKey().getRowData().toString().toLowerCase().contains("foo"));
      count++;
    }

    Assert.assertEquals(2, count);

    conn.instanceOperations().removeProperty(Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "cx1");
    conn.tableOperations().delete("table2");
  }

  @Test(timeout = 10000)
  public void testDebugPorts() {

    Set<Pair<ServerType,Integer>> debugPorts = accumulo.getDebugPorts();
    Assert.assertEquals(5, debugPorts.size());
    for (Pair<ServerType,Integer> debugPort : debugPorts) {
      Assert.assertTrue(debugPort.getSecond() > 0);
    }
  }

  @Test
  public void testConfig() {
    // ensure what user passed in is what comes back
    Assert.assertEquals(0, accumulo.getConfig().getZooKeeperPort());
    HashMap<String,String> site = new HashMap<String,String>();
    site.put(Property.TSERV_WORKQ_THREADS.getKey(), "2");
    Assert.assertEquals(site, accumulo.getConfig().getSiteConfig());
  }

  @Test
  public void testRandomPorts() throws Exception {
    File confDir = new File(testDir, "conf");
    File accumuloSite = new File(confDir, "accumulo-site.xml");
    Configuration conf = new Configuration(false);
    conf.addResource(accumuloSite.toURI().toURL());
    for (Property randomPortProp : new Property[] {Property.TSERV_CLIENTPORT, Property.MONITOR_PORT, Property.MONITOR_LOG4J_PORT, Property.MASTER_CLIENTPORT,
        Property.TRACE_PORT, Property.GC_PORT}) {
      String value = conf.get(randomPortProp.getKey());
      Assert.assertNotNull("Found no value for " + randomPortProp, value);
      Assert.assertEquals("0", value);
    }
  }

  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
  }

}
