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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessNotFoundException;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.Assert;
import org.junit.Test;

public class GarbageCollectorIT extends ConfigurableMacIT {
  private static final String OUR_SECRET = "itsreallysecret";

  @Override
  public int defaultTimeoutSeconds() {
    return 5 * 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> settings = new HashMap<String,String>();
    settings.put(Property.INSTANCE_SECRET.getKey(), OUR_SECRET);
    settings.put(Property.GC_CYCLE_START.getKey(), "1");
    settings.put(Property.GC_CYCLE_DELAY.getKey(), "1");
    settings.put(Property.GC_PORT.getKey(), "0");
    settings.put(Property.TSERV_MAXMEM.getKey(), "5K");
    settings.put(Property.TSERV_MAJC_DELAY.getKey(), "1");
    cfg.setSiteConfig(settings);

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  private void killMacGc() throws ProcessNotFoundException, InterruptedException, KeeperException {
    // kill gc started by MAC
    getCluster().killProcess(ServerType.GARBAGE_COLLECTOR, getCluster().getProcesses().get(ServerType.GARBAGE_COLLECTOR).iterator().next());
    // delete lock in zookeeper if there, this will allow next GC to start quickly
    String path = ZooUtil.getRoot(new ZooKeeperInstance(getCluster().getClientConfig())) + Constants.ZGC_LOCK;
    ZooReaderWriter zk = new ZooReaderWriter(cluster.getZooKeepers(), 30000, OUR_SECRET);
    try {
      ZooLock.deleteLock(zk, path);
    } catch (IllegalStateException e) {

    }

    assertNull(getCluster().getProcesses().get(ServerType.GARBAGE_COLLECTOR));
  }

  @Test
  public void gcTest() throws Exception {
    killMacGc();
    Connector c = getConnector();
    c.tableOperations().create("test_ingest");
    c.tableOperations().setProperty("test_ingest", Property.TABLE_SPLIT_THRESHOLD.getKey(), "5K");
    TestIngest.Opts opts = new TestIngest.Opts();
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    vopts.rows = opts.rows = 10000;
    vopts.cols = opts.cols = 1;
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    c.tableOperations().compact("test_ingest", null, null, true, true);
    int before = countFiles();
    while (true) {
      UtilWaitThread.sleep(1000);
      int more = countFiles();
      if (more <= before)
        break;
      before = more;
    }

    // restart GC
    getCluster().start();
    UtilWaitThread.sleep(15 * 1000);
    int after = countFiles();
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
    assertTrue(after < before);
  }

  @Test
  public void gcLotsOfCandidatesIT() throws Exception {
    killMacGc();

    log.info("Filling metadata table with bogus delete flags");
    Connector c = getConnector();
    addEntries(c, new BatchWriterOpts());
    cluster.getConfig().setDefaultMemory(10, MemoryUnit.MEGABYTE);
    Process gc = cluster.exec(SimpleGarbageCollector.class);
    UtilWaitThread.sleep(20 * 1000);
    String output = FunctionalTestUtils.readAll(cluster, SimpleGarbageCollector.class, gc);
    gc.destroy();
    assertTrue(output.contains("delete candidates has exceeded"));
  }

  @Test
  public void dontGCRootLog() throws Exception {
    killMacGc();
    // dirty metadata
    Connector c = getConnector();
    String table = getUniqueNames(1)[0];
    c.tableOperations().create(table);
    // let gc run for a bit
    cluster.start();
    UtilWaitThread.sleep(20 * 1000);
    killMacGc();
    // kill tservers
    for (ProcessReference ref : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
      cluster.killProcess(ServerType.TABLET_SERVER, ref);
    }
    // run recovery
    cluster.start();
    // did it recover?
    Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    FunctionalTestUtils.count(scanner);
  }

  private Mutation createDelMutation(String path, String cf, String cq, String val) {
    Text row = new Text(MetadataSchema.DeletesSection.getRowPrefix() + path);
    Mutation delFlag = new Mutation(row);
    delFlag.put(cf, cq, val);
    return delFlag;
  }

  @Test
  public void testInvalidDelete() throws Exception {
    killMacGc();

    String table = getUniqueNames(1)[0];
    getConnector().tableOperations().create(table);

    BatchWriter bw2 = getConnector().createBatchWriter(table, new BatchWriterConfig());
    Mutation m1 = new Mutation("r1");
    m1.put("cf1", "cq1", "v1");
    bw2.addMutation(m1);
    bw2.close();

    getConnector().tableOperations().flush(table, null, null, true);

    // ensure an invalid delete entry does not cause GC to go berserk ACCUMULO-2520
    getConnector().securityOperations().grantTablePermission(getConnector().whoami(), MetadataTable.NAME, TablePermission.WRITE);
    BatchWriter bw3 = getConnector().createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());

    bw3.addMutation(createDelMutation("", "", "", ""));
    bw3.addMutation(createDelMutation("", "testDel", "test", "valueTest"));
    bw3.addMutation(createDelMutation("/", "", "", ""));
    bw3.close();

    Process gc = cluster.exec(SimpleGarbageCollector.class);
    try {
      String output = "";
      while (!output.contains("Ingoring invalid deletion candidate")) {
        UtilWaitThread.sleep(250);
        try {
          output = FunctionalTestUtils.readAll(cluster, SimpleGarbageCollector.class, gc);
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }
    } finally {
      gc.destroy();
    }

    Scanner scanner = getConnector().createScanner(table, Authorizations.EMPTY);
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    assertTrue(iter.hasNext());
    Entry<Key,Value> entry = iter.next();
    Assert.assertEquals("r1", entry.getKey().getRow().toString());
    Assert.assertEquals("cf1", entry.getKey().getColumnFamily().toString());
    Assert.assertEquals("cq1", entry.getKey().getColumnQualifier().toString());
    Assert.assertEquals("v1", entry.getValue().toString());
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testProperPortAdvertisement() throws Exception {

    Connector conn = getConnector();
    Instance instance = conn.getInstance();

    ZooReaderWriter zk = new ZooReaderWriter(cluster.getZooKeepers(), 30000, OUR_SECRET);
    String path = ZooUtil.getRoot(instance) + Constants.ZGC_LOCK;
    for (int i = 0; i < 5; i++) {
      List<String> locks;
      try {
        locks = zk.getChildren(path, null);
      } catch (NoNodeException e) {
        Thread.sleep(5000);
        continue;
      }

      if (locks != null && locks.size() > 0) {
        Collections.sort(locks);

        String lockPath = path + "/" + locks.get(0);

        String gcLoc = new String(zk.getData(lockPath, null));

        Assert.assertTrue("Found unexpected data in zookeeper for GC location: " + gcLoc, gcLoc.startsWith(Service.GC_CLIENT.name()));
        int loc = gcLoc.indexOf(ServerServices.SEPARATOR_CHAR);
        Assert.assertNotEquals("Could not find split point of GC location for: " + gcLoc, -1, loc);
        String addr = gcLoc.substring(loc + 1);

        int addrSplit = addr.indexOf(':');
        Assert.assertNotEquals("Could not find split of GC host:port for: " + addr, -1, addrSplit);

        String host = addr.substring(0, addrSplit), port = addr.substring(addrSplit + 1);
        // We shouldn't have the "bindall" address in zk
        Assert.assertNotEquals("0.0.0.0", host);
        // Nor should we have the "random port" in zk
        Assert.assertNotEquals(0, Integer.parseInt(port));
        return;
      }

      Thread.sleep(5000);
    }

    Assert.fail("Could not find advertised GC address");
  }

  private int countFiles() throws Exception {
    FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
    int result = 0;
    Path path = new Path(cluster.getConfig().getDir() + "/accumulo/tables/1/*/*.rf");
    for (@SuppressWarnings("unused")
    FileStatus entry : fs.globStatus(path)) {
      result++;
    }
    return result;
  }

  public static void addEntries(Connector conn, BatchWriterOpts bwOpts) throws Exception {
    conn.securityOperations().grantTablePermission(conn.whoami(), MetadataTable.NAME, TablePermission.WRITE);
    BatchWriter bw = conn.createBatchWriter(MetadataTable.NAME, bwOpts.getBatchWriterConfig());

    for (int i = 0; i < 100000; ++i) {
      final Text emptyText = new Text("");
      Text row = new Text(String.format("%s/%020d/%s", MetadataSchema.DeletesSection.getRowPrefix(), i,
          "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjj"));
      Mutation delFlag = new Mutation(row);
      delFlag.put(emptyText, emptyText, new Value(new byte[] {}));
      bw.addMutation(delFlag);
    }
    bw.close();
  }
}
