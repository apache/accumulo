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

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

/**
 * 
 */
public class MiniAccumuloClusterGCTest {
  private static final Logger log = Logger.getLogger(MiniAccumuloClusterGCTest.class);
  
  @Test
  public void testGcConfig() throws Exception {
    File f = Files.createTempDir();
    f.deleteOnExit();
    try {
      MiniAccumuloConfig macConfig = new MiniAccumuloConfig(f, passwd);
      macConfig.setNumTservers(1);
  
      Assert.assertEquals(false, macConfig.shouldRunGC());
      
      // Turn on the garbage collector
      macConfig.runGC(true);
  
      Assert.assertEquals(true, macConfig.shouldRunGC());
    } finally {
      if (null != f && f.exists()) {
        f.delete();
      }
    }
  }

  
  private static File testDir = new File(System.getProperty("user.dir") + "/target/" + MiniAccumuloClusterGCTest.class.getName());
  private static MiniAccumuloConfig macConfig;
  private static MiniAccumuloCluster accumulo;
  private static final String passwd = "password";
  
  @BeforeClass
  public static void setupMiniCluster() throws Exception {
    FileUtils.deleteQuietly(testDir);
    testDir.mkdir();
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);

    macConfig = new MiniAccumuloConfig(testDir, passwd);
    macConfig.setNumTservers(1);

    // Turn on the garbage collector
    macConfig.runGC(true);

    // And tweak the settings to make it run often
    Map<String,String> config = ImmutableMap.of(Property.GC_CYCLE_DELAY.getKey(), "1s", Property.GC_CYCLE_START.getKey(), "0s");
    macConfig.setSiteConfig(config);

    accumulo = new MiniAccumuloCluster(macConfig);
    accumulo.start();
  }
  
  @AfterClass
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
  }
  
  // This test seems to be a little too unstable for a unit test
  @Ignore
  public void test() throws Exception {
    ZooKeeperInstance inst = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
    Connector c = inst.getConnector("root", new PasswordToken(passwd));

    final String table = "foobar";
    c.tableOperations().create(table);
    
    final String tableId = c.tableOperations().tableIdMap().get(table);

    BatchWriter bw = null;

    // Add some data
    try {
      bw = c.createBatchWriter(table, new BatchWriterConfig().setMaxMemory(100000l).setMaxLatency(100, TimeUnit.MILLISECONDS).setMaxWriteThreads(1));
      Mutation m = new Mutation("a");
      for (int i = 0; i < 500; i++) {
        m.put("colf", Integer.toString(i), "");
      }

      bw.addMutation(m);
    } finally {
      if (null != bw) {
        bw.close();
      }
    }

    File accumuloDir = new File(testDir, "accumulo");
    File tables = new File(accumuloDir.getAbsolutePath(), "tables");
    File myTable = new File(tables, tableId);
    
    log.trace("Files before compaction: " + FileUtils.listFiles(myTable, new SuffixFileFilter(".rf"), TrueFileFilter.TRUE));

    final boolean flush = true, wait = true;

    // Compact the tables to get some rfiles which we can gc
    c.tableOperations().compact(table, null, null, flush, wait);

    Collection<File> filesAfterCompaction = FileUtils.listFiles(myTable, new SuffixFileFilter(".rf"), TrueFileFilter.TRUE);
    int fileCountAfterCompaction = filesAfterCompaction.size();
    
    log.trace("Files after compaction: " + filesAfterCompaction);

    // Sleep for 10s to let the GC do its thing
    for (int i = 1; i < 10; i++) {
      Thread.sleep(1000);
      filesAfterCompaction = FileUtils.listFiles(myTable, new SuffixFileFilter(".rf"), TrueFileFilter.TRUE);
      
      log.trace("Files in loop: " + filesAfterCompaction);
      
      int fileCountAfterGCWait = filesAfterCompaction.size();

      if (fileCountAfterGCWait < fileCountAfterCompaction) {
        return;
      }
    }

    Assert.fail("Expected to find less files after compaction and pause for GC");
  }

  @Test(timeout = 10000)
  public void testAccurateProcessListReturned() throws Exception {
    Map<ServerType,Collection<ProcessReference>> procs = accumulo.getProcesses();

    for (ServerType t : new ServerType[] {ServerType.MASTER, ServerType.TABLET_SERVER, ServerType.ZOOKEEPER, ServerType.GARBAGE_COLLECTOR}) {
      Assert.assertTrue(procs.containsKey(t));
      Collection<ProcessReference> procRefs = procs.get(t);
      Assert.assertTrue(1 <= procRefs.size());

      for (ProcessReference procRef : procRefs) {
        Assert.assertNotNull(procRef);
      }
    }
  }

}
