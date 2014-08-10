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
import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableMap;

/**
 * 
 */
public class MiniAccumuloClusterGCTest {
  
  private static TemporaryFolder tmpDir = new TemporaryFolder();
  private static MiniAccumuloConfig macConfig;
  private static MiniAccumuloCluster accumulo;
  private static final String passwd = "password";
  
  public static void setupMiniCluster() throws Exception {
    tmpDir.create();
    
    macConfig = new MiniAccumuloConfig(tmpDir.getRoot(), passwd);
    macConfig.setNumTservers(1);
    
    String gcPort = Integer.toString(PortUtils.getRandomFreePort());
    
    // And tweak the settings to make it run often
    Map<String,String> config = ImmutableMap.of(Property.GC_CYCLE_DELAY.getKey(), "1s", Property.GC_CYCLE_START.getKey(), "0s", Property.GC_PORT.getKey(), gcPort);
    macConfig.setSiteConfig(config);
    
    accumulo = new MiniAccumuloCluster(macConfig);
    accumulo.start();
  }
  
  public static void tearDownMiniCluster() throws Exception {
    accumulo.stop();
    tmpDir.delete();
  }
  
  // This test seems to be a little too unstable for a unit test
  @Ignore
  public void test() throws Exception {
    ZooKeeperInstance inst = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
    Connector c = inst.getConnector("root", new PasswordToken(passwd));
    
    final String table = "foobar";
    c.tableOperations().create(table);
    
    BatchWriter bw = null;
    
    // Add some data
    try {
      bw = c.createBatchWriter(table, new BatchWriterConfig());
      Mutation m = new Mutation("a");
      for (int i = 0; i < 50; i++) {
        m.put("colf", Integer.toString(i), "");
      }
      
      bw.addMutation(m);
    } finally {
      if (null != bw) {
        bw.close();
      }
    }
    
    final boolean flush = true, wait = true;
    
    // Compact the tables to get some rfiles which we can gc
    c.tableOperations().compact(table, null, null, flush, wait);
    c.tableOperations().compact("!METADATA", null, null, flush, wait);
    
    File accumuloDir = new File(tmpDir.getRoot().getAbsolutePath(), "accumulo");
    File tables = new File(accumuloDir.getAbsolutePath(), "tables");
    
    int fileCountAfterCompaction = FileUtils.listFiles(tables, new SuffixFileFilter(".rf"), TrueFileFilter.TRUE).size();
    
    // Sleep for 4s to let the GC do its thing
    for (int i = 1; i < 5; i++) {
      Thread.sleep(1000);
      int fileCountAfterGCWait = FileUtils.listFiles(tables, new SuffixFileFilter(".rf"), TrueFileFilter.TRUE).size();

      if (fileCountAfterGCWait < fileCountAfterCompaction) {
        return;
      }
    }
    
    Assert.fail("Expected to find less files after compaction and pause for GC");
  }
  
}
