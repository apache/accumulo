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
package org.apache.accumulo.test.replication;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.replication.ReplicaSystemFactory;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.replication.AccumuloReplicaSystem;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * 
 */
public class CyclicReplicationIT {
  private static final Logger log = LoggerFactory.getLogger(CyclicReplicationIT.class);

  @Rule
  public TestName testName = new TestName();

  private File createTestDir(String name) {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    baseDir.mkdirs();
    File testDir = new File(baseDir, this.getClass().getName() + "_" + testName.getMethodName() + "_" + name);
    FileUtils.deleteQuietly(testDir);
    testDir.mkdir();
    return testDir;
  }

  private void setCoreSite(MiniAccumuloClusterImpl cluster) throws Exception {
    File csFile = new File(cluster.getConfig().getConfDir(), "core-site.xml");
    if (csFile.exists())
      throw new RuntimeException(csFile + " already exist");

    Configuration coreSite = new Configuration(false);
    coreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(cluster.getConfig().getConfDir(), "core-site.xml")));
    coreSite.writeXml(out);
    out.close();
  }

  @Test(timeout = 5 * 60 * 1000)
  public void dataIsNotOverReplicated() throws Exception {
    File master1Dir = createTestDir("master1"), master2Dir = createTestDir("master2");
    String password = "password";

    MiniAccumuloConfigImpl master1Cfg = new MiniAccumuloConfigImpl(master1Dir, password);
    master1Cfg.setNumTservers(1);
    master1Cfg.setInstanceName("master1");
    master1Cfg.setProperty(Property.REPLICATION_NAME, master1Cfg.getInstanceName());
    master1Cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "5M");
    master1Cfg.setProperty(Property.REPLICATION_THREADCHECK, "5m");
    master1Cfg.setProperty(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP, "1s");
    master1Cfg.setProperty(Property.MASTER_REPLICATION_SCAN_INTERVAL, "1s");
    MiniAccumuloClusterImpl master1Cluster = master1Cfg.build();
    setCoreSite(master1Cluster);
    
    MiniAccumuloConfigImpl master2Cfg = new MiniAccumuloConfigImpl(master2Dir, password);
    master2Cfg.setNumTservers(1);
    master2Cfg.setInstanceName("master2");
    master2Cfg.setProperty(Property.REPLICATION_NAME, master2Cfg.getInstanceName());
    master2Cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "5M");
    master2Cfg.setProperty(Property.MASTER_REPLICATION_COORDINATOR_PORT, "10003");
    master2Cfg.setProperty(Property.REPLICATION_RECEIPT_SERVICE_PORT, "10004");
    master2Cfg.setProperty(Property.REPLICATION_THREADCHECK, "5m");
    master2Cfg.setProperty(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP, "1s");
    master2Cfg.setProperty(Property.MASTER_REPLICATION_SCAN_INTERVAL, "1s");
    MiniAccumuloClusterImpl master2Cluster = master2Cfg.build();
    setCoreSite(master2Cluster);

    master1Cluster.start();
    master2Cluster.start();

    try {
      Connector connMaster1 = master1Cluster.getConnector("root", password), connMaster2 = master2Cluster.getConnector("root", password);

      connMaster1.instanceOperations().setProperty(
          Property.REPLICATION_PEERS.getKey() + master2Cluster.getInstanceName(),
          ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
              AccumuloReplicaSystem.buildConfiguration(master2Cluster.getInstanceName(), master2Cluster.getZooKeepers())));

      connMaster2.instanceOperations().setProperty(
          Property.REPLICATION_PEERS.getKey() + master1Cluster.getInstanceName(),
          ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
              AccumuloReplicaSystem.buildConfiguration(master1Cluster.getInstanceName(), master1Cluster.getZooKeepers())));
      
      connMaster1.tableOperations().create(master1Cluster.getInstanceName(), false);
      String master1TableId = connMaster1.tableOperations().tableIdMap().get(master1Cluster.getInstanceName());
      Assert.assertNotNull(master1TableId);

      connMaster2.tableOperations().create(master2Cluster.getInstanceName(), false);
      String master2TableId = connMaster2.tableOperations().tableIdMap().get(master2Cluster.getInstanceName());
      Assert.assertNotNull(master2TableId);

      // Replicate master1 in the master1 cluster to master2 in the master2 cluster
      connMaster1.tableOperations().setProperty(master1Cluster.getInstanceName(), Property.TABLE_REPLICATION.getKey(), "true");
      connMaster1.tableOperations().setProperty(master1Cluster.getInstanceName(), Property.TABLE_REPLICATION_TARGETS.getKey() + master2Cluster.getInstanceName(), master2TableId);

      // Replicate master2 in the master2 cluster to master1 in the master2 cluster
      connMaster2.tableOperations().setProperty(master2Cluster.getInstanceName(), Property.TABLE_REPLICATION.getKey(), "true");
      connMaster2.tableOperations().setProperty(master2Cluster.getInstanceName(), Property.TABLE_REPLICATION_TARGETS.getKey() + master1Cluster.getInstanceName(), master1TableId);

      IteratorSetting summingCombiner = new IteratorSetting(50, SummingCombiner.class);
      SummingCombiner.setEncodingType(summingCombiner, Type.STRING);
      SummingCombiner.setCombineAllColumns(summingCombiner, true);

      // Set a combiner on both instances that will sum multiple values
      // We can use this to verify that the mutation was not sent multiple times
      connMaster1.tableOperations().attachIterator(master1Cluster.getInstanceName(), summingCombiner);
      connMaster2.tableOperations().attachIterator(master2Cluster.getInstanceName(), summingCombiner);

      // Write a single entry
      BatchWriter bw = connMaster1.createBatchWriter(master1Cluster.getInstanceName(), new BatchWriterConfig());
      Mutation m = new Mutation("row");
      m.put("count", "", "1");
      bw.addMutation(m);
      bw.close();

      Set<String> files = connMaster1.replicationOperations().referencedFiles(master1Cluster.getInstanceName());

      log.info("Found {} that need replication from master1", files);

      // Kill and restart the tserver to close the WAL on master1
      for (ProcessReference proc : master1Cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
        master1Cluster.killProcess(ServerType.TABLET_SERVER, proc);
      }

      master1Cluster.exec(TabletServer.class);

      log.info("Restarted tserver on master1");

      // Sanity check that the element is there on master1
      Scanner s = connMaster1.createScanner(master1Cluster.getInstanceName(), Authorizations.EMPTY);
      Entry<Key,Value> entry = Iterables.getOnlyElement(s);
      Assert.assertEquals("1", entry.getValue().toString());

      // Wait for this table to replicate
      connMaster1.replicationOperations().drain(master1Cluster.getInstanceName(), files);

      Thread.sleep(5000);

      // Check that the element made it to master2 only once
      s = connMaster2.createScanner(master2Cluster.getInstanceName(), Authorizations.EMPTY);
      entry = Iterables.getOnlyElement(s);
      Assert.assertEquals("1", entry.getValue().toString());

      // Wait for master2 to finish replicating it back
      files = connMaster2.replicationOperations().referencedFiles(master2Cluster.getInstanceName());

      // Kill and restart the tserver to close the WAL on master2
      for (ProcessReference proc : master2Cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
        master2Cluster.killProcess(ServerType.TABLET_SERVER, proc);
      }

      master2Cluster.exec(TabletServer.class);

      // Check that the element made it to master2 only once
      s = connMaster2.createScanner(master2Cluster.getInstanceName(), Authorizations.EMPTY);
      entry = Iterables.getOnlyElement(s);
      Assert.assertEquals("1", entry.getValue().toString());

      connMaster2.replicationOperations().drain(master2Cluster.getInstanceName(), files);

      Thread.sleep(5000);

      // Verify that the entry wasn't sent back to master1
      s = connMaster1.createScanner(master1Cluster.getInstanceName(), Authorizations.EMPTY);
      entry = Iterables.getOnlyElement(s);
      Assert.assertEquals("1", entry.getValue().toString());
    } finally {
      master1Cluster.stop();
      master2Cluster.stop();
    }
  }

}
