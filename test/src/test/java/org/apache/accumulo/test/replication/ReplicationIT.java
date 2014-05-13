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

import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.replication.ReplicaSystemFactory;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.accumulo.tserver.replication.AccumuloReplicaSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * 
 */
public class ReplicationIT extends ConfigurableMacIT {
  private static final Logger log = LoggerFactory.getLogger(ReplicationIT.class);

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "2M");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "5s");
    cfg.setProperty(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP, "5s");
    cfg.setProperty(Property.REPLICATION_MAX_UNIT_SIZE, "8M");
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test(timeout = 60 * 5000)
  public void dataWasReplicatedToThePeer() throws Exception {
    MiniAccumuloConfigImpl peerCfg = new MiniAccumuloConfigImpl(createTestDir(this.getClass().getName() + "_" + this.testName.getMethodName() + "_peer"),
        ROOT_PASSWORD);
    peerCfg.setNumTservers(1);
    peerCfg.setInstanceName("peer");
    peerCfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "5M");
    peerCfg.setProperty(Property.MASTER_REPLICATION_COORDINATOR_PORT, "10003");
    peerCfg.setProperty(Property.REPLICATION_RECEIPT_SERVICE_PORT, "10004");
    peerCfg.setProperty(Property.REPLICATION_THREADCHECK, "5m");
    MiniAccumuloClusterImpl peerCluster = peerCfg.build();

    peerCluster.start();

    Connector connMaster = getConnector();
    Connector connPeer = peerCluster.getConnector("root", ROOT_PASSWORD);

    String peerClusterName = "peer";

    // ...peer = AccumuloReplicaSystem,instanceName,zookeepers
    connMaster.instanceOperations().setProperty(
        Property.REPLICATION_PEERS.getKey() + peerClusterName,
        ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
            AccumuloReplicaSystem.buildConfiguration(peerCluster.getInstanceName(), peerCluster.getZooKeepers())));

    String masterTable = "master", peerTable = "peer";

    connMaster.tableOperations().create(masterTable);
    String masterTableId = connMaster.tableOperations().tableIdMap().get(masterTable);
    Assert.assertNotNull(masterTableId);

    connPeer.tableOperations().create(peerTable);
    String peerTableId = connPeer.tableOperations().tableIdMap().get(peerTable);
    Assert.assertNotNull(peerTableId);

    // Replicate this table to the peerClusterName in a table with the peerTableId table id
    connMaster.tableOperations().setProperty(masterTable, Property.TABLE_REPLICATION.getKey(), "true");
    connMaster.tableOperations().setProperty(masterTable, Property.TABLE_REPLICATION_TARGETS.getKey() + peerClusterName, peerTableId);

    // Write some data to table1
    BatchWriter bw = connMaster.createBatchWriter(masterTable, new BatchWriterConfig());
    for (int rows = 0; rows < 5000; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 100; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    log.info("Wrote all data to master cluster");

    bw.close();

    while (!connMaster.tableOperations().exists(ReplicationTable.NAME)) {
      Thread.sleep(500);
    }

    connMaster.tableOperations().compact(masterTable, null, null, true, false);

    // Wait until we fully replicated something
    boolean fullyReplicated = false;
    for (int i = 0; i < 10 && !fullyReplicated; i++) {
      UtilWaitThread.sleep(2000);

      Scanner s = ReplicationTable.getScanner(connMaster);
      WorkSection.limit(s);
      for (Entry<Key,Value> entry : s) {
        Status status = Status.parseFrom(entry.getValue().get());
        if (StatusUtil.isFullyReplicated(status)) {
          fullyReplicated |= true;
        }
      }
    }

    Assert.assertNotEquals(0, fullyReplicated);

    // Once we fully replicated some file, we are guaranteed to have data on the remote
    Assert.assertTrue(0 < Iterables.size(connPeer.createScanner(peerTable, Authorizations.EMPTY)));

    peerCluster.stop();
  }


  @Test(timeout = 60 * 5000)
  public void dataReplicatedToCorrectTable() throws Exception {
    MiniAccumuloConfigImpl peer1Cfg = new MiniAccumuloConfigImpl(createTestDir(this.getClass().getName() + "_" + this.testName.getMethodName() + "_peer"),
        ROOT_PASSWORD);
    peer1Cfg.setNumTservers(1);
    peer1Cfg.setInstanceName("peer");
    peer1Cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "5M");
    peer1Cfg.setProperty(Property.MASTER_REPLICATION_COORDINATOR_PORT, "10003");
    peer1Cfg.setProperty(Property.REPLICATION_RECEIPT_SERVICE_PORT, "10004");
    peer1Cfg.setProperty(Property.REPLICATION_THREADCHECK, "5m");
    MiniAccumuloClusterImpl peer1Cluster = peer1Cfg.build();

    peer1Cluster.start();

    try {
      Connector connMaster = getConnector();
      Connector connPeer = peer1Cluster.getConnector("root", ROOT_PASSWORD);
  
      String peerClusterName = "peer";
  
      // ...peer = AccumuloReplicaSystem,instanceName,zookeepers
      connMaster.instanceOperations().setProperty(
          Property.REPLICATION_PEERS.getKey() + peerClusterName,
          ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
              AccumuloReplicaSystem.buildConfiguration(peer1Cluster.getInstanceName(), peer1Cluster.getZooKeepers())));
  
      String masterTable1 = "master1", peerTable1 = "peer1", masterTable2 = "master2", peerTable2 = "peer2";
  
      connMaster.tableOperations().create(masterTable1);
      String masterTableId1 = connMaster.tableOperations().tableIdMap().get(masterTable1);
      Assert.assertNotNull(masterTableId1);
  
      connMaster.tableOperations().create(masterTable2);
      String masterTableId2 = connMaster.tableOperations().tableIdMap().get(masterTable2);
      Assert.assertNotNull(masterTableId2);
  
      connPeer.tableOperations().create(peerTable1);
      String peerTableId1 = connPeer.tableOperations().tableIdMap().get(peerTable1);
      Assert.assertNotNull(peerTableId1);
  
      connPeer.tableOperations().create(peerTable2);
      String peerTableId2 = connPeer.tableOperations().tableIdMap().get(peerTable2);
      Assert.assertNotNull(peerTableId2);
  
      // Replicate this table to the peerClusterName in a table with the peerTableId table id
      connMaster.tableOperations().setProperty(masterTable1, Property.TABLE_REPLICATION.getKey(), "true");
      connMaster.tableOperations().setProperty(masterTable1, Property.TABLE_REPLICATION_TARGETS.getKey() + peerClusterName, peerTableId1);
  
      connMaster.tableOperations().setProperty(masterTable2, Property.TABLE_REPLICATION.getKey(), "true");
      connMaster.tableOperations().setProperty(masterTable2, Property.TABLE_REPLICATION_TARGETS.getKey() + peerClusterName, peerTableId2);
  
      // Write some data to table1
      BatchWriter bw = connMaster.createBatchWriter(masterTable1, new BatchWriterConfig());
      for (int rows = 0; rows < 2500; rows++) {
        Mutation m = new Mutation(masterTable1 + rows);
        for (int cols = 0; cols < 100; cols++) {
          String value = Integer.toString(cols);
          m.put(value, "", value);
        }
        bw.addMutation(m);
      }
  
      bw.close();
  
      // Write some data to table2
      bw = connMaster.createBatchWriter(masterTable2, new BatchWriterConfig());
      for (int rows = 0; rows < 2500; rows++) {
        Mutation m = new Mutation(masterTable2 + rows);
        for (int cols = 0; cols < 100; cols++) {
          String value = Integer.toString(cols);
          m.put(value, "", value);
        }
        bw.addMutation(m);
      }
  
      bw.close();
  
      log.info("Wrote all data to master cluster");
  
      while (!connMaster.tableOperations().exists(ReplicationTable.NAME)) {
        Thread.sleep(500);
      }
  
      connMaster.tableOperations().compact(masterTable1, null, null, true, false);
      connMaster.tableOperations().compact(masterTable2, null, null, true, false);
  
      // Wait until we fully replicated something
      boolean fullyReplicated = false;
      for (int i = 0; i < 10 && !fullyReplicated; i++) {
        UtilWaitThread.sleep(2000);
  
        Scanner s = ReplicationTable.getScanner(connMaster);
        WorkSection.limit(s);
        for (Entry<Key,Value> entry : s) {
          Status status = Status.parseFrom(entry.getValue().get());
          if (StatusUtil.isFullyReplicated(status)) {
            fullyReplicated |= true;
          }
        }
      }
  
      Assert.assertNotEquals(0, fullyReplicated);
  
      long countTable = 0l;
      for (Entry<Key,Value> entry : connPeer.createScanner(peerTable1, Authorizations.EMPTY)) {
        countTable++;
        Assert.assertTrue("Found unexpected key-value" + entry.getKey().toStringNoTruncate() + " " + entry.getValue(), entry.getKey().getRow().toString().startsWith(masterTable1));
      }

      log.info("Found {} records in {}", countTable, peerTable1);
      Assert.assertTrue(countTable > 0);
  
      countTable = 0l;
      for (Entry<Key,Value> entry : connPeer.createScanner(peerTable2, Authorizations.EMPTY)) {
        countTable++;
        Assert.assertTrue("Found unexpected key-value" + entry.getKey().toStringNoTruncate() + " " + entry.getValue(), entry.getKey().getRow().toString().startsWith(masterTable2));
      }

      log.info("Found {} records in {}", countTable, peerTable2);
      Assert.assertTrue(countTable > 0);
  
    } finally {
      peer1Cluster.stop();
    }
  }
}
