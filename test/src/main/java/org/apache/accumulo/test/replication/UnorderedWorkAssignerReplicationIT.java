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
package org.apache.accumulo.test.replication;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.master.replication.UnorderedWorkAssigner;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.accumulo.server.replication.ReplicaSystemFactory;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.replication.AccumuloReplicaSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

@Ignore("Replication ITs are not stable and not currently maintained")
public class UnorderedWorkAssignerReplicationIT extends ConfigurableMacBase {
  private static final Logger log =
      LoggerFactory.getLogger(UnorderedWorkAssignerReplicationIT.class);

  private ExecutorService executor;
  private int timeoutFactor = 1;

  @Before
  public void createExecutor() {
    executor = Executors.newSingleThreadExecutor();

    try {
      timeoutFactor = Integer.parseInt(System.getProperty("timeout.factor"));
    } catch (NumberFormatException exception) {
      log.warn("Could not parse timeout.factor, not increasing timeout.");
    }

    assertTrue("The timeout factor must be a positive, non-zero value", timeoutFactor > 0);
  }

  @After
  public void stopExecutor() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 60 * 6;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, "15s");
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "2M");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "5s");
    cfg.setProperty(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP, "1s");
    cfg.setProperty(Property.MASTER_REPLICATION_SCAN_INTERVAL, "1s");
    cfg.setProperty(Property.REPLICATION_MAX_UNIT_SIZE, "8M");
    cfg.setProperty(Property.REPLICATION_NAME, "master");
    cfg.setProperty(Property.REPLICATION_WORK_ASSIGNER, UnorderedWorkAssigner.class.getName());
    cfg.setProperty(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX, "1M");
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  /**
   * Use the same SSL and credential provider configuration that is set up by AbstractMacIT for the
   * other MAC used for replication
   */
  private void updatePeerConfigFromPrimary(MiniAccumuloConfigImpl primaryCfg,
      MiniAccumuloConfigImpl peerCfg) {
    // Set the same SSL information from the primary when present
    Map<String,String> primarySiteConfig = primaryCfg.getSiteConfig();
    if ("true".equals(primarySiteConfig.get(Property.INSTANCE_RPC_SSL_ENABLED.getKey()))) {
      Map<String,String> peerSiteConfig = new HashMap<>();
      peerSiteConfig.put(Property.INSTANCE_RPC_SSL_ENABLED.getKey(), "true");
      String keystorePath = primarySiteConfig.get(Property.RPC_SSL_KEYSTORE_PATH.getKey());
      assertNotNull("Keystore Path was null", keystorePath);
      peerSiteConfig.put(Property.RPC_SSL_KEYSTORE_PATH.getKey(), keystorePath);
      String truststorePath = primarySiteConfig.get(Property.RPC_SSL_TRUSTSTORE_PATH.getKey());
      assertNotNull("Truststore Path was null", truststorePath);
      peerSiteConfig.put(Property.RPC_SSL_TRUSTSTORE_PATH.getKey(), truststorePath);

      // Passwords might be stored in CredentialProvider
      String keystorePassword = primarySiteConfig.get(Property.RPC_SSL_KEYSTORE_PASSWORD.getKey());
      if (keystorePassword != null) {
        peerSiteConfig.put(Property.RPC_SSL_KEYSTORE_PASSWORD.getKey(), keystorePassword);
      }
      String truststorePassword =
          primarySiteConfig.get(Property.RPC_SSL_TRUSTSTORE_PASSWORD.getKey());
      if (truststorePassword != null) {
        peerSiteConfig.put(Property.RPC_SSL_TRUSTSTORE_PASSWORD.getKey(), truststorePassword);
      }

      System.out.println("Setting site configuration for peer " + peerSiteConfig);
      peerCfg.setSiteConfig(peerSiteConfig);
    }

    // Use the CredentialProvider if the primary also uses one
    String credProvider =
        primarySiteConfig.get(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey());
    if (credProvider != null) {
      Map<String,String> peerSiteConfig = peerCfg.getSiteConfig();
      peerSiteConfig.put(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey(),
          credProvider);
      peerCfg.setSiteConfig(peerSiteConfig);
    }
  }

  @Test
  public void dataWasReplicatedToThePeer() throws Exception {
    MiniAccumuloConfigImpl peerCfg = new MiniAccumuloConfigImpl(
        createTestDir(this.getClass().getName() + "_" + this.testName.getMethodName() + "_peer"),
        ROOT_PASSWORD);
    peerCfg.setNumTservers(1);
    peerCfg.setInstanceName("peer");
    updatePeerConfigFromPrimary(getCluster().getConfig(), peerCfg);
    peerCfg.setProperty(Property.REPLICATION_NAME, "peer");
    MiniAccumuloClusterImpl peerCluster = new MiniAccumuloClusterImpl(peerCfg);

    peerCluster.start();

    try (AccumuloClient clientMaster = Accumulo.newClient().from(getClientProperties()).build();
        AccumuloClient clientPeer =
            peerCluster.createAccumuloClient("root", new PasswordToken(ROOT_PASSWORD))) {

      ReplicationTable.setOnline(clientMaster);

      String peerUserName = "peer", peerPassword = "foo";

      String peerClusterName = "peer";

      clientPeer.securityOperations().createLocalUser(peerUserName,
          new PasswordToken(peerPassword));

      clientMaster.instanceOperations()
          .setProperty(Property.REPLICATION_PEER_USER.getKey() + peerClusterName, peerUserName);
      clientMaster.instanceOperations()
          .setProperty(Property.REPLICATION_PEER_PASSWORD.getKey() + peerClusterName, peerPassword);

      // ...peer = AccumuloReplicaSystem,instanceName,zookeepers
      clientMaster.instanceOperations().setProperty(
          Property.REPLICATION_PEERS.getKey() + peerClusterName,
          ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
              AccumuloReplicaSystem.buildConfiguration(peerCluster.getInstanceName(),
                  peerCluster.getZooKeepers())));

      final String masterTable = "master", peerTable = "peer";

      clientPeer.tableOperations().create(peerTable);
      String peerTableId = clientPeer.tableOperations().tableIdMap().get(peerTable);
      assertNotNull(peerTableId);

      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_REPLICATION.getKey(), "true");
      props.put(Property.TABLE_REPLICATION_TARGET.getKey() + peerClusterName, peerTableId);

      clientMaster.tableOperations().create(masterTable,
          new NewTableConfiguration().setProperties(props));
      String masterTableId = clientMaster.tableOperations().tableIdMap().get(masterTable);
      assertNotNull(masterTableId);

      clientPeer.securityOperations().grantTablePermission(peerUserName, peerTable,
          TablePermission.WRITE);

      // Wait for zookeeper updates (configuration) to propagate
      sleepUninterruptibly(3, TimeUnit.SECONDS);

      // Write some data to table1
      try (BatchWriter bw = clientMaster.createBatchWriter(masterTable)) {
        for (int rows = 0; rows < 5000; rows++) {
          Mutation m = new Mutation(Integer.toString(rows));
          for (int cols = 0; cols < 100; cols++) {
            String value = Integer.toString(cols);
            m.put(value, "", value);
          }
          bw.addMutation(m);
        }
      }

      log.info("Wrote all data to master cluster");

      final Set<String> filesNeedingReplication =
          clientMaster.replicationOperations().referencedFiles(masterTable);

      for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
        cluster.killProcess(ServerType.TABLET_SERVER, proc);
      }
      cluster.exec(TabletServer.class);

      log.info("TabletServer restarted");
      Iterators.size(ReplicationTable.getScanner(clientMaster).iterator());
      log.info("TabletServer is online");

      log.info("");
      log.info("Fetching metadata records:");
      for (Entry<Key,Value> kv : clientMaster.createScanner(MetadataTable.NAME,
          Authorizations.EMPTY)) {
        if (ReplicationSection.COLF.equals(kv.getKey().getColumnFamily())) {
          log.info("{} {}", kv.getKey().toStringNoTruncate(),
              ProtobufUtil.toString(Status.parseFrom(kv.getValue().get())));
        } else {
          log.info("{} {}", kv.getKey().toStringNoTruncate(), kv.getValue());
        }
      }

      log.info("");
      log.info("Fetching replication records:");
      for (Entry<Key,Value> kv : ReplicationTable.getScanner(clientMaster)) {
        log.info("{} {}", kv.getKey().toStringNoTruncate(),
            ProtobufUtil.toString(Status.parseFrom(kv.getValue().get())));
      }

      Future<Boolean> future = executor.submit(() -> {
        clientMaster.replicationOperations().drain(masterTable, filesNeedingReplication);
        log.info("Drain completed");
        return true;
      });

      long timeoutSeconds = timeoutFactor * 30;
      try {
        future.get(timeoutSeconds, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        future.cancel(true);
        fail("Drain did not finish within " + timeoutSeconds + " seconds");
      }

      log.info("drain completed");

      log.info("");
      log.info("Fetching metadata records:");
      for (Entry<Key,Value> kv : clientMaster.createScanner(MetadataTable.NAME,
          Authorizations.EMPTY)) {
        if (ReplicationSection.COLF.equals(kv.getKey().getColumnFamily())) {
          log.info("{} {}", kv.getKey().toStringNoTruncate(),
              ProtobufUtil.toString(Status.parseFrom(kv.getValue().get())));
        } else {
          log.info("{} {}", kv.getKey().toStringNoTruncate(), kv.getValue());
        }
      }

      log.info("");
      log.info("Fetching replication records:");
      for (Entry<Key,Value> kv : ReplicationTable.getScanner(clientMaster)) {
        log.info("{} {}", kv.getKey().toStringNoTruncate(),
            ProtobufUtil.toString(Status.parseFrom(kv.getValue().get())));
      }

      try (Scanner master = clientMaster.createScanner(masterTable, Authorizations.EMPTY);
          Scanner peer = clientPeer.createScanner(peerTable, Authorizations.EMPTY)) {
        Iterator<Entry<Key,Value>> masterIter = master.iterator(), peerIter = peer.iterator();
        Entry<Key,Value> masterEntry = null, peerEntry = null;
        while (masterIter.hasNext() && peerIter.hasNext()) {
          masterEntry = masterIter.next();
          peerEntry = peerIter.next();
          assertEquals(masterEntry.getKey() + " was not equal to " + peerEntry.getKey(), 0,
              masterEntry.getKey().compareTo(peerEntry.getKey(),
                  PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
          assertEquals(masterEntry.getValue(), peerEntry.getValue());
        }

        log.info("Last master entry: {}", masterEntry);
        log.info("Last peer entry: {}", peerEntry);

        assertFalse("Had more data to read from the master", masterIter.hasNext());
        assertFalse("Had more data to read from the peer", peerIter.hasNext());
      }
    } finally {
      peerCluster.stop();
    }
  }

  @Test
  public void dataReplicatedToCorrectTable() throws Exception {
    MiniAccumuloConfigImpl peerCfg = new MiniAccumuloConfigImpl(
        createTestDir(this.getClass().getName() + "_" + this.testName.getMethodName() + "_peer"),
        ROOT_PASSWORD);
    peerCfg.setNumTservers(1);
    peerCfg.setInstanceName("peer");
    updatePeerConfigFromPrimary(getCluster().getConfig(), peerCfg);
    peerCfg.setProperty(Property.REPLICATION_NAME, "peer");
    MiniAccumuloClusterImpl peer1Cluster = new MiniAccumuloClusterImpl(peerCfg);

    peer1Cluster.start();

    try (AccumuloClient clientMaster = Accumulo.newClient().from(getClientProperties()).build();
        AccumuloClient clientPeer =
            peer1Cluster.createAccumuloClient("root", new PasswordToken(ROOT_PASSWORD))) {

      String peerClusterName = "peer";
      String peerUserName = "peer", peerPassword = "foo";

      // Create local user
      clientPeer.securityOperations().createLocalUser(peerUserName,
          new PasswordToken(peerPassword));

      clientMaster.instanceOperations()
          .setProperty(Property.REPLICATION_PEER_USER.getKey() + peerClusterName, peerUserName);
      clientMaster.instanceOperations()
          .setProperty(Property.REPLICATION_PEER_PASSWORD.getKey() + peerClusterName, peerPassword);

      // ...peer = AccumuloReplicaSystem,instanceName,zookeepers
      clientMaster.instanceOperations().setProperty(
          Property.REPLICATION_PEERS.getKey() + peerClusterName,
          ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
              AccumuloReplicaSystem.buildConfiguration(peer1Cluster.getInstanceName(),
                  peer1Cluster.getZooKeepers())));

      String masterTable1 = "master1", peerTable1 = "peer1", masterTable2 = "master2",
          peerTable2 = "peer2";

      // Create tables

      clientPeer.tableOperations().create(peerTable1);
      String peerTableId1 = clientPeer.tableOperations().tableIdMap().get(peerTable1);
      assertNotNull(peerTableId1);

      clientPeer.tableOperations().create(peerTable2);
      String peerTableId2 = clientPeer.tableOperations().tableIdMap().get(peerTable2);
      assertNotNull(peerTableId2);

      // Grant write permission
      clientPeer.securityOperations().grantTablePermission(peerUserName, peerTable1,
          TablePermission.WRITE);
      clientPeer.securityOperations().grantTablePermission(peerUserName, peerTable2,
          TablePermission.WRITE);

      Map<String,String> props1 = new HashMap<>();
      props1.put(Property.TABLE_REPLICATION.getKey(), "true");
      props1.put(Property.TABLE_REPLICATION_TARGET.getKey() + peerClusterName, peerTableId1);

      clientMaster.tableOperations().create(masterTable1,
          new NewTableConfiguration().setProperties(props1));
      String masterTableId1 = clientMaster.tableOperations().tableIdMap().get(masterTable1);
      assertNotNull(masterTableId1);

      Map<String,String> props2 = new HashMap<>();
      props2.put(Property.TABLE_REPLICATION.getKey(), "true");
      props2.put(Property.TABLE_REPLICATION_TARGET.getKey() + peerClusterName, peerTableId2);

      clientMaster.tableOperations().create(masterTable2,
          new NewTableConfiguration().setProperties(props2));
      String masterTableId2 = clientMaster.tableOperations().tableIdMap().get(masterTable2);
      assertNotNull(masterTableId2);

      // Wait for zookeeper updates (configuration) to propagate
      sleepUninterruptibly(3, TimeUnit.SECONDS);

      // Write some data to table1
      long masterTable1Records = 0L;
      try (BatchWriter bw = clientMaster.createBatchWriter(masterTable1)) {
        for (int rows = 0; rows < 2500; rows++) {
          Mutation m = new Mutation(masterTable1 + rows);
          for (int cols = 0; cols < 100; cols++) {
            String value = Integer.toString(cols);
            m.put(value, "", value);
            masterTable1Records++;
          }
          bw.addMutation(m);
        }
      }

      // Write some data to table2
      long masterTable2Records = 0L;
      try (BatchWriter bw = clientMaster.createBatchWriter(masterTable2)) {
        for (int rows = 0; rows < 2500; rows++) {
          Mutation m = new Mutation(masterTable2 + rows);
          for (int cols = 0; cols < 100; cols++) {
            String value = Integer.toString(cols);
            m.put(value, "", value);
            masterTable2Records++;
          }
          bw.addMutation(m);
        }
      }

      log.info("Wrote all data to master cluster");

      Set<String> filesFor1 = clientMaster.replicationOperations().referencedFiles(masterTable1),
          filesFor2 = clientMaster.replicationOperations().referencedFiles(masterTable2);

      while (!ReplicationTable.isOnline(clientMaster)) {
        Thread.sleep(500);
      }

      // Restart the tserver to force a close on the WAL
      for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
        cluster.killProcess(ServerType.TABLET_SERVER, proc);
      }
      cluster.exec(TabletServer.class);

      log.info("Restarted the tserver");

      // Read the data -- the tserver is back up and running
      Iterators.size(clientMaster.createScanner(masterTable1, Authorizations.EMPTY).iterator());

      // Wait for both tables to be replicated
      log.info("Waiting for {} for {}", filesFor1, masterTable1);
      clientMaster.replicationOperations().drain(masterTable1, filesFor1);

      log.info("Waiting for {} for {}", filesFor2, masterTable2);
      clientMaster.replicationOperations().drain(masterTable2, filesFor2);

      long countTable = 0L;
      for (int i = 0; i < 5; i++) {
        countTable = 0L;
        for (Entry<Key,Value> entry : clientPeer.createScanner(peerTable1, Authorizations.EMPTY)) {
          countTable++;
          assertTrue("Found unexpected key-value" + entry.getKey().toStringNoTruncate() + " "
              + entry.getValue(), entry.getKey().getRow().toString().startsWith(masterTable1));
        }

        log.info("Found {} records in {}", countTable, peerTable1);

        if (masterTable1Records != countTable) {
          log.warn("Did not find {} expected records in {}, only found {}", masterTable1Records,
              peerTable1, countTable);
        }
      }

      assertEquals(masterTable1Records, countTable);

      for (int i = 0; i < 5; i++) {
        countTable = 0L;
        for (Entry<Key,Value> entry : clientPeer.createScanner(peerTable2, Authorizations.EMPTY)) {
          countTable++;
          assertTrue("Found unexpected key-value" + entry.getKey().toStringNoTruncate() + " "
              + entry.getValue(), entry.getKey().getRow().toString().startsWith(masterTable2));
        }

        log.info("Found {} records in {}", countTable, peerTable2);

        if (masterTable2Records != countTable) {
          log.warn("Did not find {} expected records in {}, only found {}", masterTable2Records,
              peerTable2, countTable);
        }
      }

      assertEquals(masterTable2Records, countTable);

    } finally {
      peer1Cluster.stop();
    }
  }

  @Test
  public void dataWasReplicatedToThePeerWithoutDrain() throws Exception {
    MiniAccumuloConfigImpl peerCfg = new MiniAccumuloConfigImpl(
        createTestDir(this.getClass().getName() + "_" + this.testName.getMethodName() + "_peer"),
        ROOT_PASSWORD);
    peerCfg.setNumTservers(1);
    peerCfg.setInstanceName("peer");
    updatePeerConfigFromPrimary(getCluster().getConfig(), peerCfg);
    peerCfg.setProperty(Property.REPLICATION_NAME, "peer");
    MiniAccumuloClusterImpl peerCluster = new MiniAccumuloClusterImpl(peerCfg);

    peerCluster.start();

    try (AccumuloClient clientMaster = Accumulo.newClient().from(getClientProperties()).build();
        AccumuloClient clientPeer =
            peerCluster.createAccumuloClient("root", new PasswordToken(ROOT_PASSWORD))) {

      String peerUserName = "repl";
      String peerPassword = "passwd";

      // Create a user on the peer for replication to use
      clientPeer.securityOperations().createLocalUser(peerUserName,
          new PasswordToken(peerPassword));

      String peerClusterName = "peer";

      // ...peer = AccumuloReplicaSystem,instanceName,zookeepers
      clientMaster.instanceOperations().setProperty(
          Property.REPLICATION_PEERS.getKey() + peerClusterName,
          ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
              AccumuloReplicaSystem.buildConfiguration(peerCluster.getInstanceName(),
                  peerCluster.getZooKeepers())));

      // Configure the credentials we should use to authenticate ourselves to the peer for
      // replication
      clientMaster.instanceOperations()
          .setProperty(Property.REPLICATION_PEER_USER.getKey() + peerClusterName, peerUserName);
      clientMaster.instanceOperations()
          .setProperty(Property.REPLICATION_PEER_PASSWORD.getKey() + peerClusterName, peerPassword);

      String masterTable = "master", peerTable = "peer";

      clientPeer.tableOperations().create(peerTable);
      String peerTableId = clientPeer.tableOperations().tableIdMap().get(peerTable);
      assertNotNull(peerTableId);

      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_REPLICATION.getKey(), "true");
      props.put(Property.TABLE_REPLICATION_TARGET.getKey() + peerClusterName, peerTableId);

      clientMaster.tableOperations().create(masterTable,
          new NewTableConfiguration().setProperties(props));
      String masterTableId = clientMaster.tableOperations().tableIdMap().get(masterTable);
      assertNotNull(masterTableId);

      // Give our replication user the ability to write to the table
      clientPeer.securityOperations().grantTablePermission(peerUserName, peerTable,
          TablePermission.WRITE);

      // Write some data to table1
      try (BatchWriter bw = clientMaster.createBatchWriter(masterTable)) {
        for (int rows = 0; rows < 5000; rows++) {
          Mutation m = new Mutation(Integer.toString(rows));
          for (int cols = 0; cols < 100; cols++) {
            String value = Integer.toString(cols);
            m.put(value, "", value);
          }
          bw.addMutation(m);
        }
      }

      log.info("Wrote all data to master cluster");

      Set<String> files = clientMaster.replicationOperations().referencedFiles(masterTable);
      for (String s : files) {
        log.info("Found referenced file for {}: {}", masterTable, s);
      }

      for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
        cluster.killProcess(ServerType.TABLET_SERVER, proc);
      }

      cluster.exec(TabletServer.class);

      Iterators.size(clientMaster.createScanner(masterTable, Authorizations.EMPTY).iterator());

      for (Entry<Key,Value> kv : clientMaster.createScanner(ReplicationTable.NAME,
          Authorizations.EMPTY)) {
        log.debug("{} {}", kv.getKey().toStringNoTruncate(),
            ProtobufUtil.toString(Status.parseFrom(kv.getValue().get())));
      }

      clientMaster.replicationOperations().drain(masterTable, files);

      try (Scanner master = clientMaster.createScanner(masterTable, Authorizations.EMPTY);
          Scanner peer = clientPeer.createScanner(peerTable, Authorizations.EMPTY)) {
        Iterator<Entry<Key,Value>> masterIter = master.iterator(), peerIter = peer.iterator();
        assertTrue("No data in master table", masterIter.hasNext());
        assertTrue("No data in peer table", peerIter.hasNext());
        while (masterIter.hasNext() && peerIter.hasNext()) {
          Entry<Key,Value> masterEntry = masterIter.next(), peerEntry = peerIter.next();
          assertEquals(peerEntry.getKey() + " was not equal to " + peerEntry.getKey(), 0,
              masterEntry.getKey().compareTo(peerEntry.getKey(),
                  PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
          assertEquals(masterEntry.getValue(), peerEntry.getValue());
        }

        assertFalse("Had more data to read from the master", masterIter.hasNext());
        assertFalse("Had more data to read from the peer", peerIter.hasNext());
      }
      peerCluster.stop();
    }
  }

  @Test
  public void dataReplicatedToCorrectTableWithoutDrain() throws Exception {
    MiniAccumuloConfigImpl peerCfg = new MiniAccumuloConfigImpl(
        createTestDir(this.getClass().getName() + "_" + this.testName.getMethodName() + "_peer"),
        ROOT_PASSWORD);
    peerCfg.setNumTservers(1);
    peerCfg.setInstanceName("peer");
    updatePeerConfigFromPrimary(getCluster().getConfig(), peerCfg);
    peerCfg.setProperty(Property.REPLICATION_NAME, "peer");
    MiniAccumuloClusterImpl peer1Cluster = new MiniAccumuloClusterImpl(peerCfg);

    peer1Cluster.start();

    try (AccumuloClient clientMaster = Accumulo.newClient().from(getClientProperties()).build();
        AccumuloClient clientPeer =
            peer1Cluster.createAccumuloClient("root", new PasswordToken(ROOT_PASSWORD))) {

      String peerClusterName = "peer";

      String peerUserName = "repl";
      String peerPassword = "passwd";

      // Create a user on the peer for replication to use
      clientPeer.securityOperations().createLocalUser(peerUserName,
          new PasswordToken(peerPassword));

      // Configure the credentials we should use to authenticate ourselves to the peer for
      // replication
      clientMaster.instanceOperations()
          .setProperty(Property.REPLICATION_PEER_USER.getKey() + peerClusterName, peerUserName);
      clientMaster.instanceOperations()
          .setProperty(Property.REPLICATION_PEER_PASSWORD.getKey() + peerClusterName, peerPassword);

      // ...peer = AccumuloReplicaSystem,instanceName,zookeepers
      clientMaster.instanceOperations().setProperty(
          Property.REPLICATION_PEERS.getKey() + peerClusterName,
          ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
              AccumuloReplicaSystem.buildConfiguration(peer1Cluster.getInstanceName(),
                  peer1Cluster.getZooKeepers())));

      String masterTable1 = "master1", peerTable1 = "peer1", masterTable2 = "master2",
          peerTable2 = "peer2";
      clientPeer.tableOperations().create(peerTable1, new NewTableConfiguration());
      String peerTableId1 = clientPeer.tableOperations().tableIdMap().get(peerTable1);
      assertNotNull(peerTableId1);

      clientPeer.tableOperations().create(peerTable2, new NewTableConfiguration());
      String peerTableId2 = clientPeer.tableOperations().tableIdMap().get(peerTable2);
      assertNotNull(peerTableId2);

      Map<String,String> props1 = new HashMap<>();
      props1.put(Property.TABLE_REPLICATION.getKey(), "true");
      props1.put(Property.TABLE_REPLICATION_TARGET.getKey() + peerClusterName, peerTableId1);

      clientMaster.tableOperations().create(masterTable1,
          new NewTableConfiguration().setProperties(props1));
      String masterTableId1 = clientMaster.tableOperations().tableIdMap().get(masterTable1);
      assertNotNull(masterTableId1);

      Map<String,String> props2 = new HashMap<>();
      props2.put(Property.TABLE_REPLICATION.getKey(), "true");
      props2.put(Property.TABLE_REPLICATION_TARGET.getKey() + peerClusterName, peerTableId2);

      clientMaster.tableOperations().create(masterTable2,
          new NewTableConfiguration().setProperties(props2));
      String masterTableId2 = clientMaster.tableOperations().tableIdMap().get(masterTable2);
      assertNotNull(masterTableId2);

      // Give our replication user the ability to write to the tables
      clientPeer.securityOperations().grantTablePermission(peerUserName, peerTable1,
          TablePermission.WRITE);
      clientPeer.securityOperations().grantTablePermission(peerUserName, peerTable2,
          TablePermission.WRITE);

      // Wait for zookeeper updates (configuration) to propagate
      sleepUninterruptibly(3, TimeUnit.SECONDS);

      // Write some data to table1
      try (BatchWriter bw = clientMaster.createBatchWriter(masterTable1)) {
        for (int rows = 0; rows < 2500; rows++) {
          Mutation m = new Mutation(masterTable1 + rows);
          for (int cols = 0; cols < 100; cols++) {
            String value = Integer.toString(cols);
            m.put(value, "", value);
          }
          bw.addMutation(m);
        }
      }

      // Write some data to table2
      try (BatchWriter bw = clientMaster.createBatchWriter(masterTable2)) {
        for (int rows = 0; rows < 2500; rows++) {
          Mutation m = new Mutation(masterTable2 + rows);
          for (int cols = 0; cols < 100; cols++) {
            String value = Integer.toString(cols);
            m.put(value, "", value);
          }
          bw.addMutation(m);
        }
      }

      log.info("Wrote all data to master cluster");

      while (!ReplicationTable.isOnline(clientMaster)) {
        Thread.sleep(500);
      }

      for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
        cluster.killProcess(ServerType.TABLET_SERVER, proc);
      }

      cluster.exec(TabletServer.class);

      // Wait until we fully replicated something
      boolean fullyReplicated = false;
      for (int i = 0; i < 10 && !fullyReplicated; i++) {
        sleepUninterruptibly(timeoutFactor * 2, TimeUnit.SECONDS);

        try (Scanner s = ReplicationTable.getScanner(clientMaster)) {
          WorkSection.limit(s);
          for (Entry<Key,Value> entry : s) {
            Status status = Status.parseFrom(entry.getValue().get());
            if (StatusUtil.isFullyReplicated(status)) {
              fullyReplicated |= true;
            }
          }
        }
      }

      assertNotEquals(0, fullyReplicated);

      long countTable = 0L;

      // Check a few times
      for (int i = 0; i < 10; i++) {
        countTable = 0L;
        for (Entry<Key,Value> entry : clientPeer.createScanner(peerTable1, Authorizations.EMPTY)) {
          countTable++;
          assertTrue("Found unexpected key-value" + entry.getKey().toStringNoTruncate() + " "
              + entry.getValue(), entry.getKey().getRow().toString().startsWith(masterTable1));
        }
        log.info("Found {} records in {}", countTable, peerTable1);
        if (countTable > 0) {
          break;
        }
        Thread.sleep(2000);
      }

      assertTrue("Did not find any records in " + peerTable1 + " on peer", countTable > 0);

      for (int i = 0; i < 10; i++) {
        countTable = 0L;
        for (Entry<Key,Value> entry : clientPeer.createScanner(peerTable2, Authorizations.EMPTY)) {
          countTable++;
          assertTrue("Found unexpected key-value" + entry.getKey().toStringNoTruncate() + " "
              + entry.getValue(), entry.getKey().getRow().toString().startsWith(masterTable2));
        }

        log.info("Found {} records in {}", countTable, peerTable2);
        if (countTable > 0) {
          break;
        }
        Thread.sleep(2000);
      }
      assertTrue("Did not find any records in " + peerTable2 + " on peer", countTable > 0);

    } finally {
      peer1Cluster.stop();
    }
  }
}
