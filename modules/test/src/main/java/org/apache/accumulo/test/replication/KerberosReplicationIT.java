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

import java.security.PrivilegedExceptionAction;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloITBase;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.MiniClusterHarness;
import org.apache.accumulo.harness.TestingKdc;
import org.apache.accumulo.master.replication.SequentialWorkAssigner;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.server.replication.ReplicaSystemFactory;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.accumulo.test.functional.KerberosIT;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.replication.AccumuloReplicaSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

/**
 * Ensure that replication occurs using keytabs instead of password (not to mention SASL)
 */
@Category(MiniClusterOnlyTests.class)
public class KerberosReplicationIT extends AccumuloITBase {
  private static final Logger log = LoggerFactory.getLogger(KerberosIT.class);

  private static TestingKdc kdc;
  private static String krbEnabledForITs = null;
  private static ClusterUser rootUser;

  @BeforeClass
  public static void startKdc() throws Exception {
    kdc = new TestingKdc();
    kdc.start();
    krbEnabledForITs = System.getProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION);
    if (null == krbEnabledForITs || !Boolean.parseBoolean(krbEnabledForITs)) {
      System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, "true");
    }
    rootUser = kdc.getRootUser();
  }

  @AfterClass
  public static void stopKdc() throws Exception {
    if (null != kdc) {
      kdc.stop();
    }
    if (null != krbEnabledForITs) {
      System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, krbEnabledForITs);
    }
  }

  private MiniAccumuloClusterImpl primary, peer;
  private String PRIMARY_NAME = "primary", PEER_NAME = "peer";

  @Override
  protected int defaultTimeoutSeconds() {
    return 60 * 3;
  }

  private MiniClusterConfigurationCallback getConfigCallback(final String name) {
    return new MiniClusterConfigurationCallback() {
      @Override
      public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
        cfg.setNumTservers(1);
        cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
        cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "2M");
        cfg.setProperty(Property.GC_CYCLE_START, "1s");
        cfg.setProperty(Property.GC_CYCLE_DELAY, "5s");
        cfg.setProperty(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP, "1s");
        cfg.setProperty(Property.MASTER_REPLICATION_SCAN_INTERVAL, "1s");
        cfg.setProperty(Property.REPLICATION_NAME, name);
        cfg.setProperty(Property.REPLICATION_MAX_UNIT_SIZE, "8M");
        cfg.setProperty(Property.REPLICATION_WORK_ASSIGNER, SequentialWorkAssigner.class.getName());
        cfg.setProperty(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX, "1M");
        coreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
        coreSite.set("fs.defaultFS", "file:///");
      }
    };
  }

  @Before
  public void setup() throws Exception {
    MiniClusterHarness harness = new MiniClusterHarness();

    // Create a primary and a peer instance, both with the same "root" user
    primary = harness.create(getClass().getName(), testName.getMethodName(), new PasswordToken("unused"), getConfigCallback(PRIMARY_NAME), kdc);
    primary.start();

    peer = harness.create(getClass().getName(), testName.getMethodName() + "_peer", new PasswordToken("unused"), getConfigCallback(PEER_NAME), kdc);
    peer.start();

    // Enable kerberos auth
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  @After
  public void teardown() throws Exception {
    if (null != peer) {
      peer.stop();
    }
    if (null != primary) {
      primary.stop();
    }
    UserGroupInformation.setConfiguration(new Configuration(false));
  }

  @Test
  public void dataReplicatedToCorrectTable() throws Exception {
    // Login as the root user
    final UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(rootUser.getPrincipal(), rootUser.getKeytab().toURI().toString());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        log.info("testing {}", ugi);
        final KerberosToken token = new KerberosToken();
        final Connector primaryConn = primary.getConnector(rootUser.getPrincipal(), token);
        final Connector peerConn = peer.getConnector(rootUser.getPrincipal(), token);

        ClusterUser replicationUser = kdc.getClientPrincipal(0);

        // Create user for replication to the peer
        peerConn.securityOperations().createLocalUser(replicationUser.getPrincipal(), null);

        primaryConn.instanceOperations().setProperty(Property.REPLICATION_PEER_USER.getKey() + PEER_NAME, replicationUser.getPrincipal());
        primaryConn.instanceOperations().setProperty(Property.REPLICATION_PEER_KEYTAB.getKey() + PEER_NAME, replicationUser.getKeytab().getAbsolutePath());

        // ...peer = AccumuloReplicaSystem,instanceName,zookeepers
        primaryConn.instanceOperations().setProperty(
            Property.REPLICATION_PEERS.getKey() + PEER_NAME,
            ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
                AccumuloReplicaSystem.buildConfiguration(peerConn.getInstance().getInstanceName(), peerConn.getInstance().getZooKeepers())));

        String primaryTable1 = "primary", peerTable1 = "peer";

        // Create tables
        primaryConn.tableOperations().create(primaryTable1);
        String masterTableId1 = primaryConn.tableOperations().tableIdMap().get(primaryTable1);
        Assert.assertNotNull(masterTableId1);

        peerConn.tableOperations().create(peerTable1);
        String peerTableId1 = peerConn.tableOperations().tableIdMap().get(peerTable1);
        Assert.assertNotNull(peerTableId1);

        // Grant write permission
        peerConn.securityOperations().grantTablePermission(replicationUser.getPrincipal(), peerTable1, TablePermission.WRITE);

        // Replicate this table to the peerClusterName in a table with the peerTableId table id
        primaryConn.tableOperations().setProperty(primaryTable1, Property.TABLE_REPLICATION.getKey(), "true");
        primaryConn.tableOperations().setProperty(primaryTable1, Property.TABLE_REPLICATION_TARGET.getKey() + PEER_NAME, peerTableId1);

        // Write some data to table1
        BatchWriter bw = primaryConn.createBatchWriter(primaryTable1, new BatchWriterConfig());
        long masterTable1Records = 0l;
        for (int rows = 0; rows < 2500; rows++) {
          Mutation m = new Mutation(primaryTable1 + rows);
          for (int cols = 0; cols < 100; cols++) {
            String value = Integer.toString(cols);
            m.put(value, "", value);
            masterTable1Records++;
          }
          bw.addMutation(m);
        }

        bw.close();

        log.info("Wrote all data to primary cluster");

        Set<String> filesFor1 = primaryConn.replicationOperations().referencedFiles(primaryTable1);

        // Restart the tserver to force a close on the WAL
        for (ProcessReference proc : primary.getProcesses().get(ServerType.TABLET_SERVER)) {
          primary.killProcess(ServerType.TABLET_SERVER, proc);
        }
        primary.exec(TabletServer.class);

        log.info("Restarted the tserver");

        // Read the data -- the tserver is back up and running and tablets are assigned
        Iterators.size(primaryConn.createScanner(primaryTable1, Authorizations.EMPTY).iterator());

        // Wait for both tables to be replicated
        log.info("Waiting for {} for {}", filesFor1, primaryTable1);
        primaryConn.replicationOperations().drain(primaryTable1, filesFor1);

        long countTable = 0l;
        for (Entry<Key,Value> entry : peerConn.createScanner(peerTable1, Authorizations.EMPTY)) {
          countTable++;
          Assert.assertTrue("Found unexpected key-value" + entry.getKey().toStringNoTruncate() + " " + entry.getValue(), entry.getKey().getRow().toString()
              .startsWith(primaryTable1));
        }

        log.info("Found {} records in {}", countTable, peerTable1);
        Assert.assertEquals(masterTable1Records, countTable);

        return null;
      }
    });
  }
}
