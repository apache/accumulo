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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.cluster.ClusterUser;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.conf.ClientProperty;
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
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

/**
 * Ensure that replication occurs using keytabs instead of password (not to mention SASL)
 */
@Ignore("Replication ITs are not stable and not currently maintained")
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
    if (krbEnabledForITs == null || !Boolean.parseBoolean(krbEnabledForITs)) {
      System.setProperty(MiniClusterHarness.USE_KERBEROS_FOR_IT_OPTION, "true");
    }
    rootUser = kdc.getRootUser();
  }

  @AfterClass
  public static void stopKdc() {
    if (kdc != null) {
      kdc.stop();
    }
    if (krbEnabledForITs != null) {
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
        cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, "15s");
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
    primary = harness.create(getClass().getName(), testName.getMethodName(),
        new PasswordToken("unused"), getConfigCallback(PRIMARY_NAME), kdc);
    primary.start();

    peer = harness.create(getClass().getName(), testName.getMethodName() + "_peer",
        new PasswordToken("unused"), getConfigCallback(PEER_NAME), kdc);
    peer.start();

    // Enable kerberos auth
    Configuration conf = new Configuration(false);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  @After
  public void teardown() throws Exception {
    if (peer != null) {
      peer.stop();
    }
    if (primary != null) {
      primary.stop();
    }
    UserGroupInformation.setConfiguration(new Configuration(false));
  }

  @Test
  public void dataReplicatedToCorrectTable() throws Exception {
    // Login as the root user
    final UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        rootUser.getPrincipal(), rootUser.getKeytab().toURI().toString());
    ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
      log.info("testing {}", ugi);
      final KerberosToken token = new KerberosToken();
      try (
          AccumuloClient primaryclient =
              primary.createAccumuloClient(rootUser.getPrincipal(), token);
          AccumuloClient peerclient = peer.createAccumuloClient(rootUser.getPrincipal(), token)) {

        ClusterUser replicationUser = kdc.getClientPrincipal(0);

        // Create user for replication to the peer
        peerclient.securityOperations().createLocalUser(replicationUser.getPrincipal(), null);

        primaryclient.instanceOperations().setProperty(
            Property.REPLICATION_PEER_USER.getKey() + PEER_NAME, replicationUser.getPrincipal());
        primaryclient.instanceOperations().setProperty(
            Property.REPLICATION_PEER_KEYTAB.getKey() + PEER_NAME,
            replicationUser.getKeytab().getAbsolutePath());

        // ...peer = AccumuloReplicaSystem,instanceName,zookeepers
        ClientInfo info = ClientInfo.from(peerclient.properties());
        primaryclient.instanceOperations().setProperty(
            Property.REPLICATION_PEERS.getKey() + PEER_NAME,
            ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
                AccumuloReplicaSystem.buildConfiguration(info.getInstanceName(),
                    info.getZooKeepers())));

        String primaryTable1 = "primary", peerTable1 = "peer";

        // Create tables
        peerclient.tableOperations().create(peerTable1);
        String peerTableId1 = peerclient.tableOperations().tableIdMap().get(peerTable1);
        assertNotNull(peerTableId1);

        Map<String,String> props = new HashMap<>();
        props.put(Property.TABLE_REPLICATION.getKey(), "true");
        // Replicate this table to the peerClusterName in a table with the peerTableId table id
        props.put(Property.TABLE_REPLICATION_TARGET.getKey() + PEER_NAME, peerTableId1);

        primaryclient.tableOperations().create(primaryTable1,
            new NewTableConfiguration().setProperties(props));
        String masterTableId1 = primaryclient.tableOperations().tableIdMap().get(primaryTable1);
        assertNotNull(masterTableId1);

        // Grant write permission
        peerclient.securityOperations().grantTablePermission(replicationUser.getPrincipal(),
            peerTable1, TablePermission.WRITE);

        // Write some data to table1
        long masterTable1Records = 0L;
        try (BatchWriter bw = primaryclient.createBatchWriter(primaryTable1)) {
          for (int rows = 0; rows < 2500; rows++) {
            Mutation m = new Mutation(primaryTable1 + rows);
            for (int cols = 0; cols < 100; cols++) {
              String value = Integer.toString(cols);
              m.put(value, "", value);
              masterTable1Records++;
            }
            bw.addMutation(m);
          }
        }

        log.info("Wrote all data to primary cluster");

        Set<String> filesFor1 =
            primaryclient.replicationOperations().referencedFiles(primaryTable1);

        // Restart the tserver to force a close on the WAL
        for (ProcessReference proc : primary.getProcesses().get(ServerType.TABLET_SERVER)) {
          primary.killProcess(ServerType.TABLET_SERVER, proc);
        }
        primary.exec(TabletServer.class);

        log.info("Restarted the tserver");

        // Read the data -- the tserver is back up and running and tablets are assigned
        Iterators.size(primaryclient.createScanner(primaryTable1, Authorizations.EMPTY).iterator());

        // Wait for both tables to be replicated
        log.info("Waiting for {} for {}", filesFor1, primaryTable1);
        primaryclient.replicationOperations().drain(primaryTable1, filesFor1);

        long countTable = 0L;
        for (Entry<Key,Value> entry : peerclient.createScanner(peerTable1, Authorizations.EMPTY)) {
          countTable++;
          assertTrue("Found unexpected key-value" + entry.getKey().toStringNoTruncate() + " "
              + entry.getValue(), entry.getKey().getRow().toString().startsWith(primaryTable1));
        }

        log.info("Found {} records in {}", countTable, peerTable1);
        assertEquals(masterTable1Records, countTable);

        return null;
      }
    });
  }
}
