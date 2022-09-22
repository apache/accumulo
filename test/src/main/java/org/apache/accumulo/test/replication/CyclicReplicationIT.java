/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.replication;

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloITBase;
import org.apache.accumulo.harness.Timeout;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.accumulo.miniclusterImpl.ZooKeeperBindException;
import org.apache.accumulo.server.replication.ReplicaSystemFactory;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.replication.AccumuloReplicaSystem;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Disabled("Replication ITs are not stable and not currently maintained")
@Tag(MINI_CLUSTER_ONLY)
@Deprecated
public class CyclicReplicationIT extends AccumuloITBase {
  private static final Logger log = LoggerFactory.getLogger(CyclicReplicationIT.class);

  @RegisterExtension
  Timeout timeout = Timeout.from(() -> {
    long waitLonger = 1L;
    try {
      String timeoutString = System.getProperty("timeout.factor");
      if (timeoutString != null && !timeoutString.isEmpty()) {
        waitLonger = Long.parseLong(timeoutString);
      }
    } catch (NumberFormatException exception) {
      log.warn("Could not parse timeout.factor, not scaling timeout");
    }

    return Duration.ofMinutes(waitLonger * 10);
  });

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  private File createTheTestDir(String name) {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());
    File testDir = new File(baseDir, this.getClass().getName() + "_" + testName() + "_" + name);
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir());
    return testDir;
  }

  private void setCoreSite(MiniAccumuloClusterImpl cluster) throws Exception {
    File csFile = new File(cluster.getConfig().getConfDir(), "core-site.xml");
    if (csFile.exists())
      throw new RuntimeException(csFile + " already exist");

    Configuration coreSite = new Configuration(false);
    coreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    OutputStream out = new BufferedOutputStream(
        new FileOutputStream(new File(cluster.getConfig().getConfDir(), "core-site.xml")));
    coreSite.writeXml(out);
    out.close();
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
      assertNotNull(keystorePath, "Keystore Path was null");
      peerSiteConfig.put(Property.RPC_SSL_KEYSTORE_PATH.getKey(), keystorePath);
      String truststorePath = primarySiteConfig.get(Property.RPC_SSL_TRUSTSTORE_PATH.getKey());
      assertNotNull(truststorePath, "Truststore Path was null");
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
  public void dataIsNotOverReplicated() throws Exception {
    File manager1Dir = createTheTestDir("manager1"), manager2Dir = createTheTestDir("manager2");
    String password = "password";

    MiniAccumuloConfigImpl manager1Cfg;
    MiniAccumuloClusterImpl manager1Cluster;
    while (true) {
      manager1Cfg = new MiniAccumuloConfigImpl(manager1Dir, password);
      manager1Cfg.setNumTservers(1);
      manager1Cfg.setInstanceName("manager1");

      // Set up SSL if needed
      ConfigurableMacBase.configureForEnvironment(manager1Cfg,
          ConfigurableMacBase.getSslDir(manager1Dir));

      manager1Cfg.setProperty(Property.REPLICATION_NAME, manager1Cfg.getInstanceName());
      manager1Cfg.setProperty(Property.TSERV_WAL_MAX_SIZE, "5M");
      manager1Cfg.setProperty(Property.REPLICATION_THREADCHECK, "5m");
      manager1Cfg.setProperty(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP, "1s");
      manager1Cfg.setProperty(Property.MANAGER_REPLICATION_SCAN_INTERVAL, "1s");
      manager1Cluster = new MiniAccumuloClusterImpl(manager1Cfg);
      setCoreSite(manager1Cluster);

      try {
        manager1Cluster.start();
        break;
      } catch (ZooKeeperBindException e) {
        log.warn("Failed to start ZooKeeper on {}, will retry", manager1Cfg.getZooKeeperPort());
      }
    }

    MiniAccumuloConfigImpl manager2Cfg;
    MiniAccumuloClusterImpl manager2Cluster;
    while (true) {
      manager2Cfg = new MiniAccumuloConfigImpl(manager2Dir, password);
      manager2Cfg.setNumTservers(1);
      manager2Cfg.setInstanceName("manager2");

      // Set up SSL if needed. Need to share the same SSL truststore as manager1
      this.updatePeerConfigFromPrimary(manager1Cfg, manager2Cfg);

      manager2Cfg.setProperty(Property.REPLICATION_NAME, manager2Cfg.getInstanceName());
      manager2Cfg.setProperty(Property.TSERV_WAL_MAX_SIZE, "5M");
      manager2Cfg.setProperty(Property.REPLICATION_THREADCHECK, "5m");
      manager2Cfg.setProperty(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP, "1s");
      manager2Cfg.setProperty(Property.MANAGER_REPLICATION_SCAN_INTERVAL, "1s");
      manager2Cluster = new MiniAccumuloClusterImpl(manager2Cfg);
      setCoreSite(manager2Cluster);

      try {
        manager2Cluster.start();
        break;
      } catch (ZooKeeperBindException e) {
        log.warn("Failed to start ZooKeeper on {}, will retry", manager2Cfg.getZooKeeperPort());
      }
    }

    try {
      AccumuloClient clientManager1 =
          manager1Cluster.createAccumuloClient("root", new PasswordToken(password)),
          clientManager2 =
              manager2Cluster.createAccumuloClient("root", new PasswordToken(password));

      String manager1UserName = "manager1", manager1Password = "foo";
      String manager2UserName = "manager2", manager2Password = "bar";
      String manager1Table = manager1Cluster.getInstanceName(),
          manager2Table = manager2Cluster.getInstanceName();

      clientManager1.securityOperations().createLocalUser(manager1UserName,
          new PasswordToken(manager1Password));
      clientManager2.securityOperations().createLocalUser(manager2UserName,
          new PasswordToken(manager2Password));

      // Configure the credentials we should use to authenticate ourselves to the peer for
      // replication
      clientManager1.instanceOperations().setProperty(
          Property.REPLICATION_PEER_USER.getKey() + manager2Cluster.getInstanceName(),
          manager2UserName);
      clientManager1.instanceOperations().setProperty(
          Property.REPLICATION_PEER_PASSWORD.getKey() + manager2Cluster.getInstanceName(),
          manager2Password);

      clientManager2.instanceOperations().setProperty(
          Property.REPLICATION_PEER_USER.getKey() + manager1Cluster.getInstanceName(),
          manager1UserName);
      clientManager2.instanceOperations().setProperty(
          Property.REPLICATION_PEER_PASSWORD.getKey() + manager1Cluster.getInstanceName(),
          manager1Password);

      clientManager1.instanceOperations().setProperty(
          Property.REPLICATION_PEERS.getKey() + manager2Cluster.getInstanceName(),
          ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
              AccumuloReplicaSystem.buildConfiguration(manager2Cluster.getInstanceName(),
                  manager2Cluster.getZooKeepers())));

      clientManager2.instanceOperations().setProperty(
          Property.REPLICATION_PEERS.getKey() + manager1Cluster.getInstanceName(),
          ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
              AccumuloReplicaSystem.buildConfiguration(manager1Cluster.getInstanceName(),
                  manager1Cluster.getZooKeepers())));

      clientManager1.tableOperations().create(manager1Table,
          new NewTableConfiguration().withoutDefaultIterators());
      String manager1TableId = clientManager1.tableOperations().tableIdMap().get(manager1Table);
      assertNotNull(manager1TableId);

      clientManager2.tableOperations().create(manager2Table,
          new NewTableConfiguration().withoutDefaultIterators());
      String manager2TableId = clientManager2.tableOperations().tableIdMap().get(manager2Table);
      assertNotNull(manager2TableId);

      // Replicate manager1 in the manager1 cluster to manager2 in the manager2 cluster
      clientManager1.tableOperations().setProperty(manager1Table,
          Property.TABLE_REPLICATION.getKey(), "true");
      clientManager1.tableOperations().setProperty(manager1Table,
          Property.TABLE_REPLICATION_TARGET.getKey() + manager2Cluster.getInstanceName(),
          manager2TableId);

      // Replicate manager2 in the manager2 cluster to manager1 in the manager2 cluster
      clientManager2.tableOperations().setProperty(manager2Table,
          Property.TABLE_REPLICATION.getKey(), "true");
      clientManager2.tableOperations().setProperty(manager2Table,
          Property.TABLE_REPLICATION_TARGET.getKey() + manager1Cluster.getInstanceName(),
          manager1TableId);

      // Give our replication user the ability to write to the respective table
      clientManager1.securityOperations().grantTablePermission(manager1UserName, manager1Table,
          TablePermission.WRITE);
      clientManager2.securityOperations().grantTablePermission(manager2UserName, manager2Table,
          TablePermission.WRITE);

      IteratorSetting summingCombiner = new IteratorSetting(50, SummingCombiner.class);
      SummingCombiner.setEncodingType(summingCombiner, Type.STRING);
      SummingCombiner.setCombineAllColumns(summingCombiner, true);

      // Set a combiner on both instances that will sum multiple values
      // We can use this to verify that the mutation was not sent multiple times
      clientManager1.tableOperations().attachIterator(manager1Table, summingCombiner);
      clientManager2.tableOperations().attachIterator(manager2Table, summingCombiner);

      // Write a single entry
      try (BatchWriter bw = clientManager1.createBatchWriter(manager1Table)) {
        Mutation m = new Mutation("row");
        m.put("count", "", "1");
        bw.addMutation(m);
      }

      Set<String> files = clientManager1.replicationOperations().referencedFiles(manager1Table);

      log.info("Found {} that need replication from manager1", files);

      // Kill and restart the tserver to close the WAL on manager1
      for (ProcessReference proc : manager1Cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
        manager1Cluster.killProcess(ServerType.TABLET_SERVER, proc);
      }

      manager1Cluster.exec(TabletServer.class);

      log.info("Restarted tserver on manager1");

      // Try to avoid ACCUMULO-2964
      Thread.sleep(1000);

      // Sanity check that the element is there on manager1
      Entry<Key,Value> entry;
      try (Scanner s = clientManager1.createScanner(manager1Table, Authorizations.EMPTY)) {
        entry = getOnlyElement(s);
        assertEquals("1", entry.getValue().toString());

        // Wait for this table to replicate
        clientManager1.replicationOperations().drain(manager1Table, files);

        Thread.sleep(5000);
      }

      // Check that the element made it to manager2 only once
      try (Scanner s = clientManager2.createScanner(manager2Table, Authorizations.EMPTY)) {
        entry = getOnlyElement(s);
        assertEquals("1", entry.getValue().toString());

        // Wait for manager2 to finish replicating it back
        files = clientManager2.replicationOperations().referencedFiles(manager2Table);

        // Kill and restart the tserver to close the WAL on manager2
        for (ProcessReference proc : manager2Cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
          manager2Cluster.killProcess(ServerType.TABLET_SERVER, proc);
        }

        manager2Cluster.exec(TabletServer.class);

        // Try to avoid ACCUMULO-2964
        Thread.sleep(1000);
      }

      // Check that the element made it to manager2 only once
      try (Scanner s = clientManager2.createScanner(manager2Table, Authorizations.EMPTY)) {
        entry = getOnlyElement(s);
        assertEquals("1", entry.getValue().toString());

        clientManager2.replicationOperations().drain(manager2Table, files);

        Thread.sleep(5000);
      }

      // Verify that the entry wasn't sent back to manager1
      try (Scanner s = clientManager1.createScanner(manager1Table, Authorizations.EMPTY)) {
        entry = getOnlyElement(s);
        assertEquals("1", entry.getValue().toString());
      }
    } finally {
      manager1Cluster.stop();
      manager2Cluster.stop();
    }
  }

}
