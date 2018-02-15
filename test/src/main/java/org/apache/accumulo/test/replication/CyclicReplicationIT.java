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

import static org.junit.Assert.assertTrue;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
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
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.minicluster.impl.ZooKeeperBindException;
import org.apache.accumulo.server.replication.ReplicaSystemFactory;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.replication.AccumuloReplicaSystem;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

@Category(MiniClusterOnlyTests.class)
public class CyclicReplicationIT {
  private static final Logger log = LoggerFactory.getLogger(CyclicReplicationIT.class);

  @Rule
  public Timeout getTimeout() {
    int scalingFactor = 1;
    try {
      scalingFactor = Integer.parseInt(System.getProperty("timeout.factor"));
    } catch (NumberFormatException exception) {
      log.warn("Could not parse timeout.factor, not scaling timeout");
    }

    return new Timeout(scalingFactor * 10, TimeUnit.MINUTES);
  }

  @Rule
  public TestName testName = new TestName();

  private File createTestDir(String name) {
    File baseDir = new File(System.getProperty("user.dir") + "/target/mini-tests");
    assertTrue(baseDir.mkdirs() || baseDir.isDirectory());
    File testDir = new File(baseDir, this.getClass().getName() + "_" + testName.getMethodName() + "_" + name);
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
    OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(cluster.getConfig().getConfDir(), "core-site.xml")));
    coreSite.writeXml(out);
    out.close();
  }

  /**
   * Use the same SSL and credential provider configuration that is set up by AbstractMacIT for the other MAC used for replication
   */
  private void updatePeerConfigFromPrimary(MiniAccumuloConfigImpl primaryCfg, MiniAccumuloConfigImpl peerCfg) {
    // Set the same SSL information from the primary when present
    Map<String,String> primarySiteConfig = primaryCfg.getSiteConfig();
    if ("true".equals(primarySiteConfig.get(Property.INSTANCE_RPC_SSL_ENABLED.getKey()))) {
      Map<String,String> peerSiteConfig = new HashMap<>();
      peerSiteConfig.put(Property.INSTANCE_RPC_SSL_ENABLED.getKey(), "true");
      String keystorePath = primarySiteConfig.get(Property.RPC_SSL_KEYSTORE_PATH.getKey());
      Assert.assertNotNull("Keystore Path was null", keystorePath);
      peerSiteConfig.put(Property.RPC_SSL_KEYSTORE_PATH.getKey(), keystorePath);
      String truststorePath = primarySiteConfig.get(Property.RPC_SSL_TRUSTSTORE_PATH.getKey());
      Assert.assertNotNull("Truststore Path was null", truststorePath);
      peerSiteConfig.put(Property.RPC_SSL_TRUSTSTORE_PATH.getKey(), truststorePath);

      // Passwords might be stored in CredentialProvider
      String keystorePassword = primarySiteConfig.get(Property.RPC_SSL_KEYSTORE_PASSWORD.getKey());
      if (null != keystorePassword) {
        peerSiteConfig.put(Property.RPC_SSL_KEYSTORE_PASSWORD.getKey(), keystorePassword);
      }
      String truststorePassword = primarySiteConfig.get(Property.RPC_SSL_TRUSTSTORE_PASSWORD.getKey());
      if (null != truststorePassword) {
        peerSiteConfig.put(Property.RPC_SSL_TRUSTSTORE_PASSWORD.getKey(), truststorePassword);
      }

      System.out.println("Setting site configuration for peer " + peerSiteConfig);
      peerCfg.setSiteConfig(peerSiteConfig);
    }

    // Use the CredentialProvider if the primary also uses one
    String credProvider = primarySiteConfig.get(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey());
    if (null != credProvider) {
      Map<String,String> peerSiteConfig = peerCfg.getSiteConfig();
      peerSiteConfig.put(Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS.getKey(), credProvider);
      peerCfg.setSiteConfig(peerSiteConfig);
    }
  }

  @Test
  public void dataIsNotOverReplicated() throws Exception {
    File master1Dir = createTestDir("master1"), master2Dir = createTestDir("master2");
    String password = "password";

    MiniAccumuloConfigImpl master1Cfg;
    MiniAccumuloClusterImpl master1Cluster;
    while (true) {
      master1Cfg = new MiniAccumuloConfigImpl(master1Dir, password);
      master1Cfg.setNumTservers(1);
      master1Cfg.setInstanceName("master1");

      // Set up SSL if needed
      ConfigurableMacBase.configureForEnvironment(master1Cfg, this.getClass(), ConfigurableMacBase.getSslDir(master1Dir));

      master1Cfg.setProperty(Property.REPLICATION_NAME, master1Cfg.getInstanceName());
      master1Cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "5M");
      master1Cfg.setProperty(Property.REPLICATION_THREADCHECK, "5m");
      master1Cfg.setProperty(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP, "1s");
      master1Cfg.setProperty(Property.MASTER_REPLICATION_SCAN_INTERVAL, "1s");
      master1Cluster = new MiniAccumuloClusterImpl(master1Cfg);
      setCoreSite(master1Cluster);

      try {
        master1Cluster.start();
        break;
      } catch (ZooKeeperBindException e) {
        log.warn("Failed to start ZooKeeper on {}, will retry", master1Cfg.getZooKeeperPort());
      }
    }

    MiniAccumuloConfigImpl master2Cfg;
    MiniAccumuloClusterImpl master2Cluster;
    while (true) {
      master2Cfg = new MiniAccumuloConfigImpl(master2Dir, password);
      master2Cfg.setNumTservers(1);
      master2Cfg.setInstanceName("master2");

      // Set up SSL if needed. Need to share the same SSL truststore as master1
      this.updatePeerConfigFromPrimary(master1Cfg, master2Cfg);

      master2Cfg.setProperty(Property.REPLICATION_NAME, master2Cfg.getInstanceName());
      master2Cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "5M");
      master2Cfg.setProperty(Property.REPLICATION_THREADCHECK, "5m");
      master2Cfg.setProperty(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP, "1s");
      master2Cfg.setProperty(Property.MASTER_REPLICATION_SCAN_INTERVAL, "1s");
      master2Cluster = new MiniAccumuloClusterImpl(master2Cfg);
      setCoreSite(master2Cluster);

      try {
        master2Cluster.start();
        break;
      } catch (ZooKeeperBindException e) {
        log.warn("Failed to start ZooKeeper on {}, will retry", master2Cfg.getZooKeeperPort());
      }
    }

    try {
      Connector connMaster1 = master1Cluster.getConnector("root", new PasswordToken(password)), connMaster2 = master2Cluster.getConnector("root",
          new PasswordToken(password));

      String master1UserName = "master1", master1Password = "foo";
      String master2UserName = "master2", master2Password = "bar";
      String master1Table = master1Cluster.getInstanceName(), master2Table = master2Cluster.getInstanceName();

      connMaster1.securityOperations().createLocalUser(master1UserName, new PasswordToken(master1Password));
      connMaster2.securityOperations().createLocalUser(master2UserName, new PasswordToken(master2Password));

      // Configure the credentials we should use to authenticate ourselves to the peer for replication
      connMaster1.instanceOperations().setProperty(Property.REPLICATION_PEER_USER.getKey() + master2Cluster.getInstanceName(), master2UserName);
      connMaster1.instanceOperations().setProperty(Property.REPLICATION_PEER_PASSWORD.getKey() + master2Cluster.getInstanceName(), master2Password);

      connMaster2.instanceOperations().setProperty(Property.REPLICATION_PEER_USER.getKey() + master1Cluster.getInstanceName(), master1UserName);
      connMaster2.instanceOperations().setProperty(Property.REPLICATION_PEER_PASSWORD.getKey() + master1Cluster.getInstanceName(), master1Password);

      connMaster1.instanceOperations().setProperty(
          Property.REPLICATION_PEERS.getKey() + master2Cluster.getInstanceName(),
          ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
              AccumuloReplicaSystem.buildConfiguration(master2Cluster.getInstanceName(), master2Cluster.getZooKeepers())));

      connMaster2.instanceOperations().setProperty(
          Property.REPLICATION_PEERS.getKey() + master1Cluster.getInstanceName(),
          ReplicaSystemFactory.getPeerConfigurationValue(AccumuloReplicaSystem.class,
              AccumuloReplicaSystem.buildConfiguration(master1Cluster.getInstanceName(), master1Cluster.getZooKeepers())));

      connMaster1.tableOperations().create(master1Table, new NewTableConfiguration().withoutDefaultIterators());
      String master1TableId = connMaster1.tableOperations().tableIdMap().get(master1Table);
      Assert.assertNotNull(master1TableId);

      connMaster2.tableOperations().create(master2Table, new NewTableConfiguration().withoutDefaultIterators());
      String master2TableId = connMaster2.tableOperations().tableIdMap().get(master2Table);
      Assert.assertNotNull(master2TableId);

      // Replicate master1 in the master1 cluster to master2 in the master2 cluster
      connMaster1.tableOperations().setProperty(master1Table, Property.TABLE_REPLICATION.getKey(), "true");
      connMaster1.tableOperations().setProperty(master1Table, Property.TABLE_REPLICATION_TARGET.getKey() + master2Cluster.getInstanceName(), master2TableId);

      // Replicate master2 in the master2 cluster to master1 in the master2 cluster
      connMaster2.tableOperations().setProperty(master2Table, Property.TABLE_REPLICATION.getKey(), "true");
      connMaster2.tableOperations().setProperty(master2Table, Property.TABLE_REPLICATION_TARGET.getKey() + master1Cluster.getInstanceName(), master1TableId);

      // Give our replication user the ability to write to the respective table
      connMaster1.securityOperations().grantTablePermission(master1UserName, master1Table, TablePermission.WRITE);
      connMaster2.securityOperations().grantTablePermission(master2UserName, master2Table, TablePermission.WRITE);

      IteratorSetting summingCombiner = new IteratorSetting(50, SummingCombiner.class);
      SummingCombiner.setEncodingType(summingCombiner, Type.STRING);
      SummingCombiner.setCombineAllColumns(summingCombiner, true);

      // Set a combiner on both instances that will sum multiple values
      // We can use this to verify that the mutation was not sent multiple times
      connMaster1.tableOperations().attachIterator(master1Table, summingCombiner);
      connMaster2.tableOperations().attachIterator(master2Table, summingCombiner);

      // Write a single entry
      BatchWriter bw = connMaster1.createBatchWriter(master1Table, new BatchWriterConfig());
      Mutation m = new Mutation("row");
      m.put("count", "", "1");
      bw.addMutation(m);
      bw.close();

      Set<String> files = connMaster1.replicationOperations().referencedFiles(master1Table);

      log.info("Found {} that need replication from master1", files);

      // Kill and restart the tserver to close the WAL on master1
      for (ProcessReference proc : master1Cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
        master1Cluster.killProcess(ServerType.TABLET_SERVER, proc);
      }

      master1Cluster.exec(TabletServer.class);

      log.info("Restarted tserver on master1");

      // Try to avoid ACCUMULO-2964
      Thread.sleep(1000);

      // Sanity check that the element is there on master1
      Entry<Key,Value> entry;
      try (Scanner s = connMaster1.createScanner(master1Table, Authorizations.EMPTY)) {
        entry = Iterables.getOnlyElement(s);
        Assert.assertEquals("1", entry.getValue().toString());

        // Wait for this table to replicate
        connMaster1.replicationOperations().drain(master1Table, files);

        Thread.sleep(5000);
      }

      // Check that the element made it to master2 only once
      try (Scanner s = connMaster2.createScanner(master2Table, Authorizations.EMPTY)) {
        entry = Iterables.getOnlyElement(s);
        Assert.assertEquals("1", entry.getValue().toString());

        // Wait for master2 to finish replicating it back
        files = connMaster2.replicationOperations().referencedFiles(master2Table);

        // Kill and restart the tserver to close the WAL on master2
        for (ProcessReference proc : master2Cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
          master2Cluster.killProcess(ServerType.TABLET_SERVER, proc);
        }

        master2Cluster.exec(TabletServer.class);

        // Try to avoid ACCUMULO-2964
        Thread.sleep(1000);
      }

      // Check that the element made it to master2 only once
      try (Scanner s = connMaster2.createScanner(master2Table, Authorizations.EMPTY)) {
        entry = Iterables.getOnlyElement(s);
        Assert.assertEquals("1", entry.getValue().toString());

        connMaster2.replicationOperations().drain(master2Table, files);

        Thread.sleep(5000);
      }

      // Verify that the entry wasn't sent back to master1
      try (Scanner s = connMaster1.createScanner(master1Table, Authorizations.EMPTY)) {
        entry = Iterables.getOnlyElement(s);
        Assert.assertEquals("1", entry.getValue().toString());
      }
    } finally {
      master1Cluster.stop();
      master2Cluster.stop();
    }
  }

}
