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
package org.apache.accumulo.test.upgrade;

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.test.ComprehensiveBaseIT.createSplits;
import static org.apache.accumulo.test.ComprehensiveBaseIT.generateKeys;
import static org.apache.accumulo.test.ComprehensiveBaseIT.scan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.WithTestNames;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.AccumuloStatus;
import org.apache.accumulo.test.ComprehensiveIT;
import org.apache.accumulo.test.util.Wait;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MoreCollectors;

@Disabled
@Tag(MINI_CLUSTER_ONLY)
public class UpgradeIT extends WithTestNames {

  private static final Logger log = LoggerFactory.getLogger(UpgradeIT.class);

  @Test
  public void testBasic() throws Exception {
    runUpgradeTest("basic", (client, serverContext, cluster) -> {
      // wait for upgrade to complete, scanning a table before upgrade is complete will fail with
      // error if tablet availability is not present
      Wait.waitFor(
          () -> AccumuloDataVersion.get() == AccumuloDataVersion.getCurrentVersion(serverContext));

      var seenTables = client.tableOperations().list().stream()
          .filter(tableName -> !tableName.startsWith("accumulo.")).collect(Collectors.toSet());
      assertEquals(Set.of("ut1", "ut2", "ut3"), seenTables);

      assertEquals(generateKeys(0, 1000, 3, tr -> true),
          scan(client, "ut1", ComprehensiveIT.AUTHORIZATIONS));
      assertEquals(Map.of(), scan(client, "ut2", Authorizations.EMPTY));
      assertEquals(generateKeys(0, 1000, 7, tr -> true),
          scan(client, "ut3", ComprehensiveIT.AUTHORIZATIONS));

      assertTrue(client.tableOperations().listSplits("ut1").isEmpty());
      assertTrue(client.tableOperations().listSplits("ut2").isEmpty());
      assertEquals(createSplits(0, 1000, 13),
          new TreeSet<>(client.tableOperations().listSplits("ut3")));

      assertEquals("3.14", client.tableOperations().getTableProperties("ut1")
          .get(Property.TABLE_MAJC_RATIO.getKey()));
      assertNull(client.tableOperations().getTableProperties("ut2")
          .get(Property.TABLE_MAJC_RATIO.getKey()));
      assertEquals("2.72", client.tableOperations().getTableProperties("ut3")
          .get(Property.TABLE_MAJC_RATIO.getKey()));

    });
  }

  @Test
  public void testFate() throws Exception {
    runUpgradeTest("fate", (client, serverContext, cluster) -> {
      // When fate operations are present the manager process should exit with a non zero exit code
      // and no upgrade should happen.
      var managerProcess = cluster.getProcesses().get(ServerType.MANAGER).stream()
          .collect(MoreCollectors.onlyElement()).getProcess();
      assertTrue(managerProcess.waitFor(60, TimeUnit.SECONDS));
      assertNotEquals(0, managerProcess.exitValue());
      assertNotEquals(AccumuloDataVersion.get(),
          AccumuloDataVersion.getCurrentVersion(serverContext));
    });
  }

  private interface Verifier {
    void verify(AccumuloClient client, ServerContext serverContext, MiniAccumuloClusterImpl cluster)
        throws Exception;
  }

  private void runUpgradeTest(String testName, Verifier verifier) throws Exception {
    var versions = UpgradeTestUtils.findVersions(testName);

    int run = 0;

    for (var version : versions) {
      if (version.equals(Constants.VERSION)) {
        log.info("Skipping self {} ", Constants.VERSION);
        continue;
      }

      log.info("Running upgrade test: {} {} -> {}", testName, version, Constants.VERSION);

      var originalDir = UpgradeTestUtils.getTestDir(version, testName);
      UpgradeTestUtils.backupOrRestore(version, testName);

      var newMacDir = UpgradeTestUtils.getTestDir(Constants.VERSION, testName);
      FileUtils.deleteQuietly(newMacDir);

      File csFile = new File(originalDir, "conf/hdfs-site.xml");
      Configuration hadoopSite = new Configuration();
      hadoopSite.set("fs.defaultFS", "file:///");
      try (OutputStream out =
          new BufferedOutputStream(new FileOutputStream(csFile.getAbsolutePath()))) {
        hadoopSite.writeXml(out);
      }

      var accumuloProps = new File(originalDir, "conf/accumulo.properties");

      MiniAccumuloConfigImpl config =
          new MiniAccumuloConfigImpl(newMacDir, UpgradeTestUtils.ROOT_PASSWORD);
      config.useExistingInstance(accumuloProps, new File(originalDir, "conf"));

      var cluster = new MiniAccumuloClusterImpl(config);

      var zKProc = cluster._exec(cluster.getConfig().getServerClass(ServerType.ZOOKEEPER),
          ServerType.ZOOKEEPER, Map.of(), new File(originalDir, "conf/zoo.cfg").getAbsolutePath());

      try (var serverContext = new ServerContext(SiteConfiguration.fromFile(accumuloProps).build());
          var client = Accumulo.newClient().from(cluster.getClientProperties()).build()) {
        // MiniAccumulo will be unhappy if server processes appear to be running. There may be
        // ephemeral nodes in zookeeper related to servers that need to timeout. Wait for any of
        // these nodes to go away. TODO this does not wait for scan servers or compactors.
        Wait.waitFor(() -> AccumuloStatus.isAccumuloOffline(serverContext.getZooReader(),
            serverContext.getZooKeeperRoot()), 60_000);
        cluster.start();
        verifier.verify(client, serverContext, cluster);
      } finally {
        zKProc.getProcess().destroyForcibly();
        // The cluster stop method will not kill processes because the cluster was started using an
        // existing instance.
        UpgradeTestUtils.killAll(cluster);
        cluster.stop();
      }
      run++;
    }

    assertTrue(run > 0);
  }
}
