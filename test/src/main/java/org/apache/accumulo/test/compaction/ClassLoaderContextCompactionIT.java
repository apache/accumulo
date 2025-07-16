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
package org.apache.accumulo.test.compaction;

import static org.apache.accumulo.core.conf.Property.TABLE_FILE_MAX;
import static org.apache.accumulo.core.conf.Property.TABLE_MAJC_RATIO;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.List;

import org.apache.accumulo.compactor.Compactor;
import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

public class ClassLoaderContextCompactionIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
    // After 1 failure start backing off by 5s.
    // After 3 failures, terminate the Compactor
    cfg.setProperty(Property.COMPACTOR_FAILURE_BACKOFF_THRESHOLD, "1");
    cfg.setProperty(Property.COMPACTOR_FAILURE_BACKOFF_INTERVAL, "5s");
    cfg.setProperty(Property.COMPACTOR_FAILURE_BACKOFF_RESET, "10m");
    cfg.setProperty(Property.COMPACTOR_FAILURE_TERMINATION_THRESHOLD, "3");
    cfg.setNumCompactors(2);
  }

  @Test
  public void testClassLoaderContextErrorKillsCompactor() throws Exception {
    final String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      getCluster().getClusterControl().startCoordinator(CompactionCoordinator.class);
      getCluster().getClusterControl().startCompactors(Compactor.class, 1, QUEUE1);
      Wait.waitFor(
          () -> ExternalCompactionUtil.countCompactors(QUEUE1, (ClientContext) client) == 1);
      List<HostAndPort> compactors =
          ExternalCompactionUtil.getCompactorAddrs((ClientContext) client).get(QUEUE1);
      assertEquals(1, compactors.size());
      final HostAndPort compactorAddr = compactors.get(0);
      createTable(client, table1, "cs1");
      client.tableOperations().setProperty(table1, TABLE_FILE_MAX.getKey(), "1001");
      client.tableOperations().setProperty(table1, TABLE_MAJC_RATIO.getKey(), "1001");
      TableId tid = TableId.of(client.tableOperations().tableIdMap().get(table1));

      ReadWriteIT.ingest(client, 1000, 1, 1, 0, "colf", table1, 20);

      Ample ample = ((ClientContext) client).getAmple();
      try (
          TabletsMetadata tms = ample.readTablets().forTable(tid).fetch(ColumnType.FILES).build()) {
        TabletMetadata tm = tms.iterator().next();
        assertEquals(50, tm.getFiles().size());
      }

      final MiniAccumuloClusterImpl cluster = (MiniAccumuloClusterImpl) getCluster();
      final FileSystem fs = cluster.getFileSystem();

      // Create the context directory in HDFS
      final org.apache.hadoop.fs.Path contextDir = fs.makeQualified(new org.apache.hadoop.fs.Path(
          cluster.getConfig().getAccumuloDir().toString(), "classpath"));
      assertTrue(fs.mkdirs(contextDir));

      // Copy the FooFilter.jar to the context dir
      final org.apache.hadoop.fs.Path src = new org.apache.hadoop.fs.Path(
          System.getProperty("java.io.tmpdir") + "/classes/org/apache/accumulo/test/FooFilter.jar");
      final org.apache.hadoop.fs.Path dst = new org.apache.hadoop.fs.Path(contextDir, "Test.jar");
      fs.copyFromLocalFile(src, dst);
      assertTrue(fs.exists(dst));

      // Define a classloader context that references Test.jar
      @SuppressWarnings("removal")
      final Property p = Property.VFS_CONTEXT_CLASSPATH_PROPERTY;
      client.instanceOperations().setProperty(p.getKey() + "undefined", dst.toUri().toString());

      // Force the classloader to look in the context jar first, don't delegate to the parent first
      client.instanceOperations().setProperty("general.vfs.context.classpath.undefined.delegation",
          "post");

      // Set the context on the table
      client.tableOperations().setProperty(table1, Property.TABLE_CLASSLOADER_CONTEXT.getKey(),
          "undefined");

      final IteratorSetting cfg =
          new IteratorSetting(101, "FooFilter", "org.apache.accumulo.test.FooFilter");
      client.tableOperations().attachIterator(table1, cfg, EnumSet.of(IteratorScope.majc));

      // delete Test.jar, so that the classloader will fail
      assertTrue(fs.delete(dst, false));

      // Start a compaction. The missing jar should cause a failure
      client.tableOperations().compact(table1, new CompactionConfig().setWait(false));
      Wait.waitFor(
          () -> ExternalCompactionUtil.getRunningCompaction(compactorAddr, (ClientContext) client)
              == null);
      assertEquals(1, ExternalCompactionUtil.countCompactors(QUEUE1, (ClientContext) client));

      client.tableOperations().compact(table1, new CompactionConfig().setWait(false));
      Wait.waitFor(
          () -> ExternalCompactionUtil.getRunningCompaction(compactorAddr, (ClientContext) client)
              == null);
      assertEquals(1, ExternalCompactionUtil.countCompactors(QUEUE1, (ClientContext) client));

      client.tableOperations().compact(table1, new CompactionConfig().setWait(false));
      Wait.waitFor(
          () -> ExternalCompactionUtil.getRunningCompaction(compactorAddr, (ClientContext) client)
              == null);
      assertEquals(1, ExternalCompactionUtil.countCompactors(QUEUE1, (ClientContext) client));

      // Three failures have occurred, Compactor should shut down.
      Wait.waitFor(
          () -> ExternalCompactionUtil.countCompactors(QUEUE1, (ClientContext) client) == 0);

    } finally {
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTION_COORDINATOR);
    }

  }

}
