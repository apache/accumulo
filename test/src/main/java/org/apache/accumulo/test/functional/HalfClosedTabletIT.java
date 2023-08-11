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
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

// This IT tests the cases seen in https://github.com/apache/accumulo/issues/3674
// where a failing minor compaction causes a Tablet.initiateClose to leave the
// Tablet in a half-closed state. The half-closed Tablet cannot be unloaded and
// the TabletServer cannot be shutdown normally. Because the minor compaction has
// been failing the Tablet needs to be recovered when it's ultimately re-assigned.
//
public class HalfClosedTabletIT extends SharedMiniClusterBase {

  public static class HalfClosedTabletITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      cfg.setNumTservers(1);
    }

  }

  @BeforeAll
  public static void startup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new HalfClosedTabletITConfiguration());
  }

  @AfterAll
  public static void shutdown() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testSplitWithInvalidContext() throws Exception {

    // In this scenario the table has been mis-configured with an invalid context name.
    // The minor compaction task is failing because classes like the volume chooser or
    // user iterators cannot be loaded. The user calls Tablet.split which calls initiateClose.
    // This test ensures that the Tablet can still be unloaded normally by taking if offline
    // after the split call with an invalid context. The context property is removed after the
    // split call below to get the minor compaction task to succeed on a subsequent run. Because
    // the minor compaction task backs off when retrying, this could take some time.

    String tableName = getUniqueNames(1)[0];

    try (final var client = Accumulo.newClient().from(getClientProps()).build()) {
      final var tops = client.tableOperations();
      tops.create(tableName);
      TableId tableId = TableId.of(tops.tableIdMap().get(tableName));
      try (final var bw = client.createBatchWriter(tableName)) {
        final var m1 = new Mutation("a");
        final var m2 = new Mutation("b");
        m1.put(new Text("cf"), new Text(), new Value());
        m2.put(new Text("cf"), new Text(), new Value());
        bw.addMutation(m1);
        bw.addMutation(m2);
      }

      setInvalidClassLoaderContextPropertyWithoutValidation(getCluster().getServerContext(),
          tableId);

      // Need to wait for TabletServer to pickup configuration change
      Thread.sleep(3000);

      Thread configFixer = new Thread(() -> {
        UtilWaitThread.sleep(3000);
        removeInvalidClassLoaderContextProperty(client, tableName);
      });

      long t1 = System.nanoTime();
      configFixer.start();

      // The split will probably start running w/ bad config that will cause it to get stuck.
      // However once the config is fixed by the background thread it should continue.
      tops.addSplits(tableName, Sets.newTreeSet(List.of(new Text("b"))));

      long t2 = System.nanoTime();
      // expect that split took at least 3 seconds because that is the time it takes to fix the
      // config
      assertTrue(TimeUnit.NANOSECONDS.toMillis(t2 - t1) >= 3000);

      // offline the table which will unload the tablets. If the context property is not
      // removed above, then this test will fail because the tablets will not be able to be
      // unloaded
      tops.offline(tableName);

      Wait.waitFor(() -> countHostedTablets(client, tableId) == 0L, 340_000);
    }
  }

  @Test
  public void testIteratorThrowingTransientError() throws Exception {

    // In this scenario a minc iterator throws an error some number of time, then
    // succeeds. We want to verify that the minc is being retried and the tablet
    // can be closed.

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = getUniqueNames(1)[0];
      final var tops = c.tableOperations();

      tops.create(tableName);
      final var tid = TableId.of(tops.tableIdMap().get(tableName));

      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation(new Text("r1"));
        m.put("acf", tableName, "1");
        bw.addMutation(m);
      }

      IteratorSetting setting = new IteratorSetting(50, "error", ErrorThrowingIterator.class);
      setting.addOption(ErrorThrowingIterator.TIMES, "3");
      c.tableOperations().attachIterator(tableName, setting, EnumSet.of(IteratorScope.minc));
      c.tableOperations().compact(tableName, new CompactionConfig().setWait(true).setFlush(true));

      // Taking the table offline should succeed normally
      tops.offline(tableName);

      // Minc should have completed successfully
      Wait.waitFor(() -> tabletHasExpectedRFiles(c, tableName, 1, 1, 1, 1), 340_000);

      Wait.waitFor(() -> countHostedTablets(c, tid) == 0L, 340_000);

    }
  }

  // Note that these tests can talk several minutes each because by the time the test
  // code changes the configuration, the minc has failed so many times that the minc
  // is waiting for a few minutes before trying again. For example, I saw this backoff
  // timing:
  //
  // DEBUG: MinC failed sleeping 169 ms before retrying
  // DEBUG: MinC failed sleeping 601 ms before retrying
  // DEBUG: MinC failed sleeping 2004 ms before retrying
  // DEBUG: MinC failed sleeping 11891 ms before retrying
  // DEBUG: MinC failed sleeping 43156 ms before retrying
  // DEBUG: MinC failed sleeping 179779 ms before retrying
  @Test
  public void testBadIteratorOnStack() throws Exception {

    // In this scenario the table is using an iterator for minc that is throwing an exception.
    // This test ensures that the Tablet can still be unloaded normally by taking if offline
    // after the bad iterator has been removed from the minc configuration.

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = getUniqueNames(1)[0];
      final var tops = c.tableOperations();

      tops.create(tableName);
      final var tid = TableId.of(tops.tableIdMap().get(tableName));

      IteratorSetting is = new IteratorSetting(30, BadIterator.class);
      c.tableOperations().attachIterator(tableName, is, EnumSet.of(IteratorScope.minc));

      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation(new Text("r1"));
        m.put("acf", tableName, "1");
        bw.addMutation(m);
      }

      c.tableOperations().flush(tableName, null, null, false);

      UtilWaitThread.sleepUninterruptibly(5, TimeUnit.SECONDS);

      // minc should fail, so there should be no files
      FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 0, 0);

      // tell the server to take the table offline
      tops.offline(tableName);

      // The offine operation should not be able to complete because the tablet can not minor
      // compact, give the offline operation a bit of time to attempt to complete even though it
      // should never be able to complete.
      UtilWaitThread.sleepUninterruptibly(5, TimeUnit.SECONDS);

      assertTrue(countHostedTablets(c, tid) > 0);

      // minc should fail, so there should be no files
      FunctionalTestUtils.checkRFiles(c, tableName, 1, 1, 0, 0);

      // remove the bad iterator. The failing minc task is in a backoff retry loop
      // and should pick up this change on the next try
      c.tableOperations().removeIterator(tableName, BadIterator.class.getSimpleName(),
          EnumSet.of(IteratorScope.minc));

      // Minc should have completed successfully
      Wait.waitFor(() -> tabletHasExpectedRFiles(c, tableName, 1, 1, 1, 1), 340_000);

      // The previous operation to offline the table should be able to succeed after the minor
      // compaction completed
      Wait.waitFor(() -> countHostedTablets(c, tid) == 0L, 340_000);
    }
  }

  public static void setInvalidClassLoaderContextPropertyWithoutValidation(ServerContext context,
      TableId tableId) {
    TablePropKey key = TablePropKey.of(context, tableId);
    context.getPropStore().putAll(key,
        Map.of(Property.TABLE_CLASSLOADER_CONTEXT.getKey(), "invalid"));
  }

  public static void removeInvalidClassLoaderContextProperty(AccumuloClient client,
      String tableName) {
    try {
      client.tableOperations().removeProperty(tableName,
          Property.TABLE_CLASSLOADER_CONTEXT.getKey());
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean tabletHasExpectedRFiles(AccumuloClient c, String tableName, int minTablets,
      int maxTablets, int minRFiles, int maxRFiles) {
    try {
      FunctionalTestUtils.checkRFiles(c, tableName, minTablets, maxTablets, minRFiles, maxRFiles);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public static long countHostedTablets(AccumuloClient c, TableId tid) {
    try (TabletsMetadata tm = ((ClientContext) c).getAmple().readTablets().forTable(tid)
        .fetch(ColumnType.LOCATION).build()) {
      return tm.stream().count();
    }
  }
}
