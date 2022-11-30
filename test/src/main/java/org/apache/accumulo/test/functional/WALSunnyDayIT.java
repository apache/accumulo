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

import static org.apache.accumulo.core.conf.Property.GC_CYCLE_DELAY;
import static org.apache.accumulo.core.conf.Property.GC_CYCLE_START;
import static org.apache.accumulo.core.conf.Property.INSTANCE_ZK_TIMEOUT;
import static org.apache.accumulo.core.conf.Property.TSERV_WAL_MAX_SIZE;
import static org.apache.accumulo.core.conf.Property.TSERV_WAL_REPLICATION;
import static org.apache.accumulo.core.security.Authorizations.EMPTY;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.apache.accumulo.minicluster.ServerType.GARBAGE_COLLECTOR;
import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.manager.state.SetGoalState;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterControl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalMarkerException;
import org.apache.accumulo.server.log.WalStateManager.WalState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterators;

@Tag(SUNNY_DAY)
public class WALSunnyDayIT extends ConfigurableMacBase {

  private static final Text CF = new Text(new byte[0]);

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(GC_CYCLE_DELAY, "1s");
    cfg.setProperty(GC_CYCLE_START, "0s");
    cfg.setProperty(TSERV_WAL_MAX_SIZE, "1M");
    cfg.setProperty(TSERV_WAL_REPLICATION, "1");
    cfg.setProperty(INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setNumTservers(1);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  int countInUse(Collection<WalState> bools) {
    int result = 0;
    for (WalState b : bools) {
      if (b != WalState.UNREFERENCED) {
        result++;
      }
    }
    return result;
  }

  @Test
  public void test() throws Exception {
    MiniAccumuloClusterImpl mac = getCluster();
    MiniAccumuloClusterControl control = mac.getClusterControl();
    control.stop(GARBAGE_COLLECTOR);
    ServerContext context = getServerContext();
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      writeSomeData(c, tableName, 1, 1);

      // wal markers are added lazily
      Map<String,WalState> wals = getWALsAndAssertCount(context, 2);
      assertEquals(2, countInUse(wals.values()), "all WALs should be in use");

      // roll log, get a new next
      writeSomeData(c, tableName, 1001, 50);
      Map<String,WalState> walsAfterRoll = getWALsAndAssertCount(context, 3);
      assertTrue(walsAfterRoll.keySet().containsAll(wals.keySet()),
          "new WALs should be a superset of the old WALs");
      assertEquals(3, countInUse(walsAfterRoll.values()), "all WALs should be in use");

      // flush the tables
      for (String table : new String[] {tableName, MetadataTable.NAME, RootTable.NAME}) {
        c.tableOperations().flush(table, null, null, true);
      }
      sleepUninterruptibly(1, TimeUnit.SECONDS);
      // rolled WAL is no longer in use, but needs to be GC'd
      Map<String,WalState> walsAfterflush = getWALsAndAssertCount(context, 3);
      assertEquals(2, countInUse(walsAfterflush.values()), "inUse should be 2");

      // let the GC run for a little bit
      control.start(GARBAGE_COLLECTOR);
      sleepUninterruptibly(5, TimeUnit.SECONDS);
      // make sure the unused WAL goes away
      getWALsAndAssertCount(context, 2);
      control.stop(GARBAGE_COLLECTOR);
      // restart the tserver, but don't run recovery on all tablets
      control.stop(TABLET_SERVER);
      // this delays recovery on the normal tables
      assertEquals(0, cluster.exec(SetGoalState.class, "SAFE_MODE").getProcess().waitFor());
      control.start(TABLET_SERVER);

      // wait for the metadata table to go back online
      getRecoveryMarkers(c);
      // allow a little time for the manager to notice ASSIGNED_TO_DEAD_SERVER tablets
      sleepUninterruptibly(5, TimeUnit.SECONDS);
      Map<KeyExtent,List<String>> markers = getRecoveryMarkers(c);
      // log.debug("markers " + markers);
      assertEquals(1, markers.size(), "one tablet should have markers");
      assertEquals("1", markers.keySet().iterator().next().tableId().canonical(),
          "tableId of the keyExtent should be 1");

      // put some data in the WAL
      assertEquals(0, cluster.exec(SetGoalState.class, "NORMAL").getProcess().waitFor());
      verifySomeData(c, tableName, 1001 * 50 + 1);
      writeSomeData(c, tableName, 100, 100);

      Map<String,WalState> walsAfterRestart = getWALsAndAssertCount(context, 4);
      // log.debug("wals after " + walsAfterRestart);
      assertEquals(4, countInUse(walsAfterRestart.values()), "used WALs after restart should be 4");
      control.start(GARBAGE_COLLECTOR);
      sleepUninterruptibly(5, TimeUnit.SECONDS);
      Map<String,WalState> walsAfterRestartAndGC = getWALsAndAssertCount(context, 2);
      assertEquals(2, countInUse(walsAfterRestartAndGC.values()), "logs in use should be 2");
    }
  }

  private void verifySomeData(AccumuloClient c, String tableName, int expected) throws Exception {
    try (Scanner scan = c.createScanner(tableName, EMPTY)) {
      int result = Iterators.size(scan.iterator());
      assertEquals(expected, result);
    }
  }

  private void writeSomeData(AccumuloClient client, String tableName, int row, int col)
      throws Exception {
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      byte[] rowData = new byte[10];
      byte[] cq = new byte[10];
      byte[] value = new byte[10];

      for (int r = 0; r < row; r++) {
        random.nextBytes(rowData);
        Mutation m = new Mutation(rowData);
        for (int c = 0; c < col; c++) {
          random.nextBytes(cq);
          random.nextBytes(value);
          m.put(CF, new Text(cq), new Value(value));
        }
        bw.addMutation(m);
        if (r % 100 == 0) {
          bw.flush();
        }
      }
    }
  }

  private Map<KeyExtent,List<String>> getRecoveryMarkers(AccumuloClient c) throws Exception {
    Map<KeyExtent,List<String>> result = new HashMap<>();
    try (Scanner root = c.createScanner(RootTable.NAME, EMPTY);
        Scanner meta = c.createScanner(MetadataTable.NAME, EMPTY)) {
      root.setRange(TabletsSection.getRange());
      root.fetchColumnFamily(LogColumnFamily.NAME);
      TabletColumnFamily.PREV_ROW_COLUMN.fetch(root);

      meta.setRange(TabletsSection.getRange());
      meta.fetchColumnFamily(LogColumnFamily.NAME);
      TabletColumnFamily.PREV_ROW_COLUMN.fetch(meta);

      List<String> logs = new ArrayList<>();
      Iterator<Entry<Key,Value>> both = Iterators.concat(root.iterator(), meta.iterator());
      while (both.hasNext()) {
        Entry<Key,Value> entry = both.next();
        Key key = entry.getKey();
        if (key.getColumnFamily().equals(LogColumnFamily.NAME)) {
          logs.add(key.getColumnQualifier().toString());
        }
        if (TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key) && !logs.isEmpty()) {
          KeyExtent extent = KeyExtent.fromMetaPrevRow(entry);
          result.put(extent, logs);
          logs = new ArrayList<>();
        }
      }
    }
    return result;
  }

  private final int TIMES_TO_COUNT = 20;
  private final int PAUSE_BETWEEN_COUNTS = 100;

  private Map<String,WalState> getWALsAndAssertCount(ServerContext c, int expectedCount)
      throws Exception {
    // see https://issues.apache.org/jira/browse/ACCUMULO-4110. Sometimes this test counts the logs
    // before
    // the new standby log is actually ready. So let's try a few times before failing, returning the
    // last
    // wals variable with the the correct count.
    Map<String,WalState> wals = _getWals(c);
    if (wals.size() == expectedCount) {
      return wals;
    }

    int waitLonger = getWaitFactor();
    for (int i = 1; i <= TIMES_TO_COUNT; i++) {
      Thread.sleep(i * PAUSE_BETWEEN_COUNTS * waitLonger);
      wals = _getWals(c);
      if (wals.size() == expectedCount) {
        return wals;
      }
    }

    fail(
        "Unable to get the correct number of WALs, expected " + expectedCount + " but got " + wals);
    return new HashMap<>();
  }

  private int getWaitFactor() {
    int waitLonger = 1;
    String timeoutString = System.getProperty("timeout.factor");
    if (timeoutString != null && !timeoutString.isEmpty()) {
      int timeout = Integer.parseInt(timeoutString);
      if (timeout > 1) {
        waitLonger = timeout;
      }
    }
    return waitLonger;
  }

  static Map<String,WalState> _getWals(ServerContext c) throws Exception {
    while (true) {
      try {
        Map<String,WalState> result = new HashMap<>();
        WalStateManager wals = new WalStateManager(c);
        for (Entry<Path,WalState> entry : wals.getAllState().entrySet()) {
          // WALs are in use if they are not unreferenced
          result.put(entry.getKey().toString(), entry.getValue());
        }
        return result;
      } catch (WalMarkerException wme) {
        if (wme.getCause() instanceof NoNodeException) {
          log.debug("WALs changed while reading, retrying", wme);
        } else {
          throw wme;
        }
      }
    }
  }

}
