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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.conf.Property.GC_CYCLE_DELAY;
import static org.apache.accumulo.core.conf.Property.GC_CYCLE_START;
import static org.apache.accumulo.core.conf.Property.INSTANCE_ZK_TIMEOUT;
import static org.apache.accumulo.core.conf.Property.TSERV_WALOG_MAX_SIZE;
import static org.apache.accumulo.core.conf.Property.TSERV_WAL_REPLICATION;
import static org.apache.accumulo.core.security.Authorizations.EMPTY;
import static org.apache.accumulo.minicluster.ServerType.GARBAGE_COLLECTOR;
import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.CurrentLogsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.master.state.SetGoalState;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterControl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class WALSunnyDayIT extends ConfigurableMacIT {

  private static final Text CF = new Text(new byte[0]);

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(GC_CYCLE_DELAY, "1s");
    cfg.setProperty(GC_CYCLE_START, "0s");
    cfg.setProperty(TSERV_WALOG_MAX_SIZE, "1M");
    cfg.setProperty(TSERV_WAL_REPLICATION, "1");
    cfg.setProperty(INSTANCE_ZK_TIMEOUT, "3s");
    cfg.setNumTservers(1);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  int countTrue(Collection<Boolean> bools) {
    int result = 0;
    for (Boolean b : bools) {
      if (b.booleanValue())
        result++;
    }
    return result;
  }

  @Test
  public void test() throws Exception {
    MiniAccumuloClusterImpl mac = getCluster();
    MiniAccumuloClusterControl control = mac.getClusterControl();
    control.stop(GARBAGE_COLLECTOR);
    Connector c = getConnector();
    ZooKeeper zoo = new ZooKeeper(c.getInstance().getZooKeepers(), c.getInstance().getZooKeepersSessionTimeOut(), new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        log.info(event.toString());
      }
    });
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    writeSomeData(c, tableName, 1, 1);

    // wal markers are added lazily
    Map<String,Boolean> wals = getWals(c, zoo);
    assertEquals(wals.toString(), 1, wals.size());
    for (Boolean b : wals.values()) {
      assertTrue("logs should be in use", b.booleanValue());
    }

    // roll log, get a new next
    writeSomeData(c, tableName, 1000, 50);
    Map<String,Boolean> walsAfterRoll = getWals(c, zoo);
    assertEquals("should have 3 WALs after roll", 2, walsAfterRoll.size());
    assertTrue("new WALs should be a superset of the old WALs", walsAfterRoll.keySet().containsAll(wals.keySet()));
    assertEquals("all WALs should be in use", 2, countTrue(walsAfterRoll.values()));

    // flush the tables
    for (String table : new String[] {tableName, MetadataTable.NAME, RootTable.NAME}) {
      c.tableOperations().flush(table, null, null, true);
    }
    UtilWaitThread.sleep(1000);
    // rolled WAL is no longer in use, but needs to be GC'd
    Map<String,Boolean> walsAfterflush = getWals(c, zoo);
    assertEquals(walsAfterflush.toString(), 2, walsAfterflush.size());
    assertEquals("inUse should be 1", 1, countTrue(walsAfterflush.values()));

    // let the GC run for a little bit
    control.start(GARBAGE_COLLECTOR);
    UtilWaitThread.sleep(5 * 1000);
    // make sure the unused WAL goes away
    Map<String,Boolean> walsAfterGC = getWals(c, zoo);
    assertEquals(walsAfterGC.toString(), 1, walsAfterGC.size());
    control.stop(GARBAGE_COLLECTOR);
    // restart the tserver, but don't run recovery on all tablets
    control.stop(TABLET_SERVER);
    // this delays recovery on the normal tables
    assertEquals(0, cluster.exec(SetGoalState.class, "SAFE_MODE").waitFor());
    control.start(TABLET_SERVER);

    // wait for the metadata table to go back online
    getRecoveryMarkers(c);
    // allow a little time for the master to notice ASSIGNED_TO_DEAD_SERVER tablets
    UtilWaitThread.sleep(5 * 1000);
    Map<KeyExtent,List<String>> markers = getRecoveryMarkers(c);
    // log.debug("markers " + markers);
    assertEquals("one tablet should have markers", 1, markers.keySet().size());
    assertEquals("tableId of the keyExtent should be 1", markers.keySet().iterator().next().getTableId(), new Text("1"));

    // put some data in the WAL
    assertEquals(0, cluster.exec(SetGoalState.class, "NORMAL").waitFor());
    verifySomeData(c, tableName, 1000 * 50 + 1);
    writeSomeData(c, tableName, 100, 100);

    Map<String,Boolean> walsAfterRestart = getWals(c, zoo);
    // log.debug("wals after " + walsAfterRestart);
    assertEquals("used WALs after restart should be 1", 1, countTrue(walsAfterRestart.values()));
    control.start(GARBAGE_COLLECTOR);
    UtilWaitThread.sleep(5 * 1000);
    Map<String,Boolean> walsAfterRestartAndGC = getWals(c, zoo);
    assertEquals("wals left should be 1", 1, walsAfterRestartAndGC.size());
    assertEquals("logs in use should be 1", 1, countTrue(walsAfterRestartAndGC.values()));
  }

  private void verifySomeData(Connector c, String tableName, int expected) throws Exception {
    Scanner scan = c.createScanner(tableName, EMPTY);
    int result = Iterators.size(scan.iterator());
    scan.close();
    Assert.assertEquals(expected, result);
  }

  private void writeSomeData(Connector conn, String tableName, int row, int col) throws Exception {
    Random rand = new Random();
    BatchWriter bw = conn.createBatchWriter(tableName, null);
    byte[] rowData = new byte[10];
    byte[] cq = new byte[10];
    byte[] value = new byte[10];

    for (int r = 0; r < row; r++) {
      rand.nextBytes(rowData);
      Mutation m = new Mutation(rowData);
      for (int c = 0; c < col; c++) {
        rand.nextBytes(cq);
        rand.nextBytes(value);
        m.put(CF, new Text(cq), new Value(value));
      }
      bw.addMutation(m);
      if (r % 100 == 0) {
        bw.flush();
      }
    }
    bw.close();
  }

  private Map<String,Boolean> getWals(Connector c, ZooKeeper zoo) throws Exception {
    Map<String,Boolean> result = new HashMap<>();
    Scanner root = c.createScanner(RootTable.NAME, EMPTY);
    root.setRange(CurrentLogsSection.getRange());
    Scanner meta = c.createScanner(MetadataTable.NAME, EMPTY);
    meta.setRange(root.getRange());
    Iterator<Entry<Key,Value>> both = Iterators.concat(root.iterator(), meta.iterator());
    while (both.hasNext()) {
      Entry<Key,Value> entry = both.next();
      Text path = new Text();
      CurrentLogsSection.getPath(entry.getKey(), path);
      result.put(path.toString(), entry.getValue().get().length == 0);
    }
    String zpath = ZooUtil.getRoot(c.getInstance()) + RootTable.ZROOT_TABLET_CURRENT_LOGS;
    List<String> children = zoo.getChildren(zpath, null);
    for (String child : children) {
      byte[] data = zoo.getData(zpath + "/" + child, null, null);
      result.put(new String(data), true);
    }
    return result;
  }

  private Map<KeyExtent,List<String>> getRecoveryMarkers(Connector c) throws Exception {
    Map<KeyExtent,List<String>> result = new HashMap<>();
    Scanner root = c.createScanner(RootTable.NAME, EMPTY);
    root.setRange(TabletsSection.getRange());
    root.fetchColumnFamily(TabletsSection.LogColumnFamily.NAME);
    TabletColumnFamily.PREV_ROW_COLUMN.fetch(root);

    Scanner meta = c.createScanner(MetadataTable.NAME, EMPTY);
    meta.setRange(TabletsSection.getRange());
    meta.fetchColumnFamily(TabletsSection.LogColumnFamily.NAME);
    TabletColumnFamily.PREV_ROW_COLUMN.fetch(meta);

    List<String> logs = new ArrayList<>();
    Iterator<Entry<Key,Value>> both = Iterators.concat(root.iterator(), meta.iterator());
    while (both.hasNext()) {
      Entry<Key,Value> entry = both.next();
      Key key = entry.getKey();
      if (key.getColumnFamily().equals(TabletsSection.LogColumnFamily.NAME)) {
        logs.add(key.getColumnQualifier().toString());
      }
      if (TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key) && !logs.isEmpty()) {
        KeyExtent extent = new KeyExtent(key.getRow(), entry.getValue());
        result.put(extent, logs);
        logs = new ArrayList<String>();
      }
    }
    return result;
  }

}
