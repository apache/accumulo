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
package org.apache.accumulo.test.upgrade;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.master.upgrade.Upgrader9to10;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessNotFoundException;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class GCUpgrade9to10TestIT extends ConfigurableMacBase {
  private static final String OUR_SECRET = "itsreallysecret";
  private static final String OLDDELPREFIX = "~del";
  private static final Upgrader9to10 upgrader = new Upgrader9to10();

  @Override
  public int defaultTimeoutSeconds() {
    return 5 * 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.INSTANCE_SECRET, OUR_SECRET);
    cfg.setDefaultMemory(64, MemoryUnit.MEGABYTE);
    cfg.setMemory(ServerType.MASTER, 16, MemoryUnit.MEGABYTE);
    cfg.setMemory(ServerType.ZOOKEEPER, 32, MemoryUnit.MEGABYTE);
    cfg.setProperty(Property.GC_CYCLE_START, "1");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1");
    cfg.setProperty(Property.GC_PORT, "0");
    cfg.setProperty(Property.TSERV_MAXMEM, "5K");
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "1");

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  private void killMacGc() throws ProcessNotFoundException, InterruptedException, KeeperException {
    // kill gc started by MAC
    getCluster().killProcess(ServerType.GARBAGE_COLLECTOR,
        getCluster().getProcesses().get(ServerType.GARBAGE_COLLECTOR).iterator().next());
    // delete lock in zookeeper if there, this will allow next GC to start quickly
    String path = getServerContext().getZooKeeperRoot() + Constants.ZGC_LOCK;
    ZooReaderWriter zk = new ZooReaderWriter(cluster.getZooKeepers(), 30000, OUR_SECRET);
    try {
      ZooLock.deleteLock(zk, path);
    } catch (IllegalStateException e) {
      log.error("Unable to delete ZooLock for mini accumulo-gc", e);
    }

    assertNull(getCluster().getProcesses().get(ServerType.GARBAGE_COLLECTOR));
  }

  @Test
  public void gcUpgradeRootTableDeletesIT() throws Exception {
    gcUpgradeDeletesTest(Ample.DataLevel.METADATA, 3);
  }

  @Test
  public void gcUpgradeMetadataTableDeletesIT() throws Exception {
    gcUpgradeDeletesTest(Ample.DataLevel.USER, 3);
  }

  @Test
  public void gcUpgradeNoDeletesIT() throws Exception {
    gcUpgradeDeletesTest(Ample.DataLevel.METADATA, 0);

  }

  /**
   * This is really hard to make happen - the minicluster can only use so little memory to start up.
   * The {@link org.apache.accumulo.master.upgrade.Upgrader9to10} CANDIDATE_MEMORY_PERCENTAGE can be
   * adjusted.
   */
  @Test
  public void gcUpgradeOutofMemoryTest() throws Exception {
    killMacGc(); // we do not want anything deleted

    int somebignumber = 100000;
    String longpathname = "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee"
        + "ffffffffffgggggggggghhhhhhhhhhiiiiiiiiiijjjjjjjjjj"
        + "kkkkkkkkkkkkkkkkkklllllllllllllllllllllmmmmmmmmmmmmmmmmmnnnnnnnnnnnnnnnn";
    longpathname += longpathname; // make it even longer
    Ample.DataLevel level = Ample.DataLevel.USER;

    log.info("Filling metadata table with lots of bogus delete flags");
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      addEntries(c, level.metaTable(), somebignumber, longpathname);

      sleepUninterruptibly(1, TimeUnit.SECONDS);
      upgrader.upgradeFileDeletes(getServerContext(), level);

      sleepUninterruptibly(1, TimeUnit.SECONDS);
      Range range = MetadataSchema.DeletesSection.getRange();
      Scanner scanner;
      try {
        scanner = c.createScanner(level.metaTable(), Authorizations.EMPTY);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
      scanner.setRange(range);
      assertEquals(somebignumber, Iterators.size(scanner.iterator()));
    }
  }

  private void gcUpgradeDeletesTest(Ample.DataLevel level, int count) throws Exception {
    killMacGc();// we do not want anything deleted

    log.info("Testing delete upgrades for {}", level.metaTable());
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {

      Map<String,String> expected = addEntries(c, level.metaTable(), count, "somefile");
      Map<String,String> actual = new HashMap<>();

      sleepUninterruptibly(1, TimeUnit.SECONDS);
      upgrader.upgradeFileDeletes(getServerContext(), level);
      sleepUninterruptibly(1, TimeUnit.SECONDS);
      Range range = MetadataSchema.DeletesSection.getRange();

      Scanner scanner;
      try {
        scanner = c.createScanner(level.metaTable(), Authorizations.EMPTY);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
      scanner.setRange(range);
      scanner.iterator().forEachRemaining(entry -> {
        actual.put(entry.getKey().getRow().toString(), entry.getValue().toString());
      });

      assertEquals(expected, actual);

      // ENSURE IDEMPOTENCE - run upgrade again to ensure nothing is changed because there is
      // nothing to change
      upgrader.upgradeFileDeletes(getServerContext(), level);
      try {
        scanner = c.createScanner(level.metaTable(), Authorizations.EMPTY);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
      scanner.setRange(range);
      actual.clear();
      scanner.iterator().forEachRemaining(entry -> {
        actual.put(entry.getKey().getRow().toString(), entry.getValue().toString());
      });
      assertEquals(expected, actual);
    }
  }

  private Mutation createOldDelMutation(String path, String cf, String cq, String val) {
    Text row = new Text(OLDDELPREFIX + path);
    Mutation delFlag = new Mutation(row);
    delFlag.put(cf, cq, val);
    return delFlag;
  }

  private Map<String,String> addEntries(AccumuloClient client, String table, int count,
      String filename) throws Exception {
    client.securityOperations().grantTablePermission(client.whoami(), table, TablePermission.WRITE);
    Map<String,String> expected = new TreeMap<>();
    try (BatchWriter bw = client.createBatchWriter(table)) {
      for (int i = 0; i < count; ++i) {
        String longpath = String.format("hdfs://localhost:8020/%020d/%s", i, filename);
        Mutation delFlag = createOldDelMutation(longpath, "", "", "");
        bw.addMutation(delFlag);
        expected.put(MetadataSchema.DeletesSection.encodeRow(longpath),
            Upgrader9to10.UPGRADED.toString());
      }
      return expected;
    }
  }

}
