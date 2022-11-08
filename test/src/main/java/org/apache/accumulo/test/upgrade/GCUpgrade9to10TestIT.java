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

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.DeletesSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.manager.upgrade.Upgrader9to10;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessNotFoundException;
import org.apache.accumulo.server.gc.AllVolumesDirectory;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

public class GCUpgrade9to10TestIT extends ConfigurableMacBase {
  private static final String OUR_SECRET = "itsreallysecret";
  private static final String OLDDELPREFIX = "~del";
  private static final Upgrader9to10 upgrader = new Upgrader9to10();

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.INSTANCE_SECRET, OUR_SECRET);
    cfg.setProperty(Property.GC_CYCLE_START, "1000"); // gc will be killed before it is run

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  private void killMacGc() throws ProcessNotFoundException, InterruptedException, KeeperException {
    // kill gc started by MAC
    getCluster().killProcess(ServerType.GARBAGE_COLLECTOR,
        getCluster().getProcesses().get(ServerType.GARBAGE_COLLECTOR).iterator().next());
    // delete lock in zookeeper if there, this will allow next GC to start quickly
    var path = ServiceLock.path(getServerContext().getZooKeeperRoot() + Constants.ZGC_LOCK);
    ZooReaderWriter zk = getServerContext().getZooReaderWriter();
    try {
      ServiceLock.deleteLock(zk, path);
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
   * Ensure that the size of the candidates exceeds the {@link Upgrader9to10}'s CANDIDATE_BATCH_SIZE
   * and will clean up candidates in multiple batches, without running out of memory.
   */
  @Test
  public void gcUpgradeOutofMemoryTest() throws Exception {
    killMacGc(); // we do not want anything deleted

    int numberOfEntries = 100_000;
    String longpathname = StringUtils.repeat("abcde", 100);
    assertEquals(500, longpathname.length());

    // sanity check to ensure that any batch size assumptions are still valid in this test
    assertEquals(4_000_000, Upgrader9to10.CANDIDATE_BATCH_SIZE);

    // ensure test quality by making sure we have enough candidates to
    // exceed the batch size at least ten times
    long numBatches = numberOfEntries * longpathname.length() / Upgrader9to10.CANDIDATE_BATCH_SIZE;
    assertTrue(numBatches > 10 && numBatches < 15,
        "Expected numBatches between 10 and 15, but was " + numBatches);

    Ample.DataLevel level = Ample.DataLevel.USER;

    log.info("Filling metadata table with lots of bogus delete flags");
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      Map<String,String> expected = addEntries(c, level.metaTable(), numberOfEntries, longpathname);
      assertEquals(numberOfEntries + numberOfEntries / 10, expected.size());

      Range range = DeletesSection.getRange();

      sleepUninterruptibly(1, TimeUnit.SECONDS);
      try (Scanner scanner = c.createScanner(level.metaTable(), Authorizations.EMPTY)) {
        Map<String,String> actualOldStyle = new HashMap<>();
        scanner.setRange(range);
        scanner.forEach(entry -> {
          String strKey = entry.getKey().getRow().toString();
          String strValue = entry.getValue().toString();
          actualOldStyle.put(strKey, strValue);
        });
        assertEquals(expected.size(), actualOldStyle.size());
        assertTrue(Collections.disjoint(expected.keySet(), actualOldStyle.keySet()));
      }

      upgrader.upgradeFileDeletes(getServerContext(), level);

      sleepUninterruptibly(1, TimeUnit.SECONDS);
      try (Scanner scanner = c.createScanner(level.metaTable(), Authorizations.EMPTY)) {
        Map<String,String> actualNewStyle = new HashMap<>();
        scanner.setRange(range);
        scanner.forEach(entry -> {
          String strKey = entry.getKey().getRow().toString();
          String expectedValue = expected.get(strKey);
          assertNotNull(expectedValue);
          String strValue = entry.getValue().toString();
          assertEquals(expectedValue, strValue);
          actualNewStyle.put(strKey, strValue);
        });
        assertEquals(expected.size(), actualNewStyle.size());
        assertEquals(expected, actualNewStyle);
      }
    }
  }

  private void gcUpgradeDeletesTest(Ample.DataLevel level, int count) throws Exception {
    killMacGc();// we do not want anything deleted

    log.info("Testing delete upgrades for {}", level.metaTable());
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {

      Map<String,String> expected = addEntries(c, level.metaTable(), count, "somefile");

      sleepUninterruptibly(1, TimeUnit.SECONDS);
      upgrader.upgradeFileDeletes(getServerContext(), level);
      sleepUninterruptibly(1, TimeUnit.SECONDS);
      Range range = DeletesSection.getRange();

      try (Scanner scanner = c.createScanner(level.metaTable(), Authorizations.EMPTY)) {
        Map<String,String> actual = new HashMap<>();
        scanner.setRange(range);
        scanner.forEach(entry -> {
          actual.put(entry.getKey().getRow().toString(), entry.getValue().toString());
        });
        assertEquals(expected, actual);
      }

      // ENSURE IDEMPOTENCE - run upgrade again to ensure nothing is changed because there is
      // nothing to change
      upgrader.upgradeFileDeletes(getServerContext(), level);
      try (Scanner scanner = c.createScanner(level.metaTable(), Authorizations.EMPTY)) {
        Map<String,String> actual = new HashMap<>();
        scanner.setRange(range);
        scanner.forEach(entry -> {
          actual.put(entry.getKey().getRow().toString(), entry.getValue().toString());
        });
        assertEquals(expected, actual);
      }
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
        String longpath =
            String.format("hdfs://localhost:8020/accumulo/tables/5a/t-%08x/%s", i, filename);
        Mutation delFlag = createOldDelMutation(longpath, "", "", "");
        bw.addMutation(delFlag);
        expected.put(DeletesSection.encodeRow(longpath), Upgrader9to10.UPGRADED.toString());
      }

      // create directory delete entries

      TableId tableId = TableId.of("5a");

      for (int i = 0; i < count; i += 10) {
        String dirName = String.format("t-%08x", i);
        String longpath =
            String.format("hdfs://localhost:8020/accumulo/tables/%s/%s", tableId, dirName);
        Mutation delFlag = createOldDelMutation(longpath, "", "", "");
        bw.addMutation(delFlag);
        expected.put(
            DeletesSection.encodeRow(new AllVolumesDirectory(tableId, dirName).getMetadataEntry()),
            Upgrader9to10.UPGRADED.toString());
      }

      return expected;
    }
  }

}
