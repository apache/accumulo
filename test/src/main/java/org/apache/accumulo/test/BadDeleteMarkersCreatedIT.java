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
package org.apache.accumulo.test;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.DeletesSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Accumulo3047
public class BadDeleteMarkersCreatedIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(BadDeleteMarkersCreatedIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setProperty(Property.GC_CYCLE_START, "0s");
  }

  private int timeoutFactor = 1;

  @BeforeEach
  public void getTimeoutFactor() {
    try {
      timeoutFactor = Integer.parseInt(System.getProperty("timeout.factor"));
    } catch (NumberFormatException e) {
      log.warn("Could not parse integer from timeout.factor");
    }

    assertTrue(timeoutFactor >= 1, "timeout.factor must be greater than or equal to 1");
  }

  private String gcCycleDelay, gcCycleStart;

  @BeforeEach
  public void alterConfig() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      InstanceOperations iops = client.instanceOperations();
      Map<String,String> config = iops.getSystemConfiguration();
      gcCycleDelay = config.get(Property.GC_CYCLE_DELAY.getKey());
      gcCycleStart = config.get(Property.GC_CYCLE_START.getKey());
      iops.setProperty(Property.GC_CYCLE_DELAY.getKey(), "1s");
      iops.setProperty(Property.GC_CYCLE_START.getKey(), "0s");
      log.info("Restarting garbage collector");
    }
    getCluster().getClusterControl().stopAllServers(ServerType.GARBAGE_COLLECTOR);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build();
        ClientContext context = (ClientContext) client) {
      ZooCache zcache = context.getZooCache();
      zcache.clear();
      var path = ServiceLock
          .path(ZooUtil.getRoot(client.instanceOperations().getInstanceId()) + Constants.ZGC_LOCK);
      byte[] gcLockData;
      do {
        gcLockData = ServiceLock.getLockData(zcache, path, null);
        if (gcLockData != null) {
          log.info("Waiting for GC ZooKeeper lock to expire");
          Thread.sleep(2000);
        }
      } while (gcLockData != null);

      log.info("GC lock was lost");

      getCluster().getClusterControl().startAllServers(ServerType.GARBAGE_COLLECTOR);
      log.info("Garbage collector was restarted");

      do {
        gcLockData = ServiceLock.getLockData(zcache, path, null);
        if (gcLockData == null) {
          log.info("Waiting for GC ZooKeeper lock to be acquired");
          Thread.sleep(2000);
        }
      } while (gcLockData == null);

      log.info("GC lock was acquired");
    }
  }

  @AfterEach
  public void restoreConfig() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      InstanceOperations iops = c.instanceOperations();
      if (gcCycleDelay != null) {
        iops.setProperty(Property.GC_CYCLE_DELAY.getKey(), gcCycleDelay);
      }
      if (gcCycleStart != null) {
        iops.setProperty(Property.GC_CYCLE_START.getKey(), gcCycleStart);
      }
      log.info("Restarting garbage collector");
      getCluster().getClusterControl().stopAllServers(ServerType.GARBAGE_COLLECTOR);
      getCluster().getClusterControl().startAllServers(ServerType.GARBAGE_COLLECTOR);
      log.info("Garbage collector was restarted");
    }
  }

  @Test
  public void test() throws Exception {
    // make a table
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      log.info("Creating table to be deleted");
      c.tableOperations().create(tableName);
      final String tableId = c.tableOperations().tableIdMap().get(tableName);
      assertNotNull(tableId, "Expected to find a tableId");

      // add some splits
      SortedSet<Text> splits = new TreeSet<>();
      for (int i = 0; i < 10; i++) {
        splits.add(new Text("" + i));
      }
      c.tableOperations().addSplits(tableName, splits);
      // get rid of all the splits
      c.tableOperations().deleteRows(tableName, null, null);
      // get rid of the table
      c.tableOperations().delete(tableName);
      log.info("Sleeping to let garbage collector run");
      // let gc run
      sleepUninterruptibly(timeoutFactor * 15, TimeUnit.SECONDS);
      log.info("Verifying that delete markers were deleted");
      // look for delete markers
      try (Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
        scanner.setRange(DeletesSection.getRange());
        for (Entry<Key,Value> entry : scanner) {
          String row = entry.getKey().getRow().toString();
          if (!row.contains("/" + tableId + "/")) {
            log.info("Ignoring delete entry for a table other than the one we deleted");
            continue;
          }
          fail("Delete entry should have been deleted by the garbage collector: "
              + entry.getKey().getRow());
        }
      }
    }
  }
}
