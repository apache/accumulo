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

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.test.ScanServerIT.createTableAndIngest;
import static org.apache.accumulo.test.ScanServerIT.ingest;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;

@Tag(MINI_CLUSTER_ONLY)
public class ScanServerConcurrentTabletScanIT extends SharedMiniClusterBase {

  private static class ScanServerConcurrentTabletScanITConfiguration
      implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      cfg.getClusterServerConfiguration().setNumDefaultScanServers(1);
      cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "3s");
      cfg.setProperty(Property.SSERV_MINTHREADS, "4");
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    ScanServerConcurrentTabletScanITConfiguration c =
        new ScanServerConcurrentTabletScanITConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);
  }

  @AfterAll
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private void startScanServer(String cacheExpiration, String cacheRefresh)
      throws IOException, KeeperException, InterruptedException {

    SharedMiniClusterBase.getCluster().getClusterControl().stop(ServerType.SCAN_SERVER);

    Map<String,String> overrides = new HashMap<>();
    overrides.put(Property.SSERV_CACHED_TABLET_METADATA_EXPIRATION.getKey(), cacheExpiration);
    overrides.put(Property.SSERV_CACHED_TABLET_METADATA_REFRESH_PERCENT.getKey(), cacheRefresh);
    SharedMiniClusterBase.getCluster().getClusterControl().start(ServerType.SCAN_SERVER, overrides,
        1, null);

    Wait.waitFor(() -> !getCluster().getServerContext().getServerPaths()
        .getScanServer(Optional.empty(), Optional.empty(), true).isEmpty());

  }

  @Test
  public void testScanSameTabletDifferentDataTmCacheEnabledRefreshNotTriggered() throws Exception {
    // Set the cache time to 10 minutes so it won't expire
    // Set the cache refresh to 50%, which is 5 minutes so a refresh won't be triggered
    startScanServer("10m", ".5");
    testScanSameTabletDifferentDataTabletMetadataCacheEnabled(false);
  }

  @Test
  public void testScanSameTabletDifferentDataTmCacheEnabledRefreshTriggered() throws Exception {
    // Set the cache time to 10 minutes so it won't expire
    // Set the cache refresh to 6ms, so a second hit after 6ms will trigger a background refresh
    // .00001 * 10m (600000ms) = 6 ms
    startScanServer("10m", ".00001");
    testScanSameTabletDifferentDataTabletMetadataCacheEnabled(true);
  }

  @Test
  public void testScanSameTabletDifferentDataTmCacheEnabledRefreshDisabled() throws Exception {
    // Set the cache time to 10 minutes so it won't expire and disable the refresh entirely
    startScanServer("5m", "0");
    testScanSameTabletDifferentDataTabletMetadataCacheEnabled(false);
  }

  private void testScanSameTabletDifferentDataTabletMetadataCacheEnabled(boolean shouldRefresh)
      throws Exception {

    Properties clientProperties = getClientProps();
    clientProperties.put(ClientProperty.SCANNER_BATCH_SIZE.getKey(), "100");

    try (AccumuloClient client = Accumulo.newClient().from(clientProperties).build()) {
      String tableName = getUniqueNames(1)[0];

      // Create table and ingest 1000 k/v
      final int firstBatchOfEntriesCount =
          createTableAndIngest(client, tableName, null, 10, 100, "COLA");

      Scanner scanner1 = client.createScanner(tableName, Authorizations.EMPTY);
      scanner1.setRange(new Range());
      scanner1.setBatchSize(100);
      scanner1.setReadaheadThreshold(0);
      scanner1.setConsistencyLevel(ConsistencyLevel.EVENTUAL);

      // iter1 should read 1000 k/v
      Iterator<Entry<Key,Value>> iter1 = scanner1.iterator();

      // Partially read the data and then start a 2nd scan
      int count1 = 0;
      while (iter1.hasNext() && count1 < 10) {
        iter1.next();
        count1++;
      }

      // Ingest another 100 k/v with a different column family
      final int secondBatchOfEntriesCount = ingest(client, tableName, 10, 10, 0, "COLB", true);

      // Add a sleep that is long enough that the configured refresh interval passes if
      // the test has been set to use one.
      Thread.sleep(1000);

      // iter2 should read 1000 k/v because the tablet metadata is cached.
      // This call will trigger a cache refresh for the entry in the background if refresh is
      // enabled.
      // This current iter2 scan will still return the old data (1000 k/v) as the new value
      // won't be visible until the reload finishes
      Iterator<Entry<Key,Value>> iter2 = scanner1.iterator();
      int count2 = 0;
      boolean useIter1 = true;

      do {
        if (useIter1) {
          if (iter1.hasNext()) {
            iter1.next();
            count1++;
          }
        } else {
          if (iter2.hasNext()) {
            iter2.next();
            count2++;
          }
        }
        useIter1 = !useIter1;
      } while (iter1.hasNext() || iter2.hasNext());
      assertEquals(firstBatchOfEntriesCount, count1);
      assertEquals(firstBatchOfEntriesCount, count2);

      // If a refresh was done this should see 1100 entries
      // Keep scanning until updated value is seen
      if (shouldRefresh) {
        // Count the number of entries for the third iterator to test if
        // refresh worked depending on configs
        Wait.waitFor(
            () -> countEntries(scanner1) == firstBatchOfEntriesCount + secondBatchOfEntriesCount,
            10000, 500);
      } else {
        // There's not a great way to test the case of things not refreshing as the value
        // should just be the same, so sleep for a period of time that should be longer than
        // the refresh
        Thread.sleep(1000);
        assertEquals(firstBatchOfEntriesCount, countEntries(scanner1));
      }

      scanner1.close();

      // A new scan should read all 1100 entries
      try (Scanner scanner2 = client.createScanner(tableName, Authorizations.EMPTY)) {
        int totalEntriesExpected = firstBatchOfEntriesCount + secondBatchOfEntriesCount;
        assertEquals(totalEntriesExpected, Iterables.size(scanner2));
      }
    }
  }

  private int countEntries(Scanner scanner) {
    int count = 0;
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    return count;
  }

  @Test
  public void testScanSameTabletDifferentDataTabletMetadataCacheDisabled() throws Exception {

    startScanServer("0m", "0");

    Properties clientProperties = getClientProps();
    clientProperties.put(ClientProperty.SCANNER_BATCH_SIZE.getKey(), "100");

    try (AccumuloClient client = Accumulo.newClient().from(clientProperties).build()) {
      String tableName = getUniqueNames(1)[0];

      // Create table and ingest 1000 k/v
      final int firstBatchOfEntriesCount =
          createTableAndIngest(client, tableName, null, 10, 100, "COLA");

      try (Scanner scanner1 = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner1.setRange(new Range());
        scanner1.setBatchSize(100);
        scanner1.setReadaheadThreshold(0);
        scanner1.setConsistencyLevel(ConsistencyLevel.EVENTUAL);

        // iter1 should read 1000 k/v
        Iterator<Entry<Key,Value>> iter1 = scanner1.iterator();

        // Partially read the data and then start a 2nd scan
        int count1 = 0;
        while (iter1.hasNext() && count1 < 10) {
          iter1.next();
          count1++;
        }

        // Ingest another 100 k/v with a different column family
        final int secondBatchOfEntriesCount = ingest(client, tableName, 10, 10, 0, "COLB", true);

        // iter2 should read 1100 k/v because the tablet metadata is not cached.
        Iterator<Entry<Key,Value>> iter2 = scanner1.iterator();
        int count2 = 0;
        boolean useIter1 = true;

        do {
          if (useIter1) {
            if (iter1.hasNext()) {
              iter1.next();
              count1++;
            }
          } else {
            if (iter2.hasNext()) {
              iter2.next();
              count2++;
            }
          }
          useIter1 = !useIter1;
        } while (iter1.hasNext() || iter2.hasNext());
        assertEquals(firstBatchOfEntriesCount, count1);
        assertEquals(firstBatchOfEntriesCount + secondBatchOfEntriesCount, count2);

      }
    }
  }
}
