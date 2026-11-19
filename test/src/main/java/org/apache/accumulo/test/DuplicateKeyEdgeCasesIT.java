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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.OfflineScanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Edge-case regression tests for duplicate-key scans. These are expected to fail until the noted
 * issues are addressed.
 */
public class DuplicateKeyEdgeCasesIT extends AccumuloClusterHarness {

  private AccumuloClient client;
  private String tableName;
  SecureRandom random = new SecureRandom();

  @BeforeEach
  public void setupInstance() {
    client = Accumulo.newClient().from(getClientProps()).build();
    tableName = getUniqueNames(1)[0];
  }

  @AfterEach
  public void teardown() {
    try {
      client.tableOperations().delete(tableName);
    } catch (Exception e) {
      // ignore
    }
    client.close();
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
    cfg.setNumTservers(2);
  }

  /**
   * Test that identical keys with different values are not dropped.
   */
  @Test
  public void testDuplicateScanLosesKeys() throws Exception {
    final int numRows = 100;
    final int mutationsPerRow = 10;
    final int expectedEntries = numRows * mutationsPerRow;
    byte[] randomValue = new byte[8192];

    client.tableOperations().create(tableName);

    client.tableOperations().modifyProperties(tableName, properties -> {
      properties.remove("table.iterator.scan.vers");
      properties.remove("table.iterator.minc.vers");
      properties.remove("table.iterator.majc.vers");
      properties.put(Property.TABLE_SCAN_MAXMEM.getKey(), "32k");
      properties.put(Property.TABLE_SCAN_BATCH_DUPLICATE_MAX_MULTIPLIER.getKey(), "20");
    });

    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      for (int i = 0; i < numRows; i++) {
        for (int j = 0; j < mutationsPerRow; j++) {
          Mutation m = new Mutation("row" + i);
          random.nextBytes(randomValue);
          m.put("cf" + i, "cq" + i, 100L, new Value(randomValue));
          bw.addMutation(m);
        }
      }
    }
    client.tableOperations().flush(tableName, null, null, true);
    client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

    List<String> offlineValues = getOfflineValues();

    long onlineCount;
    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
      onlineCount = scanner.stream().count();
    }

    assertEquals(expectedEntries, offlineValues.size());
    assertEquals(offlineValues.size(), onlineCount,
        "Online scan lost keys compared to direct RFile scan");
  }

  /**
   * Test that the {@link Property#TABLE_SCAN_BATCH_DUPLICATE_MAX_MULTIPLIER} property correctly
   * allows the scan to grow or otherwise fail
   */
  @Test
  public void duplicateRunExceedsBatchGrowthFailsScan() throws Exception {
    final int totalWrites = 12;

    client.tableOperations().create(tableName);
    client.tableOperations().modifyProperties(tableName, properties -> {
      properties.remove("table.iterator.scan.vers");
      properties.remove("table.iterator.minc.vers");
      properties.remove("table.iterator.majc.vers");
      properties.put(Property.TABLE_SCAN_MAXMEM.getKey(), "32k");

      // set multiplier to 1 so we can observe the scan fail due to too many duplicates
      properties.put(Property.TABLE_SCAN_BATCH_DUPLICATE_MAX_MULTIPLIER.getKey(), "1");
    });

    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      for (int i = 0; i < totalWrites; i++) {
        Mutation m = new Mutation("row");
        m.put("cf", "cq", 100L, new Value(("v-" + i).getBytes(UTF_8)));
        bw.addMutation(m);
      }
    }
    client.tableOperations().flush(tableName, null, null, true);
    client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

    assertThrows(RuntimeException.class, () -> {
      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        scanner.setBatchSize(5);
        scanner.stream().count();
      }
    }, "Expected the scan to fail due exceeding the max size");

    // increase the batch size multiplier prop then retry the scan to make sure it passes this time
    client.tableOperations().setProperty(tableName,
        Property.TABLE_SCAN_BATCH_DUPLICATE_MAX_MULTIPLIER.getKey(), "5");

    long count;
    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setBatchSize(5);
      count = scanner.stream().count();
    }
    assertEquals(totalWrites, count);
  }

  /**
   * Test that multiple entries with the same key but different values is properly read when the
   * underlying files change midway through a scan
   */
  @Test
  public void duplicateOrderChangesAcrossFilesDropsValues() throws Exception {
    final int totalWrites = 150;
    final int writesPerFlush = 50;

    client.tableOperations().create(tableName);
    client.tableOperations().modifyProperties(tableName, properties -> {
      properties.remove("table.iterator.scan.vers");
      properties.remove("table.iterator.minc.vers");
      properties.remove("table.iterator.majc.vers");
      properties.put(Property.TABLE_SCAN_MAXMEM.getKey(), "32k");
      properties.put(Property.TABLE_SCAN_BATCH_DUPLICATE_MAX_MULTIPLIER.getKey(), "20");
    });

    // Write duplicates in three flushes to get multiple files
    int written = 0;
    int flushIndex = 0;
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      while (written < totalWrites) {
        for (int i = 0; i < writesPerFlush && written < totalWrites; i++) {
          String value = "v-" + flushIndex + "-" + written;
          Mutation m = new Mutation("row");
          m.put("cf", "cq", 100L, new Value(value.getBytes(UTF_8)));
          bw.addMutation(m);
          written++;
        }
        bw.flush();
        client.tableOperations().flush(tableName, null, null, true);
        flushIndex++;
      }
    }
    assertEquals(totalWrites, written);
    client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

    List<String> offlineValues = getOfflineValues();

    List<String> seen = new ArrayList<>();
    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setRange(new Range());
      scanner.setBatchSize(8);

      var iter = scanner.iterator();
      for (int i = 0; i < 16 && iter.hasNext(); i++) {
        seen.add(new String(iter.next().getValue().get(), UTF_8));
      }

      // Rewrite files to change the source ordering.
      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

      iter.forEachRemaining(e -> seen.add(new String(e.getValue().get(), UTF_8)));
    }

    assertEquals(offlineValues.size(), seen.size());
    Collections.sort(offlineValues);
    Collections.sort(seen);
    assertEquals(offlineValues, seen);
  }

  /**
   * Test that if a tserver dies part way through a scan on a table with identical keys, nothing is
   * lost when the scan completes after a tserver comes back uo.
   */
  @Test
  public void duplicateSkipStateLostOnTserverFailover() throws Exception {
    final int totalWrites = 90;
    final int writesPerFlush = 30;

    client.tableOperations().create(tableName);
    client.tableOperations().modifyProperties(tableName, properties -> {
      properties.remove("table.iterator.scan.vers");
      properties.remove("table.iterator.minc.vers");
      properties.remove("table.iterator.majc.vers");
      properties.put(Property.TABLE_SCAN_MAXMEM.getKey(), "32k");
      properties.put(Property.TABLE_SCAN_BATCH_DUPLICATE_MAX_MULTIPLIER.getKey(), "20");
    });

    int written = 0;
    int flushIndex = 0;
    while (written < totalWrites) {
      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        for (int i = 0; i < writesPerFlush && written < totalWrites; i++) {
          String value = "v-" + flushIndex + "-" + written;
          Mutation m = new Mutation("row");
          m.put("cf", "cq", 100L, new Value(value.getBytes(UTF_8)));
          bw.addMutation(m);
          written++;
        }
      }
      flushIndex++;
    }
    assertEquals(totalWrites, written);
    client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

    List<String> offlineValues = getOfflineValues();

    List<String> seen = new ArrayList<>();
    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
      scanner.setRange(new Range());
      scanner.setBatchSize(8);

      var iter = scanner.iterator();
      for (int i = 0; i < 12 && iter.hasNext(); i++) {
        seen.add(new String(iter.next().getValue().get(), UTF_8));
      }

      getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
      getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);
      Wait.waitFor(() -> {
        System.out.println("waiting for tservers");
        return client.instanceOperations().getTabletServers().size() == 2;
      }, 60_000);

      iter.forEachRemaining(e -> seen.add(new String(e.getValue().get(), UTF_8)));
    }

    assertEquals(offlineValues.size(), seen.size());

    Collections.sort(offlineValues);
    Collections.sort(seen);
    assertEquals(offlineValues, seen);
  }

  /**
   * Offline the test table, read all values with an OfflineScanner, then bring it back online and
   * return the values.
   */
  private List<String> getOfflineValues()
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    ClientContext context = (ClientContext) client;
    client.tableOperations().offline(tableName, true);
    List<String> offlineValues;
    try (OfflineScanner offlineScanner =
        new OfflineScanner(context, context.getTableId(tableName), Authorizations.EMPTY)) {
      offlineValues = offlineScanner.stream().map(e -> new String(e.getValue().get(), UTF_8))
          .collect(Collectors.toCollection(ArrayList::new));
    }
    client.tableOperations().online(tableName, true);
    return offlineValues;
  }
}
