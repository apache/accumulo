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

import static org.apache.accumulo.test.util.FileMetadataUtil.countFencedFiles;
import static org.apache.accumulo.test.util.FileMetadataUtil.verifyMergedMarkerCleared;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaSplitIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(MetaSplitIT.class);

  private Collection<Text> metadataSplits = null;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(3);
  }

  @BeforeEach
  public void saveMetadataSplits() throws Exception {
    if (getClusterType() == ClusterType.STANDALONE) {
      try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
        Collection<Text> splits = client.tableOperations().listSplits(MetadataTable.NAME);
        // We expect a single split
        if (!splits.equals(Arrays.asList(new Text("~")))) {
          log.info("Existing splits on metadata table. Saving them, and applying"
              + " single original split of '~'");
          metadataSplits = splits;
          client.tableOperations().merge(MetadataTable.NAME, null, null);
          client.tableOperations().addSplits(MetadataTable.NAME,
              new TreeSet<>(Collections.singleton(new Text("~"))));
        }
      }
    }
  }

  @AfterEach
  public void restoreMetadataSplits() throws Exception {
    if (metadataSplits != null) {
      log.info("Restoring split on metadata table");
      try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
        client.tableOperations().merge(MetadataTable.NAME, null, null);
        client.tableOperations().addSplits(MetadataTable.NAME, new TreeSet<>(metadataSplits));
      }
    }
  }

  @Test
  public void testRootTableSplit() {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("5"));
      assertThrows(AccumuloException.class,
          () -> client.tableOperations().addSplits(RootTable.NAME, splits));
    }
  }

  @Test
  public void testRootTableMerge() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().merge(RootTable.NAME, null, null);
    }
  }

  private void addSplits(TableOperations opts, String... points) throws Exception {
    SortedSet<Text> splits = new TreeSet<>();
    for (String point : points) {
      splits.add(new Text(point));
    }
    opts.addSplits(MetadataTable.NAME, splits);
  }

  @Test
  public void testMetadataTableSplit() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      // disable compactions
      client.tableOperations().setProperty(MetadataTable.NAME, Property.TABLE_MAJC_RATIO.getKey(),
          "9999");

      TableOperations opts = client.tableOperations();
      for (int i = 1; i <= 10; i++) {
        opts.create(Integer.toString(i));
      }
      try {
        assertEquals(0, countFencedFiles(getServerContext(), MetadataTable.NAME));
        verifyMetadataTableScan(client);
        opts.merge(MetadataTable.NAME, new Text("01"), new Text("02"));
        checkMetadataSplits(1, opts);
        verifyMetadataTableScan(client);
        addSplits(opts, "4 5 6 7 8".split(" "));
        checkMetadataSplits(6, opts);
        verifyMetadataTableScan(client);

        opts.merge(MetadataTable.NAME, new Text("6"), new Text("9"));
        checkMetadataSplits(4, opts);
        // Merging tablets should produce fenced files because of no-chop merge
        assertTrue(countFencedFiles(getServerContext(), MetadataTable.NAME) > 0);
        verifyMetadataTableScan(client);
        // Verify that the MERGED marker was cleared and doesn't exist on any tablet
        verifyMergedMarkerCleared(getServerContext(), MetadataTable.ID);

        addSplits(opts, "44 55 66 77 88".split(" "));
        checkMetadataSplits(9, opts);
        assertTrue(countFencedFiles(getServerContext(), MetadataTable.NAME) > 0);
        verifyMetadataTableScan(client);
        // Verify that the MERGED marker was cleared and doesn't exist on any tablet
        verifyMergedMarkerCleared(getServerContext(), MetadataTable.ID);

        opts.merge(MetadataTable.NAME, new Text("5"), new Text("7"));
        checkMetadataSplits(6, opts);
        assertTrue(countFencedFiles(getServerContext(), MetadataTable.NAME) > 0);
        verifyMetadataTableScan(client);
        // Verify that the MERGED marker was cleared and doesn't exist on any tablet
        verifyMergedMarkerCleared(getServerContext(), MetadataTable.ID);

        opts.merge(MetadataTable.NAME, null, null);
        checkMetadataSplits(0, opts);
        assertTrue(countFencedFiles(getServerContext(), MetadataTable.NAME) > 0);
        verifyMetadataTableScan(client);
        // Verify that the MERGED marker was cleared and doesn't exist on any tablet
        verifyMergedMarkerCleared(getServerContext(), MetadataTable.ID);

        opts.compact(MetadataTable.NAME, new CompactionConfig());
        // Should be no more fenced files after compaction
        assertEquals(0, countFencedFiles(getServerContext(), MetadataTable.NAME));
        verifyMetadataTableScan(client);
      } finally {
        for (int i = 1; i <= 10; i++) {
          opts.delete(Integer.toString(i));
        }
      }
    }
  }

  // Count the number of entries that can be read in the Metadata table
  // This verifies all the entries can still be read after splits/merges
  // when ranged files are used
  private void verifyMetadataTableScan(AccumuloClient client) throws Exception {
    var tables = client.tableOperations().tableIdMap();
    var expectedExtents = tables.entrySet().stream()
        .filter(e -> !e.getKey().equals(RootTable.NAME) && !e.getKey().equals(MetadataTable.NAME))
        .map(Map.Entry::getValue).map(TableId::of).map(tid -> new KeyExtent(tid, null, null))
        .collect(Collectors.toSet());
    // Verify we have 11 tablets for metadata (Includes FateTable)
    assertEquals(11, expectedExtents.size());

    // Scan each tablet to verify data exists
    var ample = ((ClientContext) client).getAmple();
    try (var tablets = ample.readTablets().forLevel(Ample.DataLevel.USER).build()) {
      for (var tablet : tablets) {
        assertTrue(expectedExtents.remove(tablet.getExtent()));
        // check a few fields that should always be present in tablet metadata
        assertNotNull(tablet.getDirName());
        assertNotNull(tablet.getTime());
      }
    }

    // ensure all expected extents were seen
    assertEquals(0, expectedExtents.size());
  }

  private static void checkMetadataSplits(int numSplits, TableOperations opts)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException,
      InterruptedException {
    for (int i = 0; i < 10; i++) {
      if (opts.listSplits(MetadataTable.NAME).size() == numSplits) {
        break;
      }
      Thread.sleep(2000);
    }
    Collection<Text> splits = opts.listSplits(MetadataTable.NAME);
    assertEquals(numSplits, splits.size(), "Actual metadata table splits: " + splits);
  }

}
