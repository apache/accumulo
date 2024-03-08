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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.split.SplitUtils;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LargeSplitRowIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(LargeSplitRowIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
  }

  // User added split
  @Test
  public void userAddedSplit() throws Exception {

    log.info("User added split");

    // make a table and lower the TABLE_END_ROW_MAX_SIZE property
    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      Map<String,String> props = Map.of(Property.TABLE_MAX_END_ROW_SIZE.getKey(), "1000");
      client.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

      // Create a BatchWriter and add a mutation to the table
      try (BatchWriter batchWriter = client.createBatchWriter(tableName)) {
        Mutation m = new Mutation("Row");
        m.put("cf", "cq", "value");
        batchWriter.addMutation(m);
      }

      // Create a split point that is too large to be an end row and fill it with all 'm'
      SortedSet<Text> partitionKeys = new TreeSet<>();
      byte[] data = new byte[(int) (ConfigurationTypeHelper
          .getFixedMemoryAsBytes(Property.TABLE_MAX_END_ROW_SIZE.getDefaultValue()) + 2)];
      Arrays.fill(data, (byte) 'm');
      partitionKeys.add(new Text(data));

      // try to add the split point that is too large, if the split point is created the test fails.
      assertThrows(AccumuloException.class,
          () -> client.tableOperations().addSplits(tableName, partitionKeys));

      // Make sure that the information that was written to the table before we tried to add the
      // split point is still correct
      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        // Make sure there is only one line in the table and get that entry
        Entry<Key,Value> entry = getOnlyElement(scanner);

        Key k = entry.getKey();
        assertEquals("Row", k.getRow().toString());
        assertEquals("cf", k.getColumnFamily().toString());
        assertEquals("cq", k.getColumnQualifier().toString());
        assertEquals("value", entry.getValue().toString());

      }
    }
  }

  // Test tablet server split with 250 entries with all the same prefix
  @Test
  @Timeout(60)
  public void automaticSplitWith250Same() throws Exception {
    log.info("Automatic with 250 with same prefix");

    // make a table and lower the configuration properties
    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // @formatter:off
      Map<String,String> props = Map.of(
        Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K",
        Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none",
        Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "64",
        Property.TABLE_MAX_END_ROW_SIZE.getKey(), "1000",
        Property.TABLE_MAJC_RATIO.getKey(), "9999"
      );
      // @formatter:on
      client.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

      // Create a key for a table entry that is longer than the allowed size for an
      // end row
      byte[] data = new byte[(int) (ConfigurationTypeHelper
          .getFixedMemoryAsBytes(Property.TABLE_MAX_END_ROW_SIZE.getDefaultValue()) + 2)];

      // Fill this key with all m's except the last spot
      Arrays.fill(data, 0, data.length - 2, (byte) 'm');

      final int numOfMutations = 250;
      try (BatchWriter batchWriter = client.createBatchWriter(tableName)) {
        // Make the last place in the key different for every entry added to the table
        for (int i = 0; i < numOfMutations; i++) {
          data[data.length - 1] = (byte) i;
          Mutation m = new Mutation(data);
          m.put("cf", "cq", "value");
          batchWriter.addMutation(m);
        }
      }
      // Flush the BatchWriter and table and sleep for a bit to make sure that there is enough time
      // for the table to split if need be.
      client.tableOperations().flush(tableName, new Text(), new Text("z"), true);

      // Wait for the tablet to be marked as unsplittable due to the system split running
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      Wait.waitFor(() -> getServerContext().getAmple()
          .readTablet(new KeyExtent(tableId, null, null)).getUnSplittable() != null,
          Wait.MAX_WAIT_MILLIS, 100);

      // Verify that the unsplittable column is read correctly
      TabletMetadata tm =
          getServerContext().getAmple().readTablet(new KeyExtent(tableId, null, null));
      assertEquals(tm.getUnSplittable(), SplitUtils.toUnSplittable(getServerContext(), tm));

      // Make sure all the data that was put in the table is still correct
      int count = 0;
      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        for (Entry<Key,Value> entry : scanner) {
          Key k = entry.getKey();
          data[data.length - 1] = (byte) count;
          String expected = new String(data, UTF_8);
          assertEquals(expected, k.getRow().toString());
          assertEquals("cf", k.getColumnFamily().toString());
          assertEquals("cq", k.getColumnQualifier().toString());
          assertEquals("value", entry.getValue().toString());
          count++;
        }
      }
      assertEquals(numOfMutations, count);

      // Make sure no splits occurred in the table
      assertTrue(client.tableOperations().listSplits(tableName).isEmpty());
    }
  }

  // 10 0's; 10 2's; 10 4's... 10 30's etc
  @Test
  @Timeout(60)
  public void automaticSplitWithGaps() throws Exception {
    log.info("Automatic Split With Gaps");
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      automaticSplit(client, 30, 2);
    }
  }

  // 10 0's; 10 1's; 10 2's... 10 15's etc
  @Test
  @Timeout(60)
  public void automaticSplitWithoutGaps() throws Exception {
    log.info("Automatic Split Without Gaps");
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      automaticSplit(client, 15, 1);
    }
  }

  @Test
  @Timeout(120)
  public void automaticSplitLater() throws Exception {
    log.info("Split later");
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // Generate large rows which have long common prefixes and therefore no split can be found.
      // Setting max to 1 causes all rows to have long common prefixes. Setting a max of greater
      // than 1 would generate a row with a short common prefix.
      final int max = 1;
      automaticSplit(client, max, 1);

      Predicate<String> isNotNamespaceTable =
          table -> !table.startsWith(Namespace.ACCUMULO.name() + ".");
      String tableName = client.tableOperations().list().stream().filter(isNotNamespaceTable)
          .findAny().orElseGet(() -> fail("couldn't find a table"));

      // No splits should have been able to occur.
      assertTrue(client.tableOperations().listSplits(tableName).isEmpty());

      try (BatchWriter batchWriter = client.createBatchWriter(tableName)) {
        byte[] data = new byte[10];
        for (int j = max; j < 150; j++) {
          // Fill key with all j's except for the last index
          Arrays.fill(data, 0, data.length - 2, (byte) j);

          // for each j, make the last index of key 0 through 24 then add the mutation
          for (int i = 0; i < 25; i++) {
            data[data.length - 1] = (byte) i;
            Mutation m = new Mutation(data);
            m.put("cf", "cq", "value");
            batchWriter.addMutation(m);
          }
        }
      }
      // Flush the BatchWriter and table then wait for splits to be present
      client.tableOperations().flush(tableName, new Text(), new Text("z"), true);

      // Make sure a split occurs
      Wait.Condition splitsToBePresent =
          () -> client.tableOperations().listSplits(tableName).stream().findAny().isPresent();
      Wait.waitFor(splitsToBePresent, SECONDS.toMillis(60L), 250L);
    }
  }

  @Test
  @Timeout(60)
  public void testUnsplittableColumn() throws Exception {
    log.info("Unsplittable Column Test");
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // make a table and lower the configuration properties
      // @formatter:off
      var maxEndRow = 100;
      Map<String,String> props = Map.of(
          Property.TABLE_SPLIT_THRESHOLD.getKey(), "1K",
          Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none",
          Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "64",
          Property.TABLE_MAX_END_ROW_SIZE.getKey(), "" + maxEndRow,
          Property.TABLE_MAJC_RATIO.getKey(), "9999"
      );
      // @formatter:on

      final String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

      // Create a key for a table entry that is longer than the allowed size for an
      // end row and fill this key with all m's except the last spot
      byte[] data = new byte[maxEndRow + 1];
      Arrays.fill(data, 0, data.length - 2, (byte) 'm');

      final int numOfMutations = 20;
      try (BatchWriter batchWriter = client.createBatchWriter(tableName)) {
        // Make the last place in the key different for every entry added to the table
        for (int i = 0; i < numOfMutations; i++) {
          data[data.length - 1] = (byte) i;
          Mutation m = new Mutation(data);
          m.put("cf", "cq", "value");
          batchWriter.addMutation(m);
        }
      }
      // Flush the BatchWriter and table
      client.tableOperations().flush(tableName, null, null, true);

      // Wait for the tablets to be marked as unsplittable due to the system split running
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      Wait.waitFor(() -> getServerContext().getAmple()
          .readTablet(new KeyExtent(tableId, null, null)).getUnSplittable() != null,
          Wait.MAX_WAIT_MILLIS, 100);

      // Verify that the unsplittable column is read correctly
      TabletMetadata tm =
          getServerContext().getAmple().readTablet(new KeyExtent(tableId, null, null));
      var unsplittable = tm.getUnSplittable();
      assertEquals(unsplittable, SplitUtils.toUnSplittable(getServerContext(), tm));

      // Make sure no splits occurred in the table
      assertTrue(client.tableOperations().listSplits(tableName).isEmpty());

      // Bump the value for max end row by 1, we should still not be able to split but this should
      // trigger an update to the unsplittable metadata value
      client.tableOperations().setProperty(tableName, Property.TABLE_MAX_END_ROW_SIZE.getKey(),
          "101");

      // wait for the unsplittable marker to be set to a new value due to the property change
      Wait.waitFor(() -> {
        var updatedUnsplittable = getServerContext().getAmple()
            .readTablet(new KeyExtent(tableId, null, null)).getUnSplittable();
        return updatedUnsplittable != null && !updatedUnsplittable.equals(unsplittable);
      }, Wait.MAX_WAIT_MILLIS, 100);
      // recheck with the computed meta is correct after property update
      tm = getServerContext().getAmple().readTablet(new KeyExtent(tableId, null, null));
      assertEquals(tm.getUnSplittable(), SplitUtils.toUnSplittable(getServerContext(), tm));

      // Bump max end row size and verify split occurs and unsplittable column is cleaned up
      client.tableOperations().setProperty(tableName, Property.TABLE_MAX_END_ROW_SIZE.getKey(),
          "500");

      // Wait for splits to occur
      assertTrue(client.tableOperations().listSplits(tableName).isEmpty());
      Wait.waitFor(() -> !client.tableOperations().listSplits(tableName).isEmpty(),
          Wait.MAX_WAIT_MILLIS, 100);

      // Verify all tablets have no unsplittable metadata column
      Wait.waitFor(() -> {
        try (var tabletsMetadata =
            getServerContext().getAmple().readTablets().forTable(tableId).build()) {
          return tabletsMetadata.stream()
              .allMatch(tabletMetadata -> tabletMetadata.getUnSplittable() == null);
        }
      }, Wait.MAX_WAIT_MILLIS, 100);
    }
  }

  // Test the unsplittable column is cleaned up if a previously marked unsplittable tablet
  // no longer needs to be split
  @Test
  @Timeout(60)
  public void testUnsplittableCleanup() throws Exception {
    log.info("Unsplittable Column Cleanup");
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // make a table and lower the configuration properties
      // @formatter:off
      Map<String,String> props = Map.of(
          Property.TABLE_SPLIT_THRESHOLD.getKey(), "1K",
          Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none",
          Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "64",
          Property.TABLE_MAJC_RATIO.getKey(), "9999"
      );
      // @formatter:on

      final String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

      byte[] data = new byte[100];
      Arrays.fill(data, 0, data.length - 1, (byte) 'm');

      // Write enough data that will cause a split. The row is not too large for a split
      // but all the rows are the same so tablets won't be able to split except for
      // the last tablet (null end row)
      final int numOfMutations = 20;
      try (BatchWriter batchWriter = client.createBatchWriter(tableName)) {
        // Make the last place in the key different for every entry added to the table
        for (int i = 0; i < numOfMutations; i++) {
          Mutation m = new Mutation(data);
          m.put("cf", "cq" + i, "value");
          batchWriter.addMutation(m);
        }
      }
      // Flush the BatchWriter and table
      client.tableOperations().flush(tableName, null, null, true);

      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));

      // Wait for a tablet to be marked as unsplittable due to the system split running
      // There is enough data to split more than once so at least one tablet should be marked
      // as unsplittable due to the same end row for all keys after the default tablet is split
      Wait.waitFor(() -> {
        try (var tabletsMetadata =
            getServerContext().getAmple().readTablets().forTable(tableId).build()) {
          return tabletsMetadata.stream().anyMatch(tm -> tm.getUnSplittable() != null);
        }
      }, Wait.MAX_WAIT_MILLIS, 100);

      var splits = client.tableOperations().listSplits(tableName);

      // Bump split threshold and verify marker is cleared
      client.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(),
          "1M");

      // All tablets should now be cleared of the unsplittable marker, and we should have the
      // same number of splits as before
      Wait.waitFor(() -> {
        try (var tabletsMetadata =
            getServerContext().getAmple().readTablets().forTable(tableId).build()) {
          return tabletsMetadata.stream().allMatch(tm -> tm.getUnSplittable() == null);
        }
      }, Wait.MAX_WAIT_MILLIS, 100);

      // no more splits should have happened
      assertEquals(splits, client.tableOperations().listSplits(tableName));
    }
  }

  private void automaticSplit(AccumuloClient client, int max, int spacing) throws Exception {
    // make a table and lower the configuration properties
    // @formatter:off
    final int maxEndRow = 1000;
    Map<String,String> props = Map.of(
      Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K",
      Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none",
      Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "64",
      Property.TABLE_MAX_END_ROW_SIZE.getKey(), ""+maxEndRow
    );
    // @formatter:on

    final String tableName = getUniqueNames(1)[0];
    client.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props));

    byte[] data = new byte[(int) (ConfigurationTypeHelper
        .getFixedMemoryAsBytes(Property.TABLE_MAX_END_ROW_SIZE.getDefaultValue()) + 2)];

    // Create a BatchWriter and key for a table entry that is longer than the allowed size for an
    // end row
    final int numOfMutations = 10;
    try (BatchWriter batchWriter = client.createBatchWriter(tableName)) {
      for (int j = 0; j < max; j += spacing) {

        // Fill key with all j's except the last index
        Arrays.fill(data, 0, data.length - 2, (byte) j);

        // for each j, make the last index of key 0 through 9 then add the mutation
        for (int i = 0; i < numOfMutations; i++) {
          data[data.length - 1] = (byte) i;
          Mutation m = new Mutation(data);
          m.put("cf", "cq", "value");
          batchWriter.addMutation(m);
        }
      }
    }
    // Flush the BatchWriter and table and sleep for a bit to make sure that there is enough time
    // for the table to split if need be.
    client.tableOperations().flush(tableName, new Text(), new Text("z"), true);
    Thread.sleep(500);

    // Make sure all the data that was put in the table is still correct
    int count = 0;
    int extra = numOfMutations;
    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
      for (Entry<Key,Value> entry : scanner) {
        if (extra == numOfMutations) {
          extra = 0;
          // fill all but the last index
          Arrays.fill(data, 0, data.length - 2, (byte) count);
          count += spacing;
        }
        Key k = entry.getKey();
        data[data.length - 1] = (byte) extra;
        String expected = new String(data, UTF_8);
        assertEquals(expected, k.getRow().toString());
        assertEquals("cf", k.getColumnFamily().toString());
        assertEquals("cq", k.getColumnQualifier().toString());
        assertEquals("value", entry.getValue().toString());
        extra++;
      }
    }
    assertEquals(numOfMutations, extra);
    assertEquals(max, count);

    // Make sure any splits are below the threshold. Accumulo may shorten splits by examining the
    // longest common prefix between consecutive rows. Since some rows in this data may have a
    // short longest common prefix, splits could be found. Any splits found should be below the
    // configured threshold.
    assertTrue(client.tableOperations().listSplits(tableName).stream()
        .allMatch(split -> split.getLength() < maxEndRow));
  }

}
