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
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.AccumuloServerException;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
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
    cfg.setNumTservers(1);

    Map<String,String> siteConfig = Map.of(Property.TSERV_MAJC_DELAY.getKey(), "50ms");
    cfg.setSiteConfig(siteConfig);
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
      assertThrows(AccumuloServerException.class,
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
        Property.TABLE_MAX_END_ROW_SIZE.getKey(), "1000"
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
      Thread.sleep(500L);

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
      final int max = 15;
      automaticSplit(client, max, 1);

      Predicate<String> isNotNamespaceTable =
          table -> !table.startsWith(Namespace.ACCUMULO.name() + ".");
      String tableName = client.tableOperations().list().stream().filter(isNotNamespaceTable)
          .findAny().orElseGet(() -> fail("couldn't find a table"));

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

      assertTrue(client.tableOperations().listSplits(tableName).stream().findAny().isPresent());
    }
  }

  private void automaticSplit(AccumuloClient client, int max, int spacing) throws Exception {
    // make a table and lower the configuration properties
    // @formatter:off
    Map<String,String> props = Map.of(
      Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K",
      Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none",
      Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "64",
      Property.TABLE_MAX_END_ROW_SIZE.getKey(), "1000"
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

    // Make sure no splits occurred in the table
    assertTrue(client.tableOperations().listSplits(tableName).isEmpty());
  }

}
