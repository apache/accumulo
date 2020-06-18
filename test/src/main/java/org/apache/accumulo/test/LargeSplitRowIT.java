/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Scanner;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LargeSplitRowIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(LargeSplitRowIT.class);

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);

    Map<String,String> siteConfig = new HashMap<>();
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "50ms");
    cfg.setSiteConfig(siteConfig);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  // User added split
  @Test
  public void userAddedSplit() throws Exception {

    log.info("User added split");

    // make a table and lower the TABLE_END_ROW_MAX_SIZE property
    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      client.tableOperations().create(tableName);
      client.tableOperations().setProperty(tableName, Property.TABLE_MAX_END_ROW_SIZE.getKey(),
          "1000");

      // Create a BatchWriter and add a mutation to the table
      BatchWriter batchWriter = client.createBatchWriter(tableName, new BatchWriterConfig());
      Mutation m = new Mutation("Row");
      m.put("cf", "cq", "value");
      batchWriter.addMutation(m);
      batchWriter.close();

      // Create a split point that is too large to be an end row and fill it with all 'm'
      SortedSet<Text> partitionKeys = new TreeSet<>();
      byte[] data = new byte[(int) (ConfigurationTypeHelper
          .getFixedMemoryAsBytes(Property.TABLE_MAX_END_ROW_SIZE.getDefaultValue()) + 2)];
      for (int i = 0; i < data.length; i++) {
        data[i] = 'm';
      }
      partitionKeys.add(new Text(data));

      // try to add the split point that is too large, if the split point is created the test fails.
      try {
        client.tableOperations().addSplits(tableName, partitionKeys);
        fail();
      } catch (AccumuloServerException e) {}

      // Make sure that the information that was written to the table before we tried to add the
      // split
      // point is still correct
      int counter = 0;
      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        for (Entry<Key,Value> entry : scanner) {
          counter++;
          Key k = entry.getKey();
          assertEquals("Row", k.getRow().toString());
          assertEquals("cf", k.getColumnFamily().toString());
          assertEquals("cq", k.getColumnQualifier().toString());
          assertEquals("value", entry.getValue().toString());

        }
      }
      // Make sure there is only one line in the table
      assertEquals(1, counter);
    }
  }

  // Test tablet server split with 250 entries with all the same prefix
  @Test(timeout = 60 * 1000)
  public void automaticSplitWith250Same() throws Exception {
    log.info("Automatic with 250 with same prefix");

    // make a table and lower the configure properties
    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      client.tableOperations().create(tableName);
      client.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(),
          "10K");
      client.tableOperations().setProperty(tableName, Property.TABLE_FILE_COMPRESSION_TYPE.getKey(),
          "none");
      client.tableOperations().setProperty(tableName,
          Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "64");
      client.tableOperations().setProperty(tableName, Property.TABLE_MAX_END_ROW_SIZE.getKey(),
          "1000");

      // Create a BatchWriter and key for a table entry that is longer than the allowed size for an
      // end row
      // Fill this key with all m's except the last spot
      BatchWriter batchWriter = client.createBatchWriter(tableName, new BatchWriterConfig());
      byte[] data = new byte[(int) (ConfigurationTypeHelper
          .getFixedMemoryAsBytes(Property.TABLE_MAX_END_ROW_SIZE.getDefaultValue()) + 2)];
      for (int i = 0; i < data.length - 1; i++) {
        data[i] = (byte) 'm';
      }

      // Make the last place in the key different for every entry added to the table
      for (int i = 0; i < 250; i++) {
        data[data.length - 1] = (byte) i;
        Mutation m = new Mutation(data);
        m.put("cf", "cq", "value");
        batchWriter.addMutation(m);
      }
      // Flush the BatchWriter and table and sleep for a bit to make sure that there is enough time
      // for the table to split if need be.
      batchWriter.close();
      client.tableOperations().flush(tableName, new Text(), new Text("z"), true);
      Thread.sleep(500);

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
      assertEquals(250, count);

      // Make sure no splits occurred in the table
      assertEquals(0, client.tableOperations().listSplits(tableName).size());
    }
  }

  // 10 0's; 10 2's; 10 4's... 10 30's etc
  @Test(timeout = 60 * 1000)
  public void automaticSplitWithGaps() throws Exception {
    log.info("Automatic Split With Gaps");
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      automaticSplit(client, 30, 2);
    }
  }

  // 10 0's; 10 1's; 10 2's... 10 15's etc
  @Test(timeout = 60 * 1000)
  public void automaticSplitWithoutGaps() throws Exception {
    log.info("Automatic Split Without Gaps");
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      automaticSplit(client, 15, 1);
    }
  }

  @Test(timeout = 60 * 1000)
  public void automaticSplitLater() throws Exception {
    log.info("Split later");
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      automaticSplit(client, 15, 1);

      String tableName = new String();

      for (String curr : client.tableOperations().list()) {
        if (!curr.startsWith(Namespace.ACCUMULO.name() + ".")) {
          tableName = curr;
        }
      }

      // Create a BatchWriter and key for a table entry that is longer than the allowed size for an
      // end row
      BatchWriter batchWriter = client.createBatchWriter(tableName, new BatchWriterConfig());
      byte[] data = new byte[10];

      // Fill key with all j's except for last spot which alternates through 1 through 10 for every
      // j
      // value
      for (int j = 15; j < 150; j += 1) {
        for (int i = 0; i < data.length - 1; i++) {
          data[i] = (byte) j;
        }

        for (int i = 0; i < 25; i++) {
          data[data.length - 1] = (byte) i;
          Mutation m = new Mutation(data);
          m.put("cf", "cq", "value");
          batchWriter.addMutation(m);
        }
      }
      // Flush the BatchWriter and table and sleep for a bit to make sure that there is enough time
      // for the table to split if need be.
      batchWriter.close();
      client.tableOperations().flush(tableName, new Text(), new Text("z"), true);

      // Make sure a split occurs
      while (client.tableOperations().listSplits(tableName).isEmpty()) {
        Thread.sleep(250);
      }

      assertTrue(!client.tableOperations().listSplits(tableName).isEmpty());
    }
  }

  private void automaticSplit(AccumuloClient client, int max, int spacing) throws Exception {
    // make a table and lower the configure properties
    final String tableName = getUniqueNames(1)[0];
    client.tableOperations().create(tableName);
    client.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    client.tableOperations().setProperty(tableName, Property.TABLE_FILE_COMPRESSION_TYPE.getKey(),
        "none");
    client.tableOperations().setProperty(tableName,
        Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "64");
    client.tableOperations().setProperty(tableName, Property.TABLE_MAX_END_ROW_SIZE.getKey(),
        "1000");

    // Create a BatchWriter and key for a table entry that is longer than the allowed size for an
    // end row
    BatchWriter batchWriter = client.createBatchWriter(tableName, new BatchWriterConfig());
    byte[] data = new byte[(int) (ConfigurationTypeHelper
        .getFixedMemoryAsBytes(Property.TABLE_MAX_END_ROW_SIZE.getDefaultValue()) + 2)];

    // Fill key with all j's except for last spot which alternates through 1 through 10 for every j
    // value
    for (int j = 0; j < max; j += spacing) {
      for (int i = 0; i < data.length - 1; i++) {
        data[i] = (byte) j;
      }

      for (int i = 0; i < 10; i++) {
        data[data.length - 1] = (byte) i;
        Mutation m = new Mutation(data);
        m.put("cf", "cq", "value");
        batchWriter.addMutation(m);
      }
    }
    // Flush the BatchWriter and table and sleep for a bit to make sure that there is enough time
    // for the table to split if need be.
    batchWriter.close();
    client.tableOperations().flush(tableName, new Text(), new Text("z"), true);
    Thread.sleep(500);

    // Make sure all the data that was put in the table is still correct
    int count = 0;
    int extra = 10;
    try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
      for (Entry<Key,Value> entry : scanner) {
        if (extra == 10) {
          extra = 0;
          for (int i = 0; i < data.length - 1; i++) {
            data[i] = (byte) count;
          }
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
    assertEquals(10, extra);
    assertEquals(max, count);

    // Make sure no splits occurred in the table
    assertEquals(0, client.tableOperations().listSplits(tableName).size());
  }

}
