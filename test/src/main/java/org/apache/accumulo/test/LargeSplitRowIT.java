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
package org.apache.accumulo.test;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.AccumuloServerException;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LargeSplitRowIT extends ConfigurableMacBase {
  static private final Logger log = LoggerFactory.getLogger(LargeSplitRowIT.class);

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);

    Map<String,String> siteConfig = new HashMap<>();
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "50ms");
    cfg.setSiteConfig(siteConfig);
  }

  // User added split
  @Test(timeout = 60 * 1000)
  public void userAddedSplit() throws Exception {

    log.info("User added split");

    // make a table and lower the TABLE_END_ROW_MAX_SIZE property
    final String tableName = getUniqueNames(1)[0];
    final Connector conn = getConnector();
    conn.tableOperations().create(tableName);
    conn.tableOperations().setProperty(tableName, Property.TABLE_MAX_END_ROW_SIZE.getKey(), "1000");

    // Create a BatchWriter and add a mutation to the table
    BatchWriter batchWriter = conn.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("Row");
    m.put("cf", "cq", "value");
    batchWriter.addMutation(m);
    batchWriter.close();

    // Create a split point that is too large to be an end row and fill it with all 'm'
    SortedSet<Text> partitionKeys = new TreeSet<>();
    byte data[] = new byte[(int) (ConfigurationTypeHelper.getFixedMemoryAsBytes(Property.TABLE_MAX_END_ROW_SIZE.getDefaultValue()) + 2)];
    for (int i = 0; i < data.length; i++) {
      data[i] = 'm';
    }
    partitionKeys.add(new Text(data));

    // try to add the split point that is too large, if the split point is created the test fails.
    try {
      conn.tableOperations().addSplits(tableName, partitionKeys);
      Assert.fail();
    } catch (AccumuloServerException e) {}

    // Make sure that the information that was written to the table before we tried to add the split point is still correct
    int counter = 0;
    final Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
    for (Entry<Key,Value> entry : scanner) {
      counter++;
      Key k = entry.getKey();
      Assert.assertEquals("Row", k.getRow().toString());
      Assert.assertEquals("cf", k.getColumnFamily().toString());
      Assert.assertEquals("cq", k.getColumnQualifier().toString());
      Assert.assertEquals("value", entry.getValue().toString());

    }
    // Make sure there is only one line in the table
    Assert.assertEquals(1, counter);
  }

  // Test tablet server split with 250 entries with all the same prefix
  @Test(timeout = 60 * 1000)
  public void automaticSplitWith250Same() throws Exception {
    log.info("Automatic with 250 with same prefix");

    // make a table and lower the configure properties
    final String tableName = getUniqueNames(1)[0];
    final Connector conn = getConnector();
    conn.tableOperations().create(tableName);
    conn.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    conn.tableOperations().setProperty(tableName, Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none");
    conn.tableOperations().setProperty(tableName, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "64");
    conn.tableOperations().setProperty(tableName, Property.TABLE_MAX_END_ROW_SIZE.getKey(), "1000");

    // Create a BatchWriter and key for a table entry that is longer than the allowed size for an end row
    // Fill this key with all m's except the last spot
    BatchWriter batchWriter = conn.createBatchWriter(tableName, new BatchWriterConfig());
    byte data[] = new byte[(int) (ConfigurationTypeHelper.getFixedMemoryAsBytes(Property.TABLE_MAX_END_ROW_SIZE.getDefaultValue()) + 2)];
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
    // Flush the BatchWriter and table and sleep for a bit to make sure that there is enough time for the table to split if need be.
    batchWriter.close();
    conn.tableOperations().flush(tableName, new Text(), new Text("z"), true);
    Thread.sleep(500);

    // Make sure all the data that was put in the table is still correct
    int count = 0;
    final Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
    for (Entry<Key,Value> entry : scanner) {
      Key k = entry.getKey();
      data[data.length - 1] = (byte) count;
      String expected = new String(data, UTF_8);
      Assert.assertEquals(expected, k.getRow().toString());
      Assert.assertEquals("cf", k.getColumnFamily().toString());
      Assert.assertEquals("cq", k.getColumnQualifier().toString());
      Assert.assertEquals("value", entry.getValue().toString());
      count++;
    }
    Assert.assertEquals(250, count);

    // Make sure no splits occurred in the table
    Assert.assertEquals(0, conn.tableOperations().listSplits(tableName).size());
  }

  // 10 0's; 10 2's; 10 4's... 10 30's etc
  @Test(timeout = 60 * 1000)
  public void automaticSplitWithGaps() throws Exception {
    log.info("Automatic Split With Gaps");

    automaticSplit(30, 2);
  }

  // 10 0's; 10 1's; 10 2's... 10 15's etc
  @Test(timeout = 60 * 1000)
  public void automaticSplitWithoutGaps() throws Exception {
    log.info("Automatic Split Without Gaps");

    automaticSplit(15, 1);
  }

  @Test(timeout = 60 * 1000)
  public void automaticSplitLater() throws Exception {
    log.info("Split later");
    automaticSplit(15, 1);

    final Connector conn = getConnector();

    String tableName = new String();
    java.util.Iterator<String> iterator = conn.tableOperations().list().iterator();

    while (iterator.hasNext()) {
      String curr = iterator.next();
      if (!curr.startsWith(Namespaces.ACCUMULO_NAMESPACE + ".")) {
        tableName = curr;
      }
    }

    // Create a BatchWriter and key for a table entry that is longer than the allowed size for an end row
    BatchWriter batchWriter = conn.createBatchWriter(tableName, new BatchWriterConfig());
    byte data[] = new byte[10];

    // Fill key with all j's except for last spot which alternates through 1 through 10 for every j value
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
    // Flush the BatchWriter and table and sleep for a bit to make sure that there is enough time for the table to split if need be.
    batchWriter.close();
    conn.tableOperations().flush(tableName, new Text(), new Text("z"), true);

    // Make sure a split occurs
    while (conn.tableOperations().listSplits(tableName).size() == 0) {
      Thread.sleep(250);
    }

    Assert.assertTrue(0 < conn.tableOperations().listSplits(tableName).size());
  }

  private void automaticSplit(int max, int spacing) throws Exception {
    // make a table and lower the configure properties
    final String tableName = getUniqueNames(1)[0];
    final Connector conn = getConnector();
    conn.tableOperations().create(tableName);
    conn.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K");
    conn.tableOperations().setProperty(tableName, Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none");
    conn.tableOperations().setProperty(tableName, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "64");
    conn.tableOperations().setProperty(tableName, Property.TABLE_MAX_END_ROW_SIZE.getKey(), "1000");

    // Create a BatchWriter and key for a table entry that is longer than the allowed size for an end row
    BatchWriter batchWriter = conn.createBatchWriter(tableName, new BatchWriterConfig());
    byte data[] = new byte[(int) (ConfigurationTypeHelper.getFixedMemoryAsBytes(Property.TABLE_MAX_END_ROW_SIZE.getDefaultValue()) + 2)];

    // Fill key with all j's except for last spot which alternates through 1 through 10 for every j value
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
    // Flush the BatchWriter and table and sleep for a bit to make sure that there is enough time for the table to split if need be.
    batchWriter.close();
    conn.tableOperations().flush(tableName, new Text(), new Text("z"), true);
    Thread.sleep(500);

    // Make sure all the data that was put in the table is still correct
    int count = 0;
    int extra = 10;
    final Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY);
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
      Assert.assertEquals(expected, k.getRow().toString());
      Assert.assertEquals("cf", k.getColumnFamily().toString());
      Assert.assertEquals("cq", k.getColumnQualifier().toString());
      Assert.assertEquals("value", entry.getValue().toString());
      extra++;
    }
    Assert.assertEquals(10, extra);
    Assert.assertEquals(max, count);

    // Make sure no splits occured in the table
    Assert.assertEquals(0, conn.tableOperations().listSplits(tableName).size());

  }

}
