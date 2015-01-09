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
package org.apache.accumulo.core.client.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class AccumuloRowInputFormatTest {
  private static final String PREFIX = AccumuloRowInputFormatTest.class.getSimpleName();
  private static final String INSTANCE_NAME = PREFIX + "_mapreduce_instance";
  private static final String TEST_TABLE_1 = PREFIX + "_mapreduce_table_1";

  private static final String ROW1 = "row1";
  private static final String ROW2 = "row2";
  private static final String ROW3 = "row3";
  private static final String COLF1 = "colf1";
  private static List<Entry<Key,Value>> row1;
  private static List<Entry<Key,Value>> row2;
  private static List<Entry<Key,Value>> row3;
  private static AssertionError e1 = null;
  private static AssertionError e2 = null;

  public AccumuloRowInputFormatTest() {
    row1 = new ArrayList<Entry<Key,Value>>();
    row1.add(new KeyValue(new Key(ROW1, COLF1, "colq1"), "v1".getBytes()));
    row1.add(new KeyValue(new Key(ROW1, COLF1, "colq2"), "v2".getBytes()));
    row1.add(new KeyValue(new Key(ROW1, "colf2", "colq3"), "v3".getBytes()));
    row2 = new ArrayList<Entry<Key,Value>>();
    row2.add(new KeyValue(new Key(ROW2, COLF1, "colq4"), "v4".getBytes()));
    row3 = new ArrayList<Entry<Key,Value>>();
    row3.add(new KeyValue(new Key(ROW3, COLF1, "colq5"), "v5".getBytes()));
  }

  public static void checkLists(final List<Entry<Key,Value>> first, final List<Entry<Key,Value>> second) {
    assertEquals("Sizes should be the same.", first.size(), second.size());
    for (int i = 0; i < first.size(); i++) {
      assertEquals("Keys should be equal.", first.get(i).getKey(), second.get(i).getKey());
      assertEquals("Values should be equal.", first.get(i).getValue(), second.get(i).getValue());
    }
  }

  public static void checkLists(final List<Entry<Key,Value>> first, final Iterator<Entry<Key,Value>> second) {
    int entryIndex = 0;
    while (second.hasNext()) {
      final Entry<Key,Value> entry = second.next();
      assertEquals("Keys should be equal", first.get(entryIndex).getKey(), entry.getKey());
      assertEquals("Values should be equal", first.get(entryIndex).getValue(), entry.getValue());
      entryIndex++;
    }
  }

  public static void insertList(final BatchWriter writer, final List<Entry<Key,Value>> list) throws MutationsRejectedException {
    for (Entry<Key,Value> e : list) {
      final Key key = e.getKey();
      final Mutation mutation = new Mutation(key.getRow());
      ColumnVisibility colVisibility = new ColumnVisibility(key.getColumnVisibility());
      mutation.put(key.getColumnFamily(), key.getColumnQualifier(), colVisibility, key.getTimestamp(), e.getValue());
      writer.addMutation(mutation);
    }
  }

  private static class MRTester extends Configured implements Tool {
    private static class TestMapper extends Mapper<Text,PeekingIterator<Entry<Key,Value>>,Key,Value> {
      int count = 0;

      @Override
      protected void map(Text k, PeekingIterator<Entry<Key,Value>> v, Context context) throws IOException, InterruptedException {
        try {
          switch (count) {
            case 0:
              assertEquals("Current key should be " + ROW1, new Text(ROW1), k);
              checkLists(row1, v);
              break;
            case 1:
              assertEquals("Current key should be " + ROW2, new Text(ROW2), k);
              checkLists(row2, v);
              break;
            case 2:
              assertEquals("Current key should be " + ROW3, new Text(ROW3), k);
              checkLists(row3, v);
              break;
            default:
              assertTrue(false);
          }
        } catch (AssertionError e) {
          e1 = e;
        }
        count++;
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
          assertEquals(3, count);
        } catch (AssertionError e) {
          e2 = e;
        }
      }
    }

    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 3) {
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName() + " <user> <pass> <table>");
      }

      String user = args[0];
      String pass = args[1];
      String table = args[2];

      @SuppressWarnings("deprecation")
      Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());

      job.setInputFormatClass(AccumuloRowInputFormat.class);

      AccumuloInputFormat.setConnectorInfo(job, user, new PasswordToken(pass));
      AccumuloInputFormat.setInputTableName(job, table);
      AccumuloRowInputFormat.setMockInstance(job, INSTANCE_NAME);

      job.setMapperClass(TestMapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormatClass(NullOutputFormat.class);

      job.setNumReduceTasks(0);

      job.waitForCompletion(true);

      return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
      assertEquals(0, ToolRunner.run(CachedConfiguration.getInstance(), new MRTester(), args));
    }
  }

  @Test
  public void test() throws Exception {
    final MockInstance instance = new MockInstance(INSTANCE_NAME);
    final Connector conn = instance.getConnector("root", new PasswordToken(""));
    conn.tableOperations().create(TEST_TABLE_1);
    BatchWriter writer = null;
    try {
      writer = conn.createBatchWriter(TEST_TABLE_1, new BatchWriterConfig());
      insertList(writer, row1);
      insertList(writer, row2);
      insertList(writer, row3);
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
    MRTester.main(new String[] {"root", "", TEST_TABLE_1});
    assertNull(e1);
    assertNull(e2);
  }
}
