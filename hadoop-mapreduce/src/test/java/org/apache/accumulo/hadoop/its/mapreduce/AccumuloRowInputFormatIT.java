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
package org.apache.accumulo.hadoop.its.mapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.hadoop.mapreduce.AccumuloRowInputFormat;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the new MR API in the hadoop-mapreduce package.
 *
 * @since 2.0
 */
public class AccumuloRowInputFormatIT extends AccumuloClusterHarness {

  private static final String ROW1 = "row1";
  private static final String ROW2 = "row2";
  private static final String ROW3 = "row3";
  private static final String COLF1 = "colf1";
  private static List<Entry<Key,Value>> row1;
  private static List<Entry<Key,Value>> row2;
  private static List<Entry<Key,Value>> row3;
  private static AssertionError e1 = null;
  private static AssertionError e2 = null;

  @BeforeAll
  public static void prepareRows() {
    row1 = new ArrayList<>();
    row1.add(new KeyValue(new Key(ROW1, COLF1, "colq1"), "v1".getBytes()));
    row1.add(new KeyValue(new Key(ROW1, COLF1, "colq2"), "v2".getBytes()));
    row1.add(new KeyValue(new Key(ROW1, "colf2", "colq3"), "v3".getBytes()));
    row2 = new ArrayList<>();
    row2.add(new KeyValue(new Key(ROW2, COLF1, "colq4"), "v4".getBytes()));
    row3 = new ArrayList<>();
    row3.add(new KeyValue(new Key(ROW3, COLF1, "colq5"), "v5".getBytes()));
  }

  private static void checkLists(final List<Entry<Key,Value>> first,
      final Iterator<Entry<Key,Value>> second) {
    int entryIndex = 0;
    while (second.hasNext()) {
      final Entry<Key,Value> entry = second.next();
      assertEquals(first.get(entryIndex).getKey(), entry.getKey(), "Keys should be equal");
      assertEquals(first.get(entryIndex).getValue(), entry.getValue(), "Values should be equal");
      entryIndex++;
    }
  }

  private static void insertList(final BatchWriter writer, final List<Entry<Key,Value>> list)
      throws MutationsRejectedException {
    for (Entry<Key,Value> e : list) {
      final Key key = e.getKey();
      final Mutation mutation = new Mutation(key.getRow());
      ColumnVisibility colVisibility = new ColumnVisibility(key.getColumnVisibility());
      mutation.put(key.getColumnFamily(), key.getColumnQualifier(), colVisibility,
          key.getTimestamp(), e.getValue());
      writer.addMutation(mutation);
    }
  }

  private static class MRTester extends Configured implements Tool {
    private static class TestMapper
        extends Mapper<Text,PeekingIterator<Entry<Key,Value>>,Key,Value> {
      int count = 0;

      @Override
      protected void map(Text k, PeekingIterator<Entry<Key,Value>> v, Context context) {
        try {
          switch (count) {
            case 0:
              assertEquals(new Text(ROW1), k, "Current key should be " + ROW1);
              checkLists(row1, v);
              break;
            case 1:
              assertEquals(new Text(ROW2), k, "Current key should be " + ROW2);
              checkLists(row2, v);
              break;
            case 2:
              assertEquals(new Text(ROW3), k, "Current key should be " + ROW3);
              checkLists(row3, v);
              break;
            default:
              fail();
          }
        } catch (AssertionError e) {
          e1 = e;
        }
        count++;
      }

      @Override
      protected void cleanup(Context context) {
        try {
          assertEquals(3, count);
        } catch (AssertionError e) {
          e2 = e;
        }
      }
    }

    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 1) {
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName() + " <table>");
      }

      String table = args[0];

      Job job = Job.getInstance(getConf(),
          this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());

      job.setInputFormatClass(AccumuloRowInputFormat.class);

      AccumuloRowInputFormat.configure().clientProperties(getClientProps()).table(table)
          .auths(Authorizations.EMPTY).store(job);

      job.setMapperClass(TestMapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormatClass(NullOutputFormat.class);

      job.setNumReduceTasks(0);

      job.waitForCompletion(true);

      return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("mapreduce.framework.name", "local");
      conf.set("mapreduce.cluster.local.dir",
          new File(System.getProperty("user.dir"), "target/mapreduce-tmp").getAbsolutePath());
      assertEquals(0, ToolRunner.run(conf, new MRTester(), args));
    }
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      try (BatchWriter writer = client.createBatchWriter(tableName)) {
        insertList(writer, row1);
        insertList(writer, row2);
        insertList(writer, row3);
      }
      MRTester.main(new String[] {tableName});
      assertNull(e1);
      assertNull(e2);
    }
  }
}
