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
package org.apache.accumulo.hadoop.its.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.hadoopImpl.mapreduce.RangeInputSplit;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class MultiTableInputFormatIT extends AccumuloClusterHarness {

  private static AssertionError e1 = null;
  private static AssertionError e2 = null;

  private static class MRTester extends Configured implements Tool {

    private static class TestMapper extends Mapper<Key,Value,Key,Value> {
      Key key = null;
      int count = 0;

      @Override
      protected void map(Key k, Value v, Context context) throws IOException, InterruptedException {
        try {
          String tableName = ((RangeInputSplit) context.getInputSplit()).getTableName();
          if (key != null)
            assertEquals(key.getRow().toString(), new String(v.get()));
          assertEquals(new Text(String.format("%s_%09x", tableName, count + 1)), k.getRow());
          assertEquals(String.format("%s_%09x", tableName, count), new String(v.get()));
        } catch (AssertionError e) {
          e1 = e;
        }
        key = new Key(k);
        count++;
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
          assertEquals(100, count);
        } catch (AssertionError e) {
          e2 = e;
        }
      }
    }

    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 2) {
        throw new IllegalArgumentException(
            "Usage : " + MRTester.class.getName() + " <table1> <table2>");
      }

      String table1 = args[0];
      String table2 = args[1];

      Job job = Job.getInstance(getConf(),
          this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());

      job.setInputFormatClass(AccumuloInputFormat.class);

      AccumuloInputFormat.configure().clientProperties(getClientInfo().getProperties())
          .table(table1).table(table2).store(job);

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

  /**
   * Generate incrementing counts and attach table name to the key/value so that order and
   * multi-table data can be verified.
   */
  @Test
  public void testMap() throws Exception {
    String[] tableNames = getUniqueNames(2);
    String table1 = tableNames[0];
    String table2 = tableNames[1];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(table1);
      c.tableOperations().create(table2);
      BatchWriter bw = c.createBatchWriter(table1, new BatchWriterConfig());
      BatchWriter bw2 = c.createBatchWriter(table2, new BatchWriterConfig());
      for (int i = 0; i < 100; i++) {
        Mutation t1m = new Mutation(new Text(String.format("%s_%09x", table1, i + 1)));
        t1m.put(new Text(), new Text(), new Value(String.format("%s_%09x", table1, i).getBytes()));
        bw.addMutation(t1m);
        Mutation t2m = new Mutation(new Text(String.format("%s_%09x", table2, i + 1)));
        t2m.put(new Text(), new Text(), new Value(String.format("%s_%09x", table2, i).getBytes()));
        bw2.addMutation(t2m);
      }
      bw.close();
      bw2.close();

      MRTester.main(new String[] {table1, table2});
      assertNull(e1);
      assertNull(e2);
    }
  }

}
