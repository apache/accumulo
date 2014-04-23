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
package org.apache.accumulo.core.client.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.InputTableConfig;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class AccumuloMultiTableInputFormatTest {

  private static final String PREFIX = AccumuloMultiTableInputFormatTest.class.getSimpleName();
  private static final String INSTANCE_NAME = PREFIX + "_mapred_instance";
  private static final String TEST_TABLE_1 = PREFIX + "_mapred_table_1";
  private static final String TEST_TABLE_2 = PREFIX + "_mapred_table_2";

  private static AssertionError e1 = null;
  private static AssertionError e2 = null;

  private static class MRTester extends Configured implements Tool {
    private static class TestMapper implements Mapper<Key,Value,Key,Value> {
      Key key = null;
      int count = 0;

      @Override
      public void map(Key k, Value v, OutputCollector<Key,Value> output, Reporter reporter) throws IOException {
        try {
          String tableName = ((RangeInputSplit) reporter.getInputSplit()).getTableName();
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
      public void configure(JobConf job) {}

      @Override
      public void close() throws IOException {
        try {
          assertEquals(100, count);
        } catch (AssertionError e) {
          e2 = e;
        }
      }

    }

    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 4) {
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName() + " <user> <pass> <table1> <table2>");
      }

      String user = args[0];
      String pass = args[1];
      String table1 = args[2];
      String table2 = args[3];

      JobConf job = new JobConf(getConf());
      job.setJarByClass(this.getClass());

      job.setInputFormat(AccumuloInputFormat.class);

      AccumuloMultiTableInputFormat.setConnectorInfo(job, user, new PasswordToken(pass));
      AccumuloMultiTableInputFormat.setMockInstance(job, INSTANCE_NAME);

      InputTableConfig tableConfig1 = new InputTableConfig();
      InputTableConfig tableConfig2 = new InputTableConfig();

      Map<String,InputTableConfig> configMap = new HashMap<String,InputTableConfig>();
      configMap.put(table1, tableConfig1);
      configMap.put(table2, tableConfig2);

      AccumuloMultiTableInputFormat.setInputTableConfigs(job, configMap);

      job.setMapperClass(TestMapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormat(NullOutputFormat.class);

      job.setNumReduceTasks(0);

      return JobClient.runJob(job).isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
      assertEquals(0, ToolRunner.run(CachedConfiguration.getInstance(), new MRTester(), args));
    }
  }

  @Test
  public void testMap() throws Exception {
    MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
    Connector c = mockInstance.getConnector("root", new PasswordToken(""));
    c.tableOperations().create(TEST_TABLE_1);
    c.tableOperations().create(TEST_TABLE_2);
    BatchWriter bw = c.createBatchWriter(TEST_TABLE_1, new BatchWriterConfig());
    BatchWriter bw2 = c.createBatchWriter(TEST_TABLE_2, new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation t1m = new Mutation(new Text(String.format("%s_%09x", TEST_TABLE_1, i + 1)));
      t1m.put(new Text(), new Text(), new Value(String.format("%s_%09x", TEST_TABLE_1, i).getBytes()));
      bw.addMutation(t1m);
      Mutation t2m = new Mutation(new Text(String.format("%s_%09x", TEST_TABLE_2, i + 1)));
      t2m.put(new Text(), new Text(), new Value(String.format("%s_%09x", TEST_TABLE_2, i).getBytes()));
      bw2.addMutation(t2m);
    }
    bw.close();
    bw2.close();

    MRTester.main(new String[] {"root", "", TEST_TABLE_1, TEST_TABLE_2});
    assertNull(e1);
    assertNull(e2);
  }

  /**
   * Verify {@link org.apache.accumulo.core.client.mapreduce.InputTableConfig} objects get correctly serialized in the JobContext.
   */
  @Test
  public void testTableQueryConfigSerialization() throws IOException {

    JobConf job = new JobConf();

    InputTableConfig table1 = new InputTableConfig().setRanges(Collections.singletonList(new Range("a", "b")))
        .fetchColumns(Collections.singleton(new Pair<Text,Text>(new Text("CF1"), new Text("CQ1"))))
        .setIterators(Collections.singletonList(new IteratorSetting(50, "iter1", "iterclass1")));

    InputTableConfig table2 = new InputTableConfig().setRanges(Collections.singletonList(new Range("a", "b")))
        .fetchColumns(Collections.singleton(new Pair<Text,Text>(new Text("CF1"), new Text("CQ1"))))
        .setIterators(Collections.singletonList(new IteratorSetting(50, "iter1", "iterclass1")));

    Map<String,InputTableConfig> configMap = new HashMap<String,InputTableConfig>();
    configMap.put(TEST_TABLE_1, table1);
    configMap.put(TEST_TABLE_2, table2);
    AccumuloMultiTableInputFormat.setInputTableConfigs(job, configMap);

    assertEquals(table1, AccumuloMultiTableInputFormat.getInputTableConfig(job, TEST_TABLE_1));
    assertEquals(table2, AccumuloMultiTableInputFormat.getInputTableConfig(job, TEST_TABLE_2));
  }
}
