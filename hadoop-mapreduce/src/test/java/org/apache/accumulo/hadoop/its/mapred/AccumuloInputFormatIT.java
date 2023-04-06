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
package org.apache.accumulo.hadoop.its.mapred;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.hadoop.mapred.AccumuloInputFormat;
import org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder;
import org.apache.accumulo.hadoopImpl.mapred.RangeInputSplit;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AccumuloInputFormatIT extends AccumuloClusterHarness {

  @BeforeAll
  public static void setupClass() {
    System.setProperty("hadoop.tmp.dir", System.getProperty("user.dir") + "/target/hadoop-tmp");
  }

  private static AssertionError e1 = null;
  private static int e1Count = 0;
  private static AssertionError e2 = null;
  private static int e2Count = 0;

  private static class MRTester extends Configured implements Tool {
    private static class TestMapper implements Mapper<Key,Value,Key,Value> {
      Key key = null;
      int count = 0;

      @Override
      public void map(Key k, Value v, OutputCollector<Key,Value> output, Reporter reporter) {
        try {
          if (key != null) {
            assertEquals(key.getRow().toString(), new String(v.get()));
          }
          assertEquals(k.getRow(), new Text(String.format("%09x", count + 1)));
          assertEquals(new String(v.get()), String.format("%09x", count));
        } catch (AssertionError e) {
          e1 = e;
          e1Count++;
        }
        key = new Key(k);
        count++;
      }

      @Override
      public void configure(JobConf job) {}

      @Override
      public void close() {
        try {
          assertEquals(100, count);
        } catch (AssertionError e) {
          e2 = e;
          e2Count++;
        }
      }

    }

    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 1 && args.length != 3) {
        throw new IllegalArgumentException(
            "Usage : " + MRTester.class.getName() + " <table> [<batchScan> <scan sample>]");
      }

      String table = args[0];
      boolean batchScan = false;
      boolean sample = false;
      if (args.length == 3) {
        batchScan = Boolean.parseBoolean(args[1]);
        sample = Boolean.parseBoolean(args[2]);
      }

      JobConf job = new JobConf(getConf());
      job.setJarByClass(this.getClass());

      job.setInputFormat(AccumuloInputFormat.class);

      InputFormatBuilder.InputFormatOptions<JobConf> opts = AccumuloInputFormat.configure()
          .clientProperties(getClientProps()).table(table).auths(Authorizations.EMPTY);

      if (sample) {
        opts.samplerConfiguration(SAMPLER_CONFIG);
      }
      opts.batchScan(batchScan).store(job);

      job.setMapperClass(TestMapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormat(NullOutputFormat.class);

      job.setNumReduceTasks(0);

      return JobClient.runJob(job).isSuccessful() ? 0 : 1;
    }

    public static void main(String... args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("mapreduce.framework.name", "local");
      conf.set("mapreduce.cluster.local.dir",
          new File(System.getProperty("user.dir"), "target/mapreduce-tmp").getAbsolutePath());
      assertEquals(0, ToolRunner.run(conf, new MRTester(), args));
    }
  }

  @Test
  public void testMap() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(table);
      BatchWriter bw = c.createBatchWriter(table);
      for (int i = 0; i < 100; i++) {
        Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
        m.put("", "", String.format("%09x", i));
        bw.addMutation(m);
      }
      bw.close();

      e1 = null;
      e2 = null;

      MRTester.main(table);
      assertNull(e1);
      assertNull(e2);
    }
  }

  private static final SamplerConfiguration SAMPLER_CONFIG =
      new SamplerConfiguration(RowSampler.class.getName()).addOption("hasher", "murmur3_32")
          .addOption("modulus", "3");

  @Test
  public void testSample() throws Exception {
    final String TEST_TABLE_3 = getUniqueNames(1)[0];

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(TEST_TABLE_3,
          new NewTableConfiguration().enableSampling(SAMPLER_CONFIG));
      BatchWriter bw = c.createBatchWriter(TEST_TABLE_3);
      for (int i = 0; i < 100; i++) {
        Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
        m.put("", "", String.format("%09x", i));
        bw.addMutation(m);
      }
      bw.close();

      MRTester.main(TEST_TABLE_3, "False", "True");
      assertEquals(38, e1Count);
      assertEquals(1, e2Count);

      e2Count = e1Count = 0;
      MRTester.main(TEST_TABLE_3, "False", "False");
      assertEquals(0, e1Count);
      assertEquals(0, e2Count);

      e2Count = e1Count = 0;
      MRTester.main(TEST_TABLE_3, "True", "True");
      assertEquals(38, e1Count);
      assertEquals(1, e2Count);
    }
  }

  @Test
  public void testCorrectRangeInputSplits() throws Exception {
    JobConf job = new JobConf();

    String table = getUniqueNames(1)[0];
    Authorizations auths = new Authorizations("foo");
    Collection<IteratorSetting.Column> fetchColumns =
        Collections.singleton(new IteratorSetting.Column(new Text("foo"), new Text("bar")));

    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      accumuloClient.tableOperations().create(table);

      AccumuloInputFormat.configure().clientProperties(getClientProps()).table(table).auths(auths)
          .fetchColumns(fetchColumns).scanIsolation(true).localIterators(true).store(job);

      AccumuloInputFormat aif = new AccumuloInputFormat();

      InputSplit[] splits = aif.getSplits(job, 1);

      assertEquals(1, splits.length);

      InputSplit split = splits[0];

      assertEquals(RangeInputSplit.class, split.getClass());

      RangeInputSplit risplit = (RangeInputSplit) split;

      assertEquals(table, risplit.getTableName());
      assertTrue(risplit.isIsolatedScan());
      assertTrue(risplit.usesLocalIterators());
      assertEquals(fetchColumns, risplit.getFetchedColumns());
    }
  }
}
