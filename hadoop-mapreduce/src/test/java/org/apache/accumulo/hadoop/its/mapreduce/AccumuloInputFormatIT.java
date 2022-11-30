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

import static java.lang.System.currentTimeMillis;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.hadoop.mapreduce.InputFormatBuilder.InputFormatOptions;
import org.apache.accumulo.hadoopImpl.mapreduce.BatchInputSplit;
import org.apache.accumulo.hadoopImpl.mapreduce.RangeInputSplit;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * Tests the new MR API in the hadoop-mapreduce package.
 *
 * @since 2.0
 */
public class AccumuloInputFormatIT extends AccumuloClusterHarness {

  AccumuloInputFormat inputFormat;
  AccumuloClient client;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
  }

  @BeforeEach
  public void before() {
    inputFormat = new AccumuloInputFormat();
    client = Accumulo.newClient().from(getClientProps()).build();
  }

  @AfterEach
  public void closeClient() {
    client.close();
  }

  /**
   * Tests several different paths through the getSplits() method by setting different properties
   * and verifying the results.
   */
  @Test
  public void testGetSplits() throws Exception {
    String table = getUniqueNames(1)[0];
    client.tableOperations().create(table);
    insertData(table, currentTimeMillis());

    Job job = Job.getInstance();
    AccumuloInputFormat.configure().clientProperties(getClientProps()).table(table)
        .auths(Authorizations.EMPTY).scanIsolation(true).store(job);

    // split table
    TreeSet<Text> splitsToAdd = new TreeSet<>();
    for (int i = 0; i < 10000; i += 1000) {
      splitsToAdd.add(new Text(String.format("%09d", i)));
    }
    client.tableOperations().addSplits(table, splitsToAdd);
    sleepUninterruptibly(500, TimeUnit.MILLISECONDS); // wait for splits to be propagated

    // get splits without setting any range
    // No ranges set on the job so it'll start with -inf
    Collection<Text> actualSplits = client.tableOperations().listSplits(table);
    List<InputSplit> splits = inputFormat.getSplits(job);
    assertEquals(actualSplits.size() + 1, splits.size());

    // set ranges and get splits
    List<Range> ranges = new ArrayList<>();
    for (Text text : actualSplits) {
      ranges.add(new Range(text));
    }
    AccumuloInputFormat.configure().clientProperties(getClientProps()).table(table)
        .auths(Authorizations.EMPTY).ranges(ranges).store(job);
    splits = inputFormat.getSplits(job);
    assertEquals(actualSplits.size(), splits.size());

    // offline mode
    AccumuloInputFormat.configure().clientProperties(getClientProps()).table(table)
        .auths(Authorizations.EMPTY).offlineScan(true).store(job);
    assertThrows(IOException.class, () -> inputFormat.getSplits(job));

    client.tableOperations().offline(table, true);
    splits = inputFormat.getSplits(job);
    assertEquals(actualSplits.size(), splits.size());

    // auto adjust ranges
    ranges = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      // overlapping ranges
      ranges.add(new Range(String.format("%09d", i), String.format("%09d", i + 2)));
    }

    AccumuloInputFormat.configure().clientProperties(getClientProps()).table(table)
        .auths(Authorizations.EMPTY).ranges(ranges).offlineScan(true).store(job);
    splits = inputFormat.getSplits(job);
    assertEquals(2, splits.size());

    AccumuloInputFormat.configure().clientProperties(getClientProps()).table(table)
        .auths(Authorizations.EMPTY).ranges(ranges).autoAdjustRanges(false).offlineScan(true)
        .store(job);
    splits = inputFormat.getSplits(job);
    assertEquals(ranges.size(), splits.size());

    // BatchScan not available for offline scans
    AccumuloInputFormat.configure().clientProperties(getClientProps()).table(table)
        .auths(Authorizations.EMPTY).batchScan(true).offlineScan(true).store(job);

    assertThrows(IllegalArgumentException.class, () -> inputFormat.getSplits(job),
        "IllegalArgumentException should have been thrown trying to batch scan offline");

    // table online tests
    client.tableOperations().online(table, true);
    AccumuloInputFormat.configure().clientProperties(getClientProps()).table(table)
        .auths(Authorizations.EMPTY).store(job);
    // test for resumption of success
    splits = inputFormat.getSplits(job);
    assertEquals(2, splits.size());

    // BatchScan not available with isolated iterators
    AccumuloInputFormat.configure().clientProperties(getClientProps()).table(table)
        .auths(Authorizations.EMPTY).batchScan(true).scanIsolation(true).store(job);

    assertThrows(IllegalArgumentException.class, () -> inputFormat.getSplits(job),
        "IllegalArgumentException should have been thrown trying to batch scan with isolation");

    // BatchScan not available with local iterators
    AccumuloInputFormat.configure().clientProperties(getClientProps()).table(table)
        .auths(Authorizations.EMPTY).batchScan(true).localIterators(true).store(job);

    assertThrows(IllegalArgumentException.class, () -> inputFormat.getSplits(job),
        "IllegalArgumentException should have been thrown trying to batch scan locally");

    AccumuloInputFormat.configure().clientProperties(getClientProps()).table(table)
        .auths(Authorizations.EMPTY).batchScan(true).store(job);

    // Check we are getting back correct type pf split
    splits = inputFormat.getSplits(job);
    for (InputSplit split : splits) {
      assert (split instanceof BatchInputSplit);
    }

    // We should split along the tablet lines
    assertEquals(2, splits.size());
  }

  private void insertData(String tableName, long ts)
      throws AccumuloException, TableNotFoundException {
    BatchWriter bw = client.createBatchWriter(tableName);

    for (int i = 0; i < 10000; i++) {
      String row = String.format("%09d", i);

      Mutation m = new Mutation(new Text(row));
      m.put(new Text("cf1"), new Text("cq1"), ts, new Value("" + i));
      bw.addMutation(m);
    }
    bw.close();
  }

  // track errors in the map reduce job; jobs insert a dummy error for the map and cleanup tasks (to
  // ensure test correctness),
  // so error tests should check to see if there is at least one error (could be more depending on
  // the test) rather than zero
  private static Multimap<String,AssertionError> assertionErrors = ArrayListMultimap.create();

  private static class MRTester extends Configured implements Tool {
    private static class TestMapper extends Mapper<Key,Value,Key,Value> {
      Key key = null;
      int count = 0;

      @Override
      protected void map(Key k, Value v, Context context) {
        String table = context.getConfiguration().get("MRTester_tableName");
        assertNotNull(table);
        try {
          if (key != null) {
            assertEquals(key.getRow().toString(), new String(v.get()));
          }
          assertEquals(k.getRow(), new Text(String.format("%09x", count + 1)));
          assertEquals(new String(v.get()), String.format("%09x", count));
        } catch (AssertionError e) {
          assertionErrors.put(table + "_map", e);
        }
        key = new Key(k);
        count++;
      }

      @Override
      protected void cleanup(Context context) {
        String table = context.getConfiguration().get("MRTester_tableName");
        assertNotNull(table);
        try {
          assertEquals(100, count);
        } catch (AssertionError e) {
          assertionErrors.put(table + "_cleanup", e);
        }
      }
    }

    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 2 && args.length != 4) {
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName()
            + " <table> <inputFormatClass> [<batchScan> <scan sample>]");
      }

      String table = args[0];
      String inputFormatClassName = args[1];
      boolean batchScan = false;
      boolean sample = false;
      if (args.length == 4) {
        batchScan = Boolean.parseBoolean(args[2]);
        sample = Boolean.parseBoolean(args[3]);
      }

      assertionErrors.put(table + "_map", new AssertionError("Dummy_map"));
      assertionErrors.put(table + "_cleanup", new AssertionError("Dummy_cleanup"));

      @SuppressWarnings("unchecked")
      Class<? extends InputFormat<?,?>> inputFormatClass =
          (Class<? extends InputFormat<?,?>>) Class.forName(inputFormatClassName);

      Job job = Job.getInstance(getConf(),
          this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());
      job.getConfiguration().set("MRTester_tableName", table);

      job.setInputFormatClass(inputFormatClass);

      InputFormatOptions<Job> opts = AccumuloInputFormat.configure()
          .clientProperties(getClientProps()).table(table).auths(Authorizations.EMPTY);
      if (sample) {
        opts = opts.samplerConfiguration(SAMPLER_CONFIG);
      }

      opts.batchScan(batchScan).store(job);

      job.setMapperClass(TestMapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormatClass(NullOutputFormat.class);

      job.setNumReduceTasks(0);

      job.waitForCompletion(true);

      return job.isSuccessful() ? 0 : 1;
    }

    public static int main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("mapreduce.framework.name", "local");
      conf.set("mapreduce.cluster.local.dir",
          new File(System.getProperty("user.dir"), "target/mapreduce-tmp").getAbsolutePath());
      return ToolRunner.run(conf, new MRTester(), args);
    }
  }

  @Test
  public void testMap() throws Exception {
    final String TEST_TABLE_1 = getUniqueNames(1)[0];

    client.tableOperations().create(TEST_TABLE_1);
    BatchWriter bw = client.createBatchWriter(TEST_TABLE_1);
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put("", "", String.format("%09x", i));
      bw.addMutation(m);
    }
    bw.close();

    assertEquals(0,
        MRTester.main(new String[] {TEST_TABLE_1, AccumuloInputFormat.class.getName()}));
    assertEquals(1, assertionErrors.get(TEST_TABLE_1 + "_map").size());
    assertEquals(1, assertionErrors.get(TEST_TABLE_1 + "_cleanup").size());
  }

  private static final SamplerConfiguration SAMPLER_CONFIG =
      new SamplerConfiguration(RowSampler.class.getName()).addOption("hasher", "murmur3_32")
          .addOption("modulus", "3");

  @Test
  public void testSample() throws Exception {
    final String TEST_TABLE_3 = getUniqueNames(1)[0];

    client.tableOperations().create(TEST_TABLE_3,
        new NewTableConfiguration().enableSampling(SAMPLER_CONFIG));
    BatchWriter bw = client.createBatchWriter(TEST_TABLE_3);
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put("", "", String.format("%09x", i));
      bw.addMutation(m);
    }
    bw.close();

    assertEquals(0, MRTester
        .main(new String[] {TEST_TABLE_3, AccumuloInputFormat.class.getName(), "False", "True"}));
    assertEquals(39, assertionErrors.get(TEST_TABLE_3 + "_map").size());
    assertEquals(2, assertionErrors.get(TEST_TABLE_3 + "_cleanup").size());

    assertionErrors.clear();
    assertEquals(0, MRTester
        .main(new String[] {TEST_TABLE_3, AccumuloInputFormat.class.getName(), "False", "False"}));
    assertEquals(1, assertionErrors.get(TEST_TABLE_3 + "_map").size());
    assertEquals(1, assertionErrors.get(TEST_TABLE_3 + "_cleanup").size());

    assertionErrors.clear();
    assertEquals(0, MRTester
        .main(new String[] {TEST_TABLE_3, AccumuloInputFormat.class.getName(), "True", "True"}));
    assertEquals(39, assertionErrors.get(TEST_TABLE_3 + "_map").size());
    assertEquals(2, assertionErrors.get(TEST_TABLE_3 + "_cleanup").size());
  }

  @Test
  public void testMapWithBatchScanner() throws Exception {
    final String TEST_TABLE_2 = getUniqueNames(1)[0];

    client.tableOperations().create(TEST_TABLE_2);
    BatchWriter bw = client.createBatchWriter(TEST_TABLE_2);
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put("", "", String.format("%09x", i));
      bw.addMutation(m);
    }
    bw.close();

    assertEquals(0, MRTester
        .main(new String[] {TEST_TABLE_2, AccumuloInputFormat.class.getName(), "True", "False"}));
    assertEquals(1, assertionErrors.get(TEST_TABLE_2 + "_map").size());
    assertEquals(1, assertionErrors.get(TEST_TABLE_2 + "_cleanup").size());
  }

  @Test
  public void testCorrectRangeInputSplits() throws Exception {
    Job job = Job.getInstance();

    String table = getUniqueNames(1)[0];
    Authorizations auths = new Authorizations("foo");
    Collection<IteratorSetting.Column> fetchColumns =
        Collections.singleton(new IteratorSetting.Column(new Text("foo"), new Text("bar")));
    Collection<Pair<Text,Text>> fetchColumnsText =
        Collections.singleton(new Pair<>(new Text("foo"), new Text("bar")));

    client.tableOperations().create(table);

    InputFormatOptions<Job> opts = AccumuloInputFormat.configure()
        .clientProperties(getClientProps()).table(table).auths(auths);
    opts.fetchColumns(fetchColumns).scanIsolation(true).localIterators(true).store(job);

    AccumuloInputFormat aif = new AccumuloInputFormat();

    List<InputSplit> splits = aif.getSplits(job);

    assertEquals(1, splits.size());

    InputSplit split = splits.get(0);

    assertEquals(RangeInputSplit.class, split.getClass());

    RangeInputSplit risplit = (RangeInputSplit) split;

    assertEquals(table, risplit.getTableName());
    assertTrue(risplit.isIsolatedScan());
    assertTrue(risplit.usesLocalIterators());
    assertEquals(fetchColumnsText, risplit.getFetchedColumns());
  }

  @Test
  public void testPartialInputSplitDelegationToConfiguration() throws Exception {
    String table = getUniqueNames(1)[0];
    client.tableOperations().create(table);
    BatchWriter bw = client.createBatchWriter(table);
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put("", "", String.format("%09x", i));
      bw.addMutation(m);
    }
    bw.close();

    assertEquals(0,
        MRTester.main(new String[] {table, EmptySplitsAccumuloInputFormat.class.getName()}));
    assertEquals(1, assertionErrors.get(table + "_map").size());
    assertEquals(1, assertionErrors.get(table + "_cleanup").size());
  }

  /**
   * AccumuloInputFormat which returns an "empty" RangeInputSplit
   */
  public static class EmptySplitsAccumuloInputFormat extends AccumuloInputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
      List<InputSplit> oldSplits = super.getSplits(context);
      List<InputSplit> newSplits = new ArrayList<>(oldSplits.size());

      // Copy only the necessary information
      for (InputSplit oldSplit : oldSplits) {
        RangeInputSplit newSplit = new RangeInputSplit((RangeInputSplit) oldSplit);
        newSplits.add(newSplit);
      }

      return newSplits;
    }
  }
}
