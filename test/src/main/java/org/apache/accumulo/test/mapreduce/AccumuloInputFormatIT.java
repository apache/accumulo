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
package org.apache.accumulo.test.mapreduce;

import static java.lang.System.currentTimeMillis;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
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
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.Pair;
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
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * This tests deprecated mapreduce code in core jar
 */
@Deprecated
public class AccumuloInputFormatIT extends AccumuloClusterHarness {

  org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat inputFormat;

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
  }

  @Before
  public void before() {
    inputFormat = new org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat();
  }

  /**
   * Tests several different paths through the getSplits() method by setting different properties
   * and verifying the results.
   */
  @Test
  public void testGetSplits() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String table = getUniqueNames(1)[0];
      client.tableOperations().create(table);
      insertData(client, table, currentTimeMillis());

      Job job = Job.getInstance();
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setInputTableName(job, table);
      ClientInfo ci = getClientInfo();
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setZooKeeperInstance(job,
          ci.getInstanceName(), ci.getZooKeepers());
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setConnectorInfo(job,
          ci.getPrincipal(), ci.getAuthenticationToken());

      // split table
      TreeSet<Text> splitsToAdd = new TreeSet<>();
      for (int i = 0; i < 10000; i += 1000)
        splitsToAdd.add(new Text(String.format("%09d", i)));
      client.tableOperations().addSplits(table, splitsToAdd);
      sleepUninterruptibly(500, TimeUnit.MILLISECONDS); // wait for splits to be propagated

      // get splits without setting any range
      Collection<Text> actualSplits = client.tableOperations().listSplits(table);
      List<InputSplit> splits = inputFormat.getSplits(job);
      // No ranges set on the job so it'll start with -inf
      assertEquals(actualSplits.size() + 1, splits.size());

      // set ranges and get splits
      List<Range> ranges = new ArrayList<>();
      for (Text text : actualSplits)
        ranges.add(new Range(text));
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setRanges(job, ranges);
      splits = inputFormat.getSplits(job);
      assertEquals(actualSplits.size(), splits.size());

      // offline mode
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setOfflineTableScan(job, true);
      try {
        inputFormat.getSplits(job);
        fail("An exception should have been thrown");
      } catch (IOException e) {}

      client.tableOperations().offline(table, true);
      splits = inputFormat.getSplits(job);
      assertEquals(actualSplits.size(), splits.size());

      // auto adjust ranges
      ranges = new ArrayList<>();
      for (int i = 0; i < 5; i++)
        // overlapping ranges
        ranges.add(new Range(String.format("%09d", i), String.format("%09d", i + 2)));
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setRanges(job, ranges);
      splits = inputFormat.getSplits(job);
      assertEquals(2, splits.size());

      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setAutoAdjustRanges(job, false);
      splits = inputFormat.getSplits(job);
      assertEquals(ranges.size(), splits.size());

      // BatchScan not available for offline scans
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setBatchScan(job, true);
      // Reset auto-adjust ranges too
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setAutoAdjustRanges(job, true);

      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setOfflineTableScan(job, true);
      try {
        inputFormat.getSplits(job);
        fail("An exception should have been thrown");
      } catch (IllegalArgumentException e) {}

      client.tableOperations().online(table, true);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setOfflineTableScan(job, false);

      // test for resumption of success
      splits = inputFormat.getSplits(job);
      assertEquals(2, splits.size());

      // BatchScan not available with isolated iterators
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setScanIsolation(job, true);
      try {
        inputFormat.getSplits(job);
        fail("An exception should have been thrown");
      } catch (IllegalArgumentException e) {}
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setScanIsolation(job, false);

      // test for resumption of success
      splits = inputFormat.getSplits(job);
      assertEquals(2, splits.size());

      // BatchScan not available with local iterators
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setLocalIterators(job, true);
      try {
        inputFormat.getSplits(job);
        fail("An exception should have been thrown");
      } catch (IllegalArgumentException e) {}
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setLocalIterators(job, false);

      // Check we are getting back correct type pf split
      client.tableOperations().online(table);
      splits = inputFormat.getSplits(job);
      for (InputSplit split : splits)
        assert (split instanceof org.apache.accumulo.core.clientImpl.mapreduce.BatchInputSplit);

      // We should divide along the tablet lines similar to when using `setAutoAdjustRanges(job,
      // true)`
      assertEquals(2, splits.size());
    }
  }

  private void insertData(AccumuloClient client, String tableName, long ts)
      throws AccumuloException, TableNotFoundException {
    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      for (int i = 0; i < 10000; i++) {
        String row = String.format("%09d", i);
        Mutation m = new Mutation(new Text(row));
        m.put(new Text("cf1"), new Text("cq1"), ts, new Value("" + i));
        bw.addMutation(m);
      }
    }
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
          if (key != null)
            assertEquals(key.getRow().toString(), new String(v.get()));
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

      ClientInfo ci = getClientInfo();
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setZooKeeperInstance(job,
          ci.getInstanceName(), ci.getZooKeepers());
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setConnectorInfo(job,
          ci.getPrincipal(), ci.getAuthenticationToken());
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setInputTableName(job, table);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setBatchScan(job, batchScan);
      if (sample) {
        org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setSamplerConfiguration(job,
            SAMPLER_CONFIG);
      }

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

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(TEST_TABLE_1);
      AccumuloOutputFormatIT.insertData(c, TEST_TABLE_1);
      assertEquals(0, MRTester.main(new String[] {TEST_TABLE_1,
          org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.class.getName()}));
      assertEquals(1, assertionErrors.get(TEST_TABLE_1 + "_map").size());
      assertEquals(1, assertionErrors.get(TEST_TABLE_1 + "_cleanup").size());
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
      AccumuloOutputFormatIT.insertData(c, TEST_TABLE_3);
      assertEquals(0,
          MRTester.main(new String[] {TEST_TABLE_3,
              org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.class.getName(),
              "False", "True"}));
      assertEquals(39, assertionErrors.get(TEST_TABLE_3 + "_map").size());
      assertEquals(2, assertionErrors.get(TEST_TABLE_3 + "_cleanup").size());

      assertionErrors.clear();
      assertEquals(0,
          MRTester.main(new String[] {TEST_TABLE_3,
              org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.class.getName(),
              "False", "False"}));
      assertEquals(1, assertionErrors.get(TEST_TABLE_3 + "_map").size());
      assertEquals(1, assertionErrors.get(TEST_TABLE_3 + "_cleanup").size());

      assertionErrors.clear();
      assertEquals(0,
          MRTester.main(new String[] {TEST_TABLE_3,
              org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.class.getName(), "True",
              "True"}));
      assertEquals(39, assertionErrors.get(TEST_TABLE_3 + "_map").size());
      assertEquals(2, assertionErrors.get(TEST_TABLE_3 + "_cleanup").size());
    }
  }

  @Test
  public void testMapWithBatchScanner() throws Exception {
    final String TEST_TABLE_2 = getUniqueNames(1)[0];

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(TEST_TABLE_2);
      AccumuloOutputFormatIT.insertData(c, TEST_TABLE_2);
      assertEquals(0,
          MRTester.main(new String[] {TEST_TABLE_2,
              org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.class.getName(), "True",
              "False"}));
      assertEquals(1, assertionErrors.get(TEST_TABLE_2 + "_map").size());
      assertEquals(1, assertionErrors.get(TEST_TABLE_2 + "_cleanup").size());
    }
  }

  @Test
  public void testCorrectRangeInputSplits() throws Exception {
    Job job = Job.getInstance();

    String table = getUniqueNames(1)[0];
    Authorizations auths = new Authorizations("foo");
    Collection<Pair<Text,Text>> fetchColumns =
        Collections.singleton(new Pair<>(new Text("foo"), new Text("bar")));
    boolean isolated = true, localIters = true;
    Level level = Level.WARN;

    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProps()).build()) {
      accumuloClient.tableOperations().create(table);

      ClientInfo ci = getClientInfo();
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setZooKeeperInstance(job,
          ci.getInstanceName(), ci.getZooKeepers());
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setConnectorInfo(job,
          ci.getPrincipal(), ci.getAuthenticationToken());
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setInputTableName(job, table);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setScanAuthorizations(job,
          auths);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setScanIsolation(job, isolated);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setLocalIterators(job,
          localIters);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.fetchColumns(job, fetchColumns);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setLogLevel(job, level);

      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat aif =
          new org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat();

      List<InputSplit> splits = aif.getSplits(job);

      assertEquals(1, splits.size());

      InputSplit split = splits.get(0);

      assertEquals(org.apache.accumulo.core.client.mapreduce.RangeInputSplit.class,
          split.getClass());

      org.apache.accumulo.core.client.mapreduce.RangeInputSplit risplit =
          (org.apache.accumulo.core.client.mapreduce.RangeInputSplit) split;

      assertEquals(table, risplit.getTableName());
      assertEquals(isolated, risplit.isIsolatedScan());
      assertEquals(localIters, risplit.usesLocalIterators());
      assertEquals(fetchColumns, risplit.getFetchedColumns());
      assertEquals(level, risplit.getLogLevel());
    }
  }

  @Test(expected = IOException.class)
  public void testGetSplitsNoReadPermission() throws Exception {
    Job job = Job.getInstance();

    String table = getUniqueNames(1)[0];
    Authorizations auths = new Authorizations("foo");
    Collection<Pair<Text,Text>> fetchColumns =
        Collections.singleton(new Pair<>(new Text("foo"), new Text("bar")));
    boolean isolated = true, localIters = true;
    Level level = Level.WARN;

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);
      client.securityOperations().revokeTablePermission(client.whoami(), table,
          TablePermission.READ);

      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setZooKeeperInstance(job,
          cluster.getClientConfig());
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setConnectorInfo(job,
          getAdminPrincipal(), getAdminToken());

      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setInputTableName(job, table);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setScanAuthorizations(job,
          auths);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setScanIsolation(job, isolated);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setLocalIterators(job,
          localIters);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.fetchColumns(job, fetchColumns);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setLogLevel(job, level);

      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat aif =
          new org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat();

      aif.getSplits(job);
    }
  }

  /*
   * This tests the case where we do not have Table.READ permission, but we do have Namespace.READ.
   * See issue #1370.
   */
  @Test
  public void testGetSplitsWithNamespaceReadPermission() throws Exception {
    Job job = Job.getInstance();

    final String[] namespaceAndTable = getUniqueNames(2);
    final String namespace = namespaceAndTable[0];
    final String tableSimpleName = namespaceAndTable[1];
    final String table = namespace + "." + tableSimpleName;
    Authorizations auths = new Authorizations("foo");
    Collection<Pair<Text,Text>> fetchColumns =
        Collections.singleton(new Pair<>(new Text("foo"), new Text("bar")));
    final boolean isolated = true;
    final boolean localIters = true;
    Level level = Level.WARN;

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.namespaceOperations().create(namespace); // creating namespace implies Namespace.READ
      client.tableOperations().create(table);
      client.securityOperations().revokeTablePermission(client.whoami(), table,
          TablePermission.READ);

      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setZooKeeperInstance(job,
          cluster.getClientConfig());
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setConnectorInfo(job,
          getAdminPrincipal(), getAdminToken());

      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setInputTableName(job, table);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setScanAuthorizations(job,
          auths);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setScanIsolation(job, isolated);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setLocalIterators(job,
          localIters);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.fetchColumns(job, fetchColumns);
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setLogLevel(job, level);

      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat aif =
          new org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat();

      List<InputSplit> splits = aif.getSplits(job);

      assertEquals(1, splits.size());
    }
  }

  @Test
  public void testPartialInputSplitDelegationToConfiguration() throws Exception {
    String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(table);
      AccumuloOutputFormatIT.insertData(c, table);
      assertEquals(0,
          MRTester.main(new String[] {table, EmptySplitsAccumuloInputFormat.class.getName()}));
      assertEquals(1, assertionErrors.get(table + "_map").size());
      assertEquals(1, assertionErrors.get(table + "_cleanup").size());
    }
  }

  /**
   * AccumuloInputFormat which returns an "empty" RangeInputSplit
   */
  public static class EmptySplitsAccumuloInputFormat
      extends org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
      List<InputSplit> oldSplits = super.getSplits(context);
      List<InputSplit> newSplits = new ArrayList<>(oldSplits.size());

      // Copy only the necessary information
      for (InputSplit oldSplit : oldSplits) {
        org.apache.accumulo.core.client.mapreduce.RangeInputSplit newSplit =
            new org.apache.accumulo.core.client.mapreduce.RangeInputSplit(
                (org.apache.accumulo.core.client.mapreduce.RangeInputSplit) oldSplit);
        newSplits.add(newSplit);
      }

      return newSplits;
    }
  }
}
