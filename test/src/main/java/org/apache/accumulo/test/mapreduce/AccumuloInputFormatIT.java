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
package org.apache.accumulo.test.mapreduce;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.System.currentTimeMillis;
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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;
import org.apache.accumulo.core.client.mapreduce.impl.BatchInputSplit;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class AccumuloInputFormatIT extends AccumuloClusterHarness {

  AccumuloInputFormat inputFormat;

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
    inputFormat = new AccumuloInputFormat();
  }

  /**
   * Tests several different paths through the getSplits() method by setting different properties and verifying the results.
   */
  @Test
  public void testGetSplits() throws Exception {
    Connector conn = getConnector();
    String table = getUniqueNames(1)[0];
    conn.tableOperations().create(table);
    insertData(table, currentTimeMillis());

    ClientConfiguration clientConf = cluster.getClientConfig();
    AccumuloConfiguration clusterClientConf = new ConfigurationCopy(DefaultConfiguration.getInstance());

    // Pass SSL and CredentialProvider options into the ClientConfiguration given to AccumuloInputFormat
    boolean sslEnabled = Boolean.valueOf(clusterClientConf.get(Property.INSTANCE_RPC_SSL_ENABLED));
    if (sslEnabled) {
      ClientProperty[] sslProperties = new ClientProperty[] {ClientProperty.INSTANCE_RPC_SSL_ENABLED, ClientProperty.INSTANCE_RPC_SSL_CLIENT_AUTH,
          ClientProperty.RPC_SSL_KEYSTORE_PATH, ClientProperty.RPC_SSL_KEYSTORE_TYPE, ClientProperty.RPC_SSL_KEYSTORE_PASSWORD,
          ClientProperty.RPC_SSL_TRUSTSTORE_PATH, ClientProperty.RPC_SSL_TRUSTSTORE_TYPE, ClientProperty.RPC_SSL_TRUSTSTORE_PASSWORD,
          ClientProperty.RPC_USE_JSSE, ClientProperty.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS};

      for (ClientProperty prop : sslProperties) {
        // The default property is returned if it's not in the ClientConfiguration so we don't have to check if the value is actually defined
        clientConf.setProperty(prop, clusterClientConf.get(prop.getKey()));
      }
    }

    Job job = Job.getInstance();
    AccumuloInputFormat.setInputTableName(job, table);
    AccumuloInputFormat.setZooKeeperInstance(job, clientConf);
    AccumuloInputFormat.setConnectorInfo(job, getAdminPrincipal(), getAdminToken());

    // split table
    TreeSet<Text> splitsToAdd = new TreeSet<>();
    for (int i = 0; i < 10000; i += 1000)
      splitsToAdd.add(new Text(String.format("%09d", i)));
    conn.tableOperations().addSplits(table, splitsToAdd);
    sleepUninterruptibly(500, TimeUnit.MILLISECONDS); // wait for splits to be propagated

    // get splits without setting any range
    Collection<Text> actualSplits = conn.tableOperations().listSplits(table);
    List<InputSplit> splits = inputFormat.getSplits(job);
    assertEquals(actualSplits.size() + 1, splits.size()); // No ranges set on the job so it'll start with -inf

    // set ranges and get splits
    List<Range> ranges = new ArrayList<>();
    for (Text text : actualSplits)
      ranges.add(new Range(text));
    AccumuloInputFormat.setRanges(job, ranges);
    splits = inputFormat.getSplits(job);
    assertEquals(actualSplits.size(), splits.size());

    // offline mode
    AccumuloInputFormat.setOfflineTableScan(job, true);
    try {
      inputFormat.getSplits(job);
      fail("An exception should have been thrown");
    } catch (IOException e) {}

    conn.tableOperations().offline(table, true);
    splits = inputFormat.getSplits(job);
    assertEquals(actualSplits.size(), splits.size());

    // auto adjust ranges
    ranges = new ArrayList<>();
    for (int i = 0; i < 5; i++)
      // overlapping ranges
      ranges.add(new Range(String.format("%09d", i), String.format("%09d", i + 2)));
    AccumuloInputFormat.setRanges(job, ranges);
    splits = inputFormat.getSplits(job);
    assertEquals(2, splits.size());

    AccumuloInputFormat.setAutoAdjustRanges(job, false);
    splits = inputFormat.getSplits(job);
    assertEquals(ranges.size(), splits.size());

    // BatchScan not available for offline scans
    AccumuloInputFormat.setBatchScan(job, true);
    // Reset auto-adjust ranges too
    AccumuloInputFormat.setAutoAdjustRanges(job, true);

    AccumuloInputFormat.setOfflineTableScan(job, true);
    try {
      inputFormat.getSplits(job);
      fail("An exception should have been thrown");
    } catch (IllegalArgumentException e) {}

    conn.tableOperations().online(table, true);
    AccumuloInputFormat.setOfflineTableScan(job, false);

    // test for resumption of success
    splits = inputFormat.getSplits(job);
    assertEquals(2, splits.size());

    // BatchScan not available with isolated iterators
    AccumuloInputFormat.setScanIsolation(job, true);
    try {
      inputFormat.getSplits(job);
      fail("An exception should have been thrown");
    } catch (IllegalArgumentException e) {}
    AccumuloInputFormat.setScanIsolation(job, false);

    // test for resumption of success
    splits = inputFormat.getSplits(job);
    assertEquals(2, splits.size());

    // BatchScan not available with local iterators
    AccumuloInputFormat.setLocalIterators(job, true);
    try {
      inputFormat.getSplits(job);
      fail("An exception should have been thrown");
    } catch (IllegalArgumentException e) {}
    AccumuloInputFormat.setLocalIterators(job, false);

    // Check we are getting back correct type pf split
    conn.tableOperations().online(table);
    splits = inputFormat.getSplits(job);
    for (InputSplit split : splits)
      assert (split instanceof BatchInputSplit);

    // We should divide along the tablet lines similar to when using `setAutoAdjustRanges(job, true)`
    assertEquals(2, splits.size());
  }

  private void insertData(String tableName, long ts) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    BatchWriter bw = getConnector().createBatchWriter(tableName, null);

    for (int i = 0; i < 10000; i++) {
      String row = String.format("%09d", i);

      Mutation m = new Mutation(new Text(row));
      m.put(new Text("cf1"), new Text("cq1"), ts, new Value(("" + i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();
  }

  // track errors in the map reduce job; jobs insert a dummy error for the map and cleanup tasks (to ensure test correctness),
  // so error tests should check to see if there is at least one error (could be more depending on the test) rather than zero
  private static Multimap<String,AssertionError> assertionErrors = ArrayListMultimap.create();

  private static class MRTester extends Configured implements Tool {
    private static class TestMapper extends Mapper<Key,Value,Key,Value> {
      Key key = null;
      int count = 0;

      @Override
      protected void map(Key k, Value v, Context context) throws IOException, InterruptedException {
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
      protected void cleanup(Context context) throws IOException, InterruptedException {
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
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName() + " <table> <inputFormatClass> [<batchScan> <scan sample>]");
      }

      String table = args[0];
      String inputFormatClassName = args[1];
      Boolean batchScan = false;
      boolean sample = false;
      if (args.length == 4) {
        batchScan = Boolean.parseBoolean(args[2]);
        sample = Boolean.parseBoolean(args[3]);
      }

      assertionErrors.put(table + "_map", new AssertionError("Dummy_map"));
      assertionErrors.put(table + "_cleanup", new AssertionError("Dummy_cleanup"));

      @SuppressWarnings("unchecked")
      Class<? extends InputFormat<?,?>> inputFormatClass = (Class<? extends InputFormat<?,?>>) Class.forName(inputFormatClassName);

      Job job = Job.getInstance(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());
      job.getConfiguration().set("MRTester_tableName", table);

      job.setInputFormatClass(inputFormatClass);

      AccumuloInputFormat.setZooKeeperInstance(job, cluster.getClientConfig());
      AccumuloInputFormat.setConnectorInfo(job, getAdminPrincipal(), getAdminToken());
      AccumuloInputFormat.setInputTableName(job, table);
      AccumuloInputFormat.setBatchScan(job, batchScan);
      if (sample) {
        AccumuloInputFormat.setSamplerConfiguration(job, SAMPLER_CONFIG);
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
      conf.set("mapreduce.cluster.local.dir", new File(System.getProperty("user.dir"), "target/mapreduce-tmp").getAbsolutePath());
      return ToolRunner.run(conf, new MRTester(), args);
    }
  }

  @Test
  public void testMap() throws Exception {
    final String TEST_TABLE_1 = getUniqueNames(1)[0];

    Connector c = getConnector();
    c.tableOperations().create(TEST_TABLE_1);
    BatchWriter bw = c.createBatchWriter(TEST_TABLE_1, new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();

    Assert.assertEquals(0, MRTester.main(new String[] {TEST_TABLE_1, AccumuloInputFormat.class.getName()}));
    assertEquals(1, assertionErrors.get(TEST_TABLE_1 + "_map").size());
    assertEquals(1, assertionErrors.get(TEST_TABLE_1 + "_cleanup").size());
  }

  private static final SamplerConfiguration SAMPLER_CONFIG = new SamplerConfiguration(RowSampler.class.getName()).addOption("hasher", "murmur3_32").addOption(
      "modulus", "3");

  @Test
  public void testSample() throws Exception {
    final String TEST_TABLE_3 = getUniqueNames(1)[0];

    Connector c = getConnector();
    c.tableOperations().create(TEST_TABLE_3, new NewTableConfiguration().enableSampling(SAMPLER_CONFIG));
    BatchWriter bw = c.createBatchWriter(TEST_TABLE_3, new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();

    Assert.assertEquals(0, MRTester.main(new String[] {TEST_TABLE_3, AccumuloInputFormat.class.getName(), "False", "True"}));
    assertEquals(39, assertionErrors.get(TEST_TABLE_3 + "_map").size());
    assertEquals(2, assertionErrors.get(TEST_TABLE_3 + "_cleanup").size());

    assertionErrors.clear();
    Assert.assertEquals(0, MRTester.main(new String[] {TEST_TABLE_3, AccumuloInputFormat.class.getName(), "False", "False"}));
    assertEquals(1, assertionErrors.get(TEST_TABLE_3 + "_map").size());
    assertEquals(1, assertionErrors.get(TEST_TABLE_3 + "_cleanup").size());

    assertionErrors.clear();
    Assert.assertEquals(0, MRTester.main(new String[] {TEST_TABLE_3, AccumuloInputFormat.class.getName(), "True", "True"}));
    assertEquals(39, assertionErrors.get(TEST_TABLE_3 + "_map").size());
    assertEquals(2, assertionErrors.get(TEST_TABLE_3 + "_cleanup").size());
  }

  @Test
  public void testMapWithBatchScanner() throws Exception {
    final String TEST_TABLE_2 = getUniqueNames(1)[0];

    Connector c = getConnector();
    c.tableOperations().create(TEST_TABLE_2);
    BatchWriter bw = c.createBatchWriter(TEST_TABLE_2, new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();

    Assert.assertEquals(0, MRTester.main(new String[] {TEST_TABLE_2, AccumuloInputFormat.class.getName(), "True", "False"}));
    assertEquals(1, assertionErrors.get(TEST_TABLE_2 + "_map").size());
    assertEquals(1, assertionErrors.get(TEST_TABLE_2 + "_cleanup").size());
  }

  @Test
  public void testCorrectRangeInputSplits() throws Exception {
    Job job = Job.getInstance();

    String table = getUniqueNames(1)[0];
    Authorizations auths = new Authorizations("foo");
    Collection<Pair<Text,Text>> fetchColumns = Collections.singleton(new Pair<>(new Text("foo"), new Text("bar")));
    boolean isolated = true, localIters = true;
    Level level = Level.WARN;

    Connector connector = getConnector();
    connector.tableOperations().create(table);

    AccumuloInputFormat.setZooKeeperInstance(job, cluster.getClientConfig());
    AccumuloInputFormat.setConnectorInfo(job, getAdminPrincipal(), getAdminToken());

    AccumuloInputFormat.setInputTableName(job, table);
    AccumuloInputFormat.setScanAuthorizations(job, auths);
    AccumuloInputFormat.setScanIsolation(job, isolated);
    AccumuloInputFormat.setLocalIterators(job, localIters);
    AccumuloInputFormat.fetchColumns(job, fetchColumns);
    AccumuloInputFormat.setLogLevel(job, level);

    AccumuloInputFormat aif = new AccumuloInputFormat();

    List<InputSplit> splits = aif.getSplits(job);

    Assert.assertEquals(1, splits.size());

    InputSplit split = splits.get(0);

    Assert.assertEquals(RangeInputSplit.class, split.getClass());

    RangeInputSplit risplit = (RangeInputSplit) split;

    Assert.assertEquals(getAdminPrincipal(), risplit.getPrincipal());
    Assert.assertEquals(table, risplit.getTableName());
    Assert.assertEquals(getAdminToken(), risplit.getToken());
    Assert.assertEquals(auths, risplit.getAuths());
    Assert.assertEquals(getConnector().getInstance().getInstanceName(), risplit.getInstanceName());
    Assert.assertEquals(isolated, risplit.isIsolatedScan());
    Assert.assertEquals(localIters, risplit.usesLocalIterators());
    Assert.assertEquals(fetchColumns, risplit.getFetchedColumns());
    Assert.assertEquals(level, risplit.getLogLevel());
  }

  @Test
  public void testPartialInputSplitDelegationToConfiguration() throws Exception {
    String table = getUniqueNames(1)[0];
    Connector c = getConnector();
    c.tableOperations().create(table);
    BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();

    Assert.assertEquals(0, MRTester.main(new String[] {table, EmptySplitsAccumuloInputFormat.class.getName()}));
    assertEquals(1, assertionErrors.get(table + "_map").size());
    assertEquals(1, assertionErrors.get(table + "_cleanup").size());
  }

  @Test
  public void testPartialFailedInputSplitDelegationToConfiguration() throws Exception {
    String table = getUniqueNames(1)[0];
    Connector c = getConnector();
    c.tableOperations().create(table);
    BatchWriter bw = c.createBatchWriter(table, new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();

    Assert.assertEquals(1, MRTester.main(new String[] {table, BadPasswordSplitsAccumuloInputFormat.class.getName()}));
    assertEquals(1, assertionErrors.get(table + "_map").size());
    // We should fail when the RecordReader fails to get the next key/value pair, because the record reader is set up with a clientcontext, rather than a
    // connector, so it doesn't do fast-fail on bad credentials
    assertEquals(2, assertionErrors.get(table + "_cleanup").size());
  }

  /**
   * AccumuloInputFormat which returns an "empty" RangeInputSplit
   */
  public static class BadPasswordSplitsAccumuloInputFormat extends AccumuloInputFormat {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
      List<InputSplit> splits = super.getSplits(context);

      for (InputSplit split : splits) {
        org.apache.accumulo.core.client.mapreduce.RangeInputSplit rangeSplit = (org.apache.accumulo.core.client.mapreduce.RangeInputSplit) split;
        rangeSplit.setToken(new PasswordToken("anythingelse"));
      }

      return splits;
    }
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
        org.apache.accumulo.core.client.mapreduce.RangeInputSplit newSplit = new org.apache.accumulo.core.client.mapreduce.RangeInputSplit(
            (org.apache.accumulo.core.client.mapreduce.RangeInputSplit) oldSplit);
        newSplits.add(newSplit);
      }

      return newSplits;
    }
  }
}
