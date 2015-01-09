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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

public class AccumuloInputFormatTest {

  private static final String PREFIX = AccumuloInputFormatTest.class.getSimpleName();

  /**
   * Check that the iterator configuration is getting stored in the Job conf correctly.
   */
  @Test
  public void testSetIterator() throws IOException {
    @SuppressWarnings("deprecation")
    Job job = new Job();

    IteratorSetting is = new IteratorSetting(1, "WholeRow", "org.apache.accumulo.core.iterators.WholeRowIterator");
    AccumuloInputFormat.addIterator(job, is);
    Configuration conf = job.getConfiguration();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    is.write(new DataOutputStream(baos));
    String iterators = conf.get("AccumuloInputFormat.ScanOpts.Iterators");
    assertEquals(Base64.encodeBase64String(baos.toByteArray()), iterators);
  }

  @Test
  public void testAddIterator() throws IOException {
    @SuppressWarnings("deprecation")
    Job job = new Job();

    AccumuloInputFormat.addIterator(job, new IteratorSetting(1, "WholeRow", WholeRowIterator.class));
    AccumuloInputFormat.addIterator(job, new IteratorSetting(2, "Versions", "org.apache.accumulo.core.iterators.VersioningIterator"));
    IteratorSetting iter = new IteratorSetting(3, "Count", "org.apache.accumulo.core.iterators.CountingIterator");
    iter.addOption("v1", "1");
    iter.addOption("junk", "\0omg:!\\xyzzy");
    AccumuloInputFormat.addIterator(job, iter);

    List<IteratorSetting> list = AccumuloInputFormat.getIterators(job);

    // Check the list size
    assertTrue(list.size() == 3);

    // Walk the list and make sure our settings are correct
    IteratorSetting setting = list.get(0);
    assertEquals(1, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.user.WholeRowIterator", setting.getIteratorClass());
    assertEquals("WholeRow", setting.getName());
    assertEquals(0, setting.getOptions().size());

    setting = list.get(1);
    assertEquals(2, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.VersioningIterator", setting.getIteratorClass());
    assertEquals("Versions", setting.getName());
    assertEquals(0, setting.getOptions().size());

    setting = list.get(2);
    assertEquals(3, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.CountingIterator", setting.getIteratorClass());
    assertEquals("Count", setting.getName());
    assertEquals(2, setting.getOptions().size());
    assertEquals("1", setting.getOptions().get("v1"));
    assertEquals("\0omg:!\\xyzzy", setting.getOptions().get("junk"));
  }

  /**
   * Test adding iterator options where the keys and values contain both the FIELD_SEPARATOR character (':') and ITERATOR_SEPARATOR (',') characters. There
   * should be no exceptions thrown when trying to parse these types of option entries.
   *
   * This test makes sure that the expected raw values, as appears in the Job, are equal to what's expected.
   */
  @Test
  public void testIteratorOptionEncoding() throws Throwable {
    String key = "colon:delimited:key";
    String value = "comma,delimited,value";
    IteratorSetting someSetting = new IteratorSetting(1, "iterator", "Iterator.class");
    someSetting.addOption(key, value);
    @SuppressWarnings("deprecation")
    Job job = new Job();
    AccumuloInputFormat.addIterator(job, someSetting);

    List<IteratorSetting> list = AccumuloInputFormat.getIterators(job);
    assertEquals(1, list.size());
    assertEquals(1, list.get(0).getOptions().size());
    assertEquals(list.get(0).getOptions().get(key), value);

    someSetting.addOption(key + "2", value);
    someSetting.setPriority(2);
    someSetting.setName("it2");
    AccumuloInputFormat.addIterator(job, someSetting);
    list = AccumuloInputFormat.getIterators(job);
    assertEquals(2, list.size());
    assertEquals(1, list.get(0).getOptions().size());
    assertEquals(list.get(0).getOptions().get(key), value);
    assertEquals(2, list.get(1).getOptions().size());
    assertEquals(list.get(1).getOptions().get(key), value);
    assertEquals(list.get(1).getOptions().get(key + "2"), value);
  }

  /**
   * Test getting iterator settings for multiple iterators set
   */
  @Test
  public void testGetIteratorSettings() throws IOException {
    @SuppressWarnings("deprecation")
    Job job = new Job();

    AccumuloInputFormat.addIterator(job, new IteratorSetting(1, "WholeRow", "org.apache.accumulo.core.iterators.WholeRowIterator"));
    AccumuloInputFormat.addIterator(job, new IteratorSetting(2, "Versions", "org.apache.accumulo.core.iterators.VersioningIterator"));
    AccumuloInputFormat.addIterator(job, new IteratorSetting(3, "Count", "org.apache.accumulo.core.iterators.CountingIterator"));

    List<IteratorSetting> list = AccumuloInputFormat.getIterators(job);

    // Check the list size
    assertTrue(list.size() == 3);

    // Walk the list and make sure our settings are correct
    IteratorSetting setting = list.get(0);
    assertEquals(1, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.WholeRowIterator", setting.getIteratorClass());
    assertEquals("WholeRow", setting.getName());

    setting = list.get(1);
    assertEquals(2, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.VersioningIterator", setting.getIteratorClass());
    assertEquals("Versions", setting.getName());

    setting = list.get(2);
    assertEquals(3, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.CountingIterator", setting.getIteratorClass());
    assertEquals("Count", setting.getName());

  }

  @Test
  public void testSetRegex() throws IOException {
    @SuppressWarnings("deprecation")
    Job job = new Job();

    String regex = ">\"*%<>\'\\";

    IteratorSetting is = new IteratorSetting(50, regex, RegExFilter.class);
    RegExFilter.setRegexs(is, regex, null, null, null, false);
    AccumuloInputFormat.addIterator(job, is);

    assertTrue(regex.equals(AccumuloInputFormat.getIterators(job).get(0).getName()));
  }

  private static AssertionError e1 = null;
  private static AssertionError e2 = null;

  private static class MRTester extends Configured implements Tool {
    private static class TestMapper extends Mapper<Key,Value,Key,Value> {
      Key key = null;
      int count = 0;

      @Override
      protected void map(Key k, Value v, Context context) throws IOException, InterruptedException {
        try {
          if (key != null)
            assertEquals(key.getRow().toString(), new String(v.get()));
          assertEquals(k.getRow(), new Text(String.format("%09x", count + 1)));
          assertEquals(new String(v.get()), String.format("%09x", count));
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

      if (args.length != 5) {
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName() + " <user> <pass> <table> <instanceName> <inputFormatClass>");
      }

      String user = args[0];
      String pass = args[1];
      String table = args[2];

      String instanceName = args[3];
      String inputFormatClassName = args[4];
      @SuppressWarnings("unchecked")
      Class<? extends InputFormat<?,?>> inputFormatClass = (Class<? extends InputFormat<?,?>>) Class.forName(inputFormatClassName);

      @SuppressWarnings("deprecation")
      Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());

      job.setInputFormatClass(inputFormatClass);

      AccumuloInputFormat.setConnectorInfo(job, user, new PasswordToken(pass));
      AccumuloInputFormat.setInputTableName(job, table);
      AccumuloInputFormat.setMockInstance(job, instanceName);

      job.setMapperClass(TestMapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormatClass(NullOutputFormat.class);

      job.setNumReduceTasks(0);

      job.waitForCompletion(true);

      return job.isSuccessful() ? 0 : 1;
    }

    public static int main(String[] args) throws Exception {
      return ToolRunner.run(CachedConfiguration.getInstance(), new MRTester(), args);
    }
  }

  @Test
  public void testMap() throws Exception {
    final String INSTANCE_NAME = PREFIX + "_mapreduce_instance";
    final String TEST_TABLE_1 = PREFIX + "_mapreduce_table_1";

    MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
    Connector c = mockInstance.getConnector("root", new PasswordToken(""));
    c.tableOperations().create(TEST_TABLE_1);
    BatchWriter bw = c.createBatchWriter(TEST_TABLE_1, new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();

    Assert.assertEquals(0, MRTester.main(new String[] {"root", "", TEST_TABLE_1, INSTANCE_NAME, AccumuloInputFormat.class.getCanonicalName()}));
    assertNull(e1);
    assertNull(e2);
  }

  @Test
  public void testCorrectRangeInputSplits() throws Exception {
    @SuppressWarnings("deprecation")
    Job job = new Job(new Configuration(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());

    String username = "user", table = "table", instance = "mapreduce_testCorrectRangeInputSplits";
    PasswordToken password = new PasswordToken("password");
    Authorizations auths = new Authorizations("foo");
    Collection<Pair<Text,Text>> fetchColumns = Collections.singleton(new Pair<Text,Text>(new Text("foo"), new Text("bar")));
    boolean isolated = true, localIters = true;
    Level level = Level.WARN;

    Instance inst = new MockInstance(instance);
    Connector connector = inst.getConnector(username, password);
    connector.tableOperations().create(table);

    AccumuloInputFormat.setConnectorInfo(job, username, password);
    AccumuloInputFormat.setInputTableName(job, table);
    AccumuloInputFormat.setScanAuthorizations(job, auths);
    AccumuloInputFormat.setMockInstance(job, instance);
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

    Assert.assertEquals(username, risplit.getPrincipal());
    Assert.assertEquals(table, risplit.getTableName());
    Assert.assertEquals(password, risplit.getToken());
    Assert.assertEquals(auths, risplit.getAuths());
    Assert.assertEquals(instance, risplit.getInstanceName());
    Assert.assertEquals(isolated, risplit.isIsolatedScan());
    Assert.assertEquals(localIters, risplit.usesLocalIterators());
    Assert.assertEquals(fetchColumns, risplit.getFetchedColumns());
    Assert.assertEquals(level, risplit.getLogLevel());
  }

  @Test
  public void testPartialInputSplitDelegationToConfiguration() throws Exception {
    String user = "testPartialInputSplitUser";
    PasswordToken password = new PasswordToken("");

    MockInstance mockInstance = new MockInstance("testPartialInputSplitDelegationToConfiguration");
    Connector c = mockInstance.getConnector(user, password);
    c.tableOperations().create("testtable");
    BatchWriter bw = c.createBatchWriter("testtable", new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();

    Assert.assertEquals(
        0,
        MRTester.main(new String[] {user, "", "testtable", "testPartialInputSplitDelegationToConfiguration",
            EmptySplitsAccumuloInputFormat.class.getCanonicalName()}));
    assertNull(e1);
    assertNull(e2);
  }

  @Test
  public void testPartialFailedInputSplitDelegationToConfiguration() throws Exception {
    String user = "testPartialFailedInputSplit";
    PasswordToken password = new PasswordToken("");

    MockInstance mockInstance = new MockInstance("testPartialFailedInputSplitDelegationToConfiguration");
    Connector c = mockInstance.getConnector(user, password);
    c.tableOperations().create("testtable");
    BatchWriter bw = c.createBatchWriter("testtable", new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();

    // We should fail before we even get into the Mapper because we can't make the RecordReader
    Assert.assertEquals(
        1,
        MRTester.main(new String[] {user, "", "testtable", "testPartialFailedInputSplitDelegationToConfiguration",
            BadPasswordSplitsAccumuloInputFormat.class.getCanonicalName()}));
    assertNull(e1);
    assertNull(e2);
  }

  @Test
  public void testEmptyColumnFamily() throws IOException {
    @SuppressWarnings("deprecation")
    Job job = new Job();
    Set<Pair<Text,Text>> cols = new HashSet<Pair<Text,Text>>();
    cols.add(new Pair<Text,Text>(new Text(""), null));
    cols.add(new Pair<Text,Text>(new Text("foo"), new Text("bar")));
    cols.add(new Pair<Text,Text>(new Text(""), new Text("bar")));
    cols.add(new Pair<Text,Text>(new Text(""), new Text("")));
    cols.add(new Pair<Text,Text>(new Text("foo"), new Text("")));
    AccumuloInputFormat.fetchColumns(job, cols);
    Set<Pair<Text,Text>> setCols = AccumuloInputFormat.getFetchedColumns(job);
    assertEquals(cols, setCols);
  }
}
