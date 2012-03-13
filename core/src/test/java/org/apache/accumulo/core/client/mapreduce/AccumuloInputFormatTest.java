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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase.AccumuloIterator;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase.AccumuloIteratorOption;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase.RangeInputSplit;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ContextFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.Test;

public class AccumuloInputFormatTest {
  
  @After
  public void tearDown() throws Exception {}
  
  /**
   * Test basic setting & getting of max versions.
   * 
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @Test
  public void testMaxVersions() throws IOException {
    JobContext job = ContextFactory.createJobContext();
    AccumuloInputFormat.setMaxVersions(job.getConfiguration(), 1);
    int version = AccumuloInputFormat.getMaxVersions(job.getConfiguration());
    assertEquals(1, version);
  }
  
  /**
   * Test max versions with an invalid value.
   * 
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  @Test(expected = IOException.class)
  public void testMaxVersionsLessThan1() throws IOException {
    JobContext job = ContextFactory.createJobContext();
    AccumuloInputFormat.setMaxVersions(job.getConfiguration(), 0);
  }
  
  /**
   * Test no max version configured.
   */
  @Test
  public void testNoMaxVersion() {
    JobContext job = ContextFactory.createJobContext();
    assertEquals(-1, AccumuloInputFormat.getMaxVersions(job.getConfiguration()));
  }
  
  /**
   * Check that the iterator configuration is getting stored in the Job conf correctly.
   */
  @Test
  public void testSetIterator() {
    JobContext job = ContextFactory.createJobContext();
    
    AccumuloInputFormat.addIterator(job.getConfiguration(), new IteratorSetting(1, "WholeRow", "org.apache.accumulo.core.iterators.WholeRowIterator"));
    Configuration conf = job.getConfiguration();
    String iterators = conf.get("AccumuloInputFormat.iterators");
    assertEquals("1:org.apache.accumulo.core.iterators.WholeRowIterator:WholeRow", iterators);
  }
  
  @Test
  public void testAddIterator() {
    JobContext job = ContextFactory.createJobContext();
    
    AccumuloInputFormat.addIterator(job.getConfiguration(), new IteratorSetting(1, "WholeRow", WholeRowIterator.class));
    AccumuloInputFormat.addIterator(job.getConfiguration(), new IteratorSetting(2, "Versions", "org.apache.accumulo.core.iterators.VersioningIterator"));
    IteratorSetting iter = new IteratorSetting(3, "Count", "org.apache.accumulo.core.iterators.CountingIterator");
    iter.addOption("v1", "1");
    iter.addOption("junk", "\0omg:!\\xyzzy");
    AccumuloInputFormat.addIterator(job.getConfiguration(), iter);
    
    List<AccumuloIterator> list = AccumuloInputFormat.getIterators(job.getConfiguration());
    
    // Check the list size
    assertTrue(list.size() == 3);
    
    // Walk the list and make sure our settings are correct
    AccumuloIterator setting = list.get(0);
    assertEquals(1, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.user.WholeRowIterator", setting.getIteratorClass());
    assertEquals("WholeRow", setting.getIteratorName());
    
    setting = list.get(1);
    assertEquals(2, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.VersioningIterator", setting.getIteratorClass());
    assertEquals("Versions", setting.getIteratorName());
    
    setting = list.get(2);
    assertEquals(3, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.CountingIterator", setting.getIteratorClass());
    assertEquals("Count", setting.getIteratorName());
    
    List<AccumuloIteratorOption> iteratorOptions = AccumuloInputFormat.getIteratorOptions(job.getConfiguration());
    assertEquals(2, iteratorOptions.size());
    assertEquals("Count", iteratorOptions.get(0).getIteratorName());
    assertEquals("Count", iteratorOptions.get(1).getIteratorName());
    assertEquals("v1", iteratorOptions.get(0).getKey());
    assertEquals("1", iteratorOptions.get(0).getValue());
    assertEquals("junk", iteratorOptions.get(1).getKey());
    assertEquals("\0omg:!\\xyzzy", iteratorOptions.get(1).getValue());
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
    Job job = new Job();
    AccumuloInputFormat.addIterator(job.getConfiguration(), someSetting);
    
    final String rawConfigOpt = new AccumuloIteratorOption("iterator", key, value).toString();
    
    assertEquals(rawConfigOpt, job.getConfiguration().get("AccumuloInputFormat.iterators.options"));
    
    List<AccumuloIteratorOption> opts = AccumuloInputFormat.getIteratorOptions(job.getConfiguration());
    assertEquals(1, opts.size());
    assertEquals(opts.get(0).getKey(), key);
    assertEquals(opts.get(0).getValue(), value);
    
    someSetting.addOption(key + "2", value);
    someSetting.setPriority(2);
    someSetting.setName("it2");
    AccumuloInputFormat.addIterator(job.getConfiguration(), someSetting);
    opts = AccumuloInputFormat.getIteratorOptions(job.getConfiguration());
    assertEquals(3, opts.size());
    for (AccumuloIteratorOption opt : opts) {
      assertEquals(opt.getKey().substring(0, key.length()), key);
      assertEquals(opt.getValue(), value);
    }
  }
  
  /**
   * Test getting iterator settings for multiple iterators set
   */
  @Test
  public void testGetIteratorSettings() {
    JobContext job = ContextFactory.createJobContext();
    
    AccumuloInputFormat.addIterator(job.getConfiguration(), new IteratorSetting(1, "WholeRow", "org.apache.accumulo.core.iterators.WholeRowIterator"));
    AccumuloInputFormat.addIterator(job.getConfiguration(), new IteratorSetting(2, "Versions", "org.apache.accumulo.core.iterators.VersioningIterator"));
    AccumuloInputFormat.addIterator(job.getConfiguration(), new IteratorSetting(3, "Count", "org.apache.accumulo.core.iterators.CountingIterator"));
    
    List<AccumuloIterator> list = AccumuloInputFormat.getIterators(job.getConfiguration());
    
    // Check the list size
    assertTrue(list.size() == 3);
    
    // Walk the list and make sure our settings are correct
    AccumuloIterator setting = list.get(0);
    assertEquals(1, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.WholeRowIterator", setting.getIteratorClass());
    assertEquals("WholeRow", setting.getIteratorName());
    
    setting = list.get(1);
    assertEquals(2, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.VersioningIterator", setting.getIteratorClass());
    assertEquals("Versions", setting.getIteratorName());
    
    setting = list.get(2);
    assertEquals(3, setting.getPriority());
    assertEquals("org.apache.accumulo.core.iterators.CountingIterator", setting.getIteratorClass());
    assertEquals("Count", setting.getIteratorName());
    
  }
  
  @Test
  public void testSetRegex() {
    JobContext job = ContextFactory.createJobContext();
    
    String regex = ">\"*%<>\'\\";
    
    IteratorSetting is = new IteratorSetting(50, regex, RegExFilter.class);
    RegExFilter.setRegexs(is, regex, null, null, null, false);
    AccumuloInputFormat.addIterator(job.getConfiguration(), is);
    
    assertTrue(regex.equals(AccumuloInputFormat.getIterators(job.getConfiguration()).get(0).getIteratorName()));
  }
  
  static class TestMapper extends Mapper<Key,Value,Key,Value> {
    Key key = null;
    int count = 0;
    
    @Override
    protected void map(Key k, Value v, Context context) throws IOException, InterruptedException {
      if (key != null)
        assertEquals(key.getRow().toString(), new String(v.get()));
      assertEquals(k.getRow(), new Text(String.format("%09x", count + 1)));
      assertEquals(new String(v.get()), String.format("%09x", count));
      key = new Key(k);
      count++;
    }
  }
  
  @Test
  public void testMap() throws Exception {
    MockInstance mockInstance = new MockInstance("testmapinstance");
    Connector c = mockInstance.getConnector("root", new byte[] {});
    c.tableOperations().create("testtable");
    BatchWriter bw = c.createBatchWriter("testtable", 10000L, 1000L, 4);
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();
    
    Job job = new Job(new Configuration());
    job.setInputFormatClass(AccumuloInputFormat.class);
    job.setMapperClass(TestMapper.class);
    job.setNumReduceTasks(0);
    AccumuloInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), "testtable", new Authorizations());
    AccumuloInputFormat.setMockInstance(job.getConfiguration(), "testmapinstance");
    
    AccumuloInputFormat input = new AccumuloInputFormat();
    List<InputSplit> splits = input.getSplits(job);
    assertEquals(splits.size(), 1);
    
    TestMapper mapper = (TestMapper) job.getMapperClass().newInstance();
    for (InputSplit split : splits) {
      TaskAttemptContext tac = ContextFactory.createTaskAttemptContext(job);
      RecordReader<Key,Value> reader = input.createRecordReader(split, tac);
      Mapper<Key,Value,Key,Value>.Context context = ContextFactory.createMapContext(mapper, tac, reader, null, split);
      reader.initialize(split, context);
      mapper.run(context);
    }
  }
  
  @Test
  public void testSimple() throws Exception {
    MockInstance mockInstance = new MockInstance("testmapinstance");
    Connector c = mockInstance.getConnector("root", new byte[] {});
    c.tableOperations().create("testtable2");
    BatchWriter bw = c.createBatchWriter("testtable2", 10000L, 1000L, 4);
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();
    
    JobContext job = ContextFactory.createJobContext();
    AccumuloInputFormat.setInputInfo(job.getConfiguration(), "root", "".getBytes(), "testtable2", new Authorizations());
    AccumuloInputFormat.setMockInstance(job.getConfiguration(), "testmapinstance");
    AccumuloInputFormat input = new AccumuloInputFormat();
    RangeInputSplit ris = new RangeInputSplit();
    TaskAttemptContext tac = ContextFactory.createTaskAttemptContext(job);
    RecordReader<Key,Value> rr = input.createRecordReader(ris, tac);
    rr.initialize(ris, tac);
    
    TestMapper mapper = new TestMapper();
    Mapper<Key,Value,Key,Value>.Context context = ContextFactory.createMapContext(mapper, tac, rr, null, ris);
    rr.initialize(ris, tac);
    while (rr.nextKeyValue()) {
      mapper.map(rr.getCurrentKey(), rr.getCurrentValue(), (TestMapper.Context) context);
    }
  }
}
