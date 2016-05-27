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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.sample.RowSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class AccumuloFileOutputFormatIT extends AccumuloClusterHarness {

  private String PREFIX;
  private String BAD_TABLE;
  private String TEST_TABLE;
  private String EMPTY_TABLE;

  private static final SamplerConfiguration SAMPLER_CONFIG = new SamplerConfiguration(RowSampler.class.getName()).addOption("hasher", "murmur3_32").addOption(
      "modulus", "3");

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Rule
  public TemporaryFolder folder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Before
  public void setup() throws Exception {
    PREFIX = testName.getMethodName() + "_";
    BAD_TABLE = PREFIX + "_mapreduce_bad_table";
    TEST_TABLE = PREFIX + "_mapreduce_test_table";
    EMPTY_TABLE = PREFIX + "_mapreduce_empty_table";

    Connector c = getConnector();
    c.tableOperations().create(EMPTY_TABLE);
    c.tableOperations().create(TEST_TABLE);
    c.tableOperations().create(BAD_TABLE);
    BatchWriter bw = c.createBatchWriter(TEST_TABLE, new BatchWriterConfig());
    Mutation m = new Mutation("Key");
    m.put("", "", "");
    bw.addMutation(m);
    bw.close();
    bw = c.createBatchWriter(BAD_TABLE, new BatchWriterConfig());
    m = new Mutation("r1");
    m.put("cf1", "cq1", "A&B");
    m.put("cf1", "cq1", "A&B");
    m.put("cf1", "cq2", "A&");
    bw.addMutation(m);
    bw.close();
  }

  @Test
  public void testEmptyWrite() throws Exception {
    handleWriteTests(false);
  }

  @Test
  public void testRealWrite() throws Exception {
    handleWriteTests(true);
  }

  private static class MRTester extends Configured implements Tool {
    private static class BadKeyMapper extends Mapper<Key,Value,Key,Value> {
      int index = 0;

      @Override
      protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
        String table = context.getConfiguration().get("MRTester_tableName");
        assertNotNull(table);
        try {
          try {
            context.write(key, value);
            if (index == 2)
              assertTrue(false);
          } catch (Exception e) {
            assertEquals(2, index);
          }
        } catch (AssertionError e) {
          assertionErrors.put(table + "_map", e);
        }
        index++;
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        String table = context.getConfiguration().get("MRTester_tableName");
        assertNotNull(table);
        try {
          assertEquals(2, index);
        } catch (AssertionError e) {
          assertionErrors.put(table + "_cleanup", e);
        }
      }
    }

    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 2) {
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName() + " <table> <outputfile>");
      }

      String table = args[0];
      assertionErrors.put(table + "_map", new AssertionError("Dummy_map"));
      assertionErrors.put(table + "_cleanup", new AssertionError("Dummy_cleanup"));

      Job job = Job.getInstance(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());

      job.setInputFormatClass(AccumuloInputFormat.class);

      AccumuloInputFormat.setConnectorInfo(job, getAdminPrincipal(), getAdminToken());
      AccumuloInputFormat.setInputTableName(job, table);
      AccumuloInputFormat.setZooKeeperInstance(job, getCluster().getClientConfig());
      AccumuloFileOutputFormat.setOutputPath(job, new Path(args[1]));
      AccumuloFileOutputFormat.setSampler(job, SAMPLER_CONFIG);

      job.setMapperClass(table.endsWith("_mapreduce_bad_table") ? BadKeyMapper.class : Mapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormatClass(AccumuloFileOutputFormat.class);
      job.getConfiguration().set("MRTester_tableName", table);

      job.setNumReduceTasks(0);

      job.waitForCompletion(true);

      return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("mapreduce.framework.name", "local");
      conf.set("mapreduce.cluster.local.dir", new File(System.getProperty("user.dir"), "target/mapreduce-tmp").getAbsolutePath());
      assertEquals(0, ToolRunner.run(conf, new MRTester(), args));
    }
  }

  private void handleWriteTests(boolean content) throws Exception {
    File f = folder.newFile(testName.getMethodName());
    assertTrue(f.delete());
    MRTester.main(new String[] {content ? TEST_TABLE : EMPTY_TABLE, f.getAbsolutePath()});

    assertTrue(f.exists());
    File[] files = f.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.getName().startsWith("part-m-");
      }
    });
    assertNotNull(files);
    if (content) {
      assertEquals(1, files.length);
      assertTrue(files[0].exists());

      Configuration conf = CachedConfiguration.getInstance();
      DefaultConfiguration acuconf = DefaultConfiguration.getInstance();
      FileSKVIterator sample = RFileOperations.getInstance().newReaderBuilder().forFile(files[0].toString(), FileSystem.get(conf), conf)
          .withTableConfiguration(acuconf).build().getSample(new SamplerConfigurationImpl(SAMPLER_CONFIG));
      assertNotNull(sample);
    } else {
      assertEquals(0, files.length);
    }
  }

  // track errors in the map reduce job; jobs insert a dummy error for the map and cleanup tasks (to ensure test correctness),
  // so error tests should check to see if there is at least one error (could be more depending on the test) rather than zero
  private static Multimap<String,AssertionError> assertionErrors = ArrayListMultimap.create();

  @Test
  public void writeBadVisibility() throws Exception {
    File f = folder.newFile(testName.getMethodName());
    assertTrue(f.delete());
    MRTester.main(new String[] {BAD_TABLE, f.getAbsolutePath()});
    assertTrue(f.exists());
    assertEquals(1, assertionErrors.get(BAD_TABLE + "_map").size());
    assertEquals(1, assertionErrors.get(BAD_TABLE + "_cleanup").size());
  }

}
