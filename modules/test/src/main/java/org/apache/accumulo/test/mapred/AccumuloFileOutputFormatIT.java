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

package org.apache.accumulo.test.mapred;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapred.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase;
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
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloFileOutputFormatIT extends AccumuloClusterHarness {
  private static final Logger log = LoggerFactory.getLogger(AccumuloFileOutputFormatIT.class);
  private static final int JOB_VISIBILITY_CACHE_SIZE = 3000;
  private static final String PREFIX = AccumuloFileOutputFormatIT.class.getSimpleName();
  private static final String BAD_TABLE = PREFIX + "_mapred_bad_table";
  private static final String TEST_TABLE = PREFIX + "_mapred_test_table";
  private static final String EMPTY_TABLE = PREFIX + "_mapred_empty_table";

  private static AssertionError e1 = null;
  private static AssertionError e2 = null;

  private static final SamplerConfiguration SAMPLER_CONFIG = new SamplerConfiguration(RowSampler.class.getName()).addOption("hasher", "murmur3_32").addOption(
      "modulus", "3");

  @Rule
  public TemporaryFolder folder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Test
  public void testEmptyWrite() throws Exception {
    getConnector().tableOperations().create(EMPTY_TABLE);
    handleWriteTests(false);
  }

  @Test
  public void testRealWrite() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create(TEST_TABLE);
    BatchWriter bw = c.createBatchWriter(TEST_TABLE, new BatchWriterConfig());
    Mutation m = new Mutation("Key");
    m.put("", "", "");
    bw.addMutation(m);
    bw.close();
    handleWriteTests(true);
  }

  private static class MRTester extends Configured implements Tool {
    private static class BadKeyMapper implements Mapper<Key,Value,Key,Value> {

      int index = 0;

      @Override
      public void map(Key key, Value value, OutputCollector<Key,Value> output, Reporter reporter) throws IOException {
        try {
          try {
            output.collect(key, value);
            if (index == 2)
              fail();
          } catch (Exception e) {
            log.error(e.toString(), e);
            assertEquals(2, index);
          }
        } catch (AssertionError e) {
          e1 = e;
        }
        index++;
      }

      @Override
      public void configure(JobConf job) {}

      @Override
      public void close() throws IOException {
        try {
          assertEquals(2, index);
        } catch (AssertionError e) {
          e2 = e;
        }
      }

    }

    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 2) {
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName() + " <table> <outputfile>");
      }

      String table = args[0];

      JobConf job = new JobConf(getConf());
      job.setJarByClass(this.getClass());
      ConfiguratorBase.setVisibilityCacheSize(job, JOB_VISIBILITY_CACHE_SIZE);

      job.setInputFormat(AccumuloInputFormat.class);

      AccumuloInputFormat.setZooKeeperInstance(job, getCluster().getClientConfig());
      AccumuloInputFormat.setConnectorInfo(job, getAdminPrincipal(), getAdminToken());
      AccumuloInputFormat.setInputTableName(job, table);
      AccumuloFileOutputFormat.setOutputPath(job, new Path(args[1]));
      AccumuloFileOutputFormat.setSampler(job, SAMPLER_CONFIG);

      job.setMapperClass(BAD_TABLE.equals(table) ? BadKeyMapper.class : IdentityMapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormat(AccumuloFileOutputFormat.class);

      job.setNumReduceTasks(0);

      return JobClient.runJob(job).isSuccessful() ? 0 : 1;
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
    if (f.delete()) {
      log.debug("Deleted {}", f);
    }
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

  @Test
  public void writeBadVisibility() throws Exception {
    Connector c = getConnector();
    c.tableOperations().create(BAD_TABLE);
    BatchWriter bw = c.createBatchWriter(BAD_TABLE, new BatchWriterConfig());
    Mutation m = new Mutation("r1");
    m.put("cf1", "cq1", "A&B");
    m.put("cf1", "cq1", "A&B");
    m.put("cf1", "cq2", "A&");
    bw.addMutation(m);
    bw.close();
    File f = folder.newFile(testName.getMethodName());
    if (f.delete()) {
      log.debug("Deleted {}", f);
    }
    MRTester.main(new String[] {BAD_TABLE, f.getAbsolutePath()});
    assertNull(e1);
    assertNull(e2);
  }
}
