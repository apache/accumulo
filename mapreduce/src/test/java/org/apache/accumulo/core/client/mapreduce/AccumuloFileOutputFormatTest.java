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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AccumuloFileOutputFormatTest {
  private static final String PREFIX = AccumuloFileOutputFormatTest.class.getSimpleName();
  private static final String INSTANCE_NAME = PREFIX + "_mapreduce_instance";
  private static final String BAD_TABLE = PREFIX + "_mapreduce_bad_table";
  private static final String TEST_TABLE = PREFIX + "_mapreduce_test_table";
  private static final String EMPTY_TABLE = PREFIX + "_mapreduce_empty_table";

  private static AssertionError e1 = null;
  private static AssertionError e2 = null;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @BeforeClass
  public static void setup() throws Exception {
    MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
    Connector c = mockInstance.getConnector("root", new PasswordToken(""));
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
        try {
          try {
            context.write(key, value);
            if (index == 2)
              assertTrue(false);
          } catch (Exception e) {
            assertEquals(2, index);
          }
        } catch (AssertionError e) {
          e1 = e;
        }
        index++;
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
          assertEquals(2, index);
        } catch (AssertionError e) {
          e2 = e;
        }
      }
    }

    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 4) {
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName() + " <user> <pass> <table> <outputfile>");
      }

      String user = args[0];
      String pass = args[1];
      String table = args[2];

      @SuppressWarnings("deprecation")
      Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());

      job.setInputFormatClass(AccumuloInputFormat.class);

      AccumuloInputFormat.setConnectorInfo(job, user, new PasswordToken(pass));
      AccumuloInputFormat.setInputTableName(job, table);
      AccumuloInputFormat.setMockInstance(job, INSTANCE_NAME);
      AccumuloFileOutputFormat.setOutputPath(job, new Path(args[3]));

      job.setMapperClass(BAD_TABLE.equals(table) ? BadKeyMapper.class : Mapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormatClass(AccumuloFileOutputFormat.class);

      job.setNumReduceTasks(0);

      job.waitForCompletion(true);

      return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
      assertEquals(0, ToolRunner.run(CachedConfiguration.getInstance(), new MRTester(), args));
    }
  }

  public void handleWriteTests(boolean content) throws Exception {
    File f = folder.newFile("handleWriteTests");
    f.delete();
    MRTester.main(new String[] {"root", "", content ? TEST_TABLE : EMPTY_TABLE, f.getAbsolutePath()});

    assertTrue(f.exists());
    File[] files = f.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.getName().startsWith("part-m-");
      }
    });
    if (content) {
      assertEquals(1, files.length);
      assertTrue(files[0].exists());
    } else {
      assertEquals(0, files.length);
    }
  }

  @Test
  public void writeBadVisibility() throws Exception {
    File f = folder.newFile("writeBadVisibility");
    f.delete();
    MRTester.main(new String[] {"root", "", BAD_TABLE, f.getAbsolutePath()});
    assertNull(e1);
    assertNull(e2);
  }

  @Test
  public void validateConfiguration() throws IOException, InterruptedException {

    int a = 7;
    long b = 300l;
    long c = 50l;
    long d = 10l;
    String e = "snappy";

    @SuppressWarnings("deprecation")
    Job job1 = new Job();
    AccumuloFileOutputFormat.setReplication(job1, a);
    AccumuloFileOutputFormat.setFileBlockSize(job1, b);
    AccumuloFileOutputFormat.setDataBlockSize(job1, c);
    AccumuloFileOutputFormat.setIndexBlockSize(job1, d);
    AccumuloFileOutputFormat.setCompressionType(job1, e);

    AccumuloConfiguration acuconf = AccumuloFileOutputFormat.getAccumuloConfiguration(job1);

    assertEquals(7, acuconf.getCount(Property.TABLE_FILE_REPLICATION));
    assertEquals(300l, acuconf.getMemoryInBytes(Property.TABLE_FILE_BLOCK_SIZE));
    assertEquals(50l, acuconf.getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE));
    assertEquals(10l, acuconf.getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX));
    assertEquals("snappy", acuconf.get(Property.TABLE_FILE_COMPRESSION_TYPE));

    a = 17;
    b = 1300l;
    c = 150l;
    d = 110l;
    e = "lzo";

    @SuppressWarnings("deprecation")
    Job job2 = new Job();
    AccumuloFileOutputFormat.setReplication(job2, a);
    AccumuloFileOutputFormat.setFileBlockSize(job2, b);
    AccumuloFileOutputFormat.setDataBlockSize(job2, c);
    AccumuloFileOutputFormat.setIndexBlockSize(job2, d);
    AccumuloFileOutputFormat.setCompressionType(job2, e);

    acuconf = AccumuloFileOutputFormat.getAccumuloConfiguration(job2);

    assertEquals(17, acuconf.getCount(Property.TABLE_FILE_REPLICATION));
    assertEquals(1300l, acuconf.getMemoryInBytes(Property.TABLE_FILE_BLOCK_SIZE));
    assertEquals(150l, acuconf.getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE));
    assertEquals(110l, acuconf.getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX));
    assertEquals("lzo", acuconf.get(Property.TABLE_FILE_COMPRESSION_TYPE));

  }
}
