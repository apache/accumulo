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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AccumuloFileOutputFormatTest {
  public static TemporaryFolder folder = new TemporaryFolder();
  private static AssertionError e1 = null;
  private static AssertionError e2 = null;
  
  @BeforeClass
  public static void setup() throws Exception {
    folder.create();
    
    MockInstance mockInstance = new MockInstance("testinstance");
    Connector c = mockInstance.getConnector("root", new byte[] {});
    c.tableOperations().create("emptytable");
    c.tableOperations().create("testtable");
    c.tableOperations().create("badtable");
    BatchWriter bw = c.createBatchWriter("testtable", new BatchWriterConfig());
    Mutation m = new Mutation("Key");
    m.put("", "", "");
    bw.addMutation(m);
    bw.close();
    bw = c.createBatchWriter("badtable", new BatchWriterConfig());
    m = new Mutation("r1");
    m.put("cf1", "cq1", "A&B");
    m.put("cf1", "cq1", "A&B");
    m.put("cf1", "cq2", "A&");
    bw.addMutation(m);
    bw.close();
  }
  
  @AfterClass
  public static void teardown() throws IOException {
    folder.delete();
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
      
      Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());
      
      job.setInputFormatClass(AccumuloInputFormat.class);
      Authorizations authorizations;
      authorizations = Constants.NO_AUTHS;
      
      AccumuloInputFormat.setInputInfo(job.getConfiguration(), user, pass.getBytes(), table, authorizations);
      AccumuloInputFormat.setMockInstance(job.getConfiguration(), "testinstance");
      AccumuloFileOutputFormat.setOutputPath(job, new Path(args[3]));
      
      job.setMapperClass("badtable".equals(table) ? BadKeyMapper.class : Mapper.class);
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
    File f = folder.newFile();
    f.delete();
    MRTester.main(new String[] {"root", "", content ? "testtable" : "emptytable", f.getAbsolutePath()});
    
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
    File f = folder.newFile();
    f.delete();
    MRTester.main(new String[] {"root", "", "badtable", f.getAbsolutePath()});
    assertNull(e1);
    assertNull(e2);
  }
  
  @Test
  public void validateConfiguration() throws IOException, InterruptedException {
    
    int a = 7;
    long b = 300l;
    long c = 50l;
    long d = 10l;
    String e = "type";
    
    Job job = new Job();
    AccumuloFileOutputFormat.setReplication(job, a);
    AccumuloFileOutputFormat.setFileBlockSize(job, b);
    AccumuloFileOutputFormat.setDataBlockSize(job, c);
    AccumuloFileOutputFormat.setIndexBlockSize(job, d);
    AccumuloFileOutputFormat.setCompressionType(job, e);
    
    AccumuloConfiguration acuconf = AccumuloFileOutputFormat.getAccumuloConfiguration(job);
    
    assertEquals(a, acuconf.getCount(Property.TABLE_FILE_REPLICATION));
    assertEquals(b, acuconf.getMemoryInBytes(Property.TABLE_FILE_BLOCK_SIZE));
    assertEquals(c, acuconf.getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE));
    assertEquals(d, acuconf.getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX));
    assertEquals(e, acuconf.get(Property.TABLE_FILE_COMPRESSION_TYPE));
  }
}
