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
package org.apache.accumulo.examples.simple.filedata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.simple.mapreduce.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class ChunkInputFormatTest {

  private static AssertionError e0 = null;
  private static AssertionError e1 = null;
  private static AssertionError e2 = null;
  private static IOException e3 = null;

  private static final Authorizations AUTHS = new Authorizations("A", "B", "C", "D");

  private static List<Entry<Key,Value>> data;
  private static List<Entry<Key,Value>> baddata;

  @BeforeClass
  public static void setupClass() {
    System.setProperty("hadoop.tmp.dir", System.getProperty("user.dir") + "/target/hadoop-tmp");

    data = new ArrayList<Entry<Key,Value>>();
    ChunkInputStreamTest.addData(data, "a", "refs", "ida\0ext", "A&B", "ext");
    ChunkInputStreamTest.addData(data, "a", "refs", "ida\0name", "A&B", "name");
    ChunkInputStreamTest.addData(data, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
    ChunkInputStreamTest.addData(data, "a", "~chunk", 100, 1, "A&B", "");
    ChunkInputStreamTest.addData(data, "b", "refs", "ida\0ext", "A&B", "ext");
    ChunkInputStreamTest.addData(data, "b", "refs", "ida\0name", "A&B", "name");
    ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 0, "A&B", "qwertyuiop");
    ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 0, "B&C", "qwertyuiop");
    ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "A&B", "");
    ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "B&C", "");
    ChunkInputStreamTest.addData(data, "b", "~chunk", 100, 1, "D", "");
    baddata = new ArrayList<Entry<Key,Value>>();
    ChunkInputStreamTest.addData(baddata, "c", "refs", "ida\0ext", "A&B", "ext");
    ChunkInputStreamTest.addData(baddata, "c", "refs", "ida\0name", "A&B", "name");
  }

  public static void entryEquals(Entry<Key,Value> e1, Entry<Key,Value> e2) {
    assertEquals(e1.getKey(), e2.getKey());
    assertEquals(e1.getValue(), e2.getValue());
  }

  public static class CIFTester extends Configured implements Tool {
    public static class TestMapper extends Mapper<List<Entry<Key,Value>>,InputStream,List<Entry<Key,Value>>,InputStream> {
      int count = 0;

      @Override
      protected void map(List<Entry<Key,Value>> key, InputStream value, Context context) throws IOException, InterruptedException {
        byte[] b = new byte[20];
        int read;
        try {
          switch (count) {
            case 0:
              assertEquals(key.size(), 2);
              entryEquals(key.get(0), data.get(0));
              entryEquals(key.get(1), data.get(1));
              assertEquals(read = value.read(b), 8);
              assertEquals(new String(b, 0, read), "asdfjkl;");
              assertEquals(read = value.read(b), -1);
              break;
            case 1:
              assertEquals(key.size(), 2);
              entryEquals(key.get(0), data.get(4));
              entryEquals(key.get(1), data.get(5));
              assertEquals(read = value.read(b), 10);
              assertEquals(new String(b, 0, read), "qwertyuiop");
              assertEquals(read = value.read(b), -1);
              break;
            default:
              assertTrue(false);
          }
        } catch (AssertionError e) {
          e1 = e;
        } finally {
          value.close();
        }
        count++;
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
          assertEquals(2, count);
        } catch (AssertionError e) {
          e2 = e;
        }
      }
    }

    public static class TestNoClose extends Mapper<List<Entry<Key,Value>>,InputStream,List<Entry<Key,Value>>,InputStream> {
      int count = 0;

      @Override
      protected void map(List<Entry<Key,Value>> key, InputStream value, Context context) throws IOException, InterruptedException {
        byte[] b = new byte[5];
        int read;
        try {
          switch (count) {
            case 0:
              assertEquals(read = value.read(b), 5);
              assertEquals(new String(b, 0, read), "asdfj");
              break;
            default:
              assertTrue(false);
          }
        } catch (AssertionError e) {
          e1 = e;
        }
        count++;
        try {
          context.nextKeyValue();
          assertTrue(false);
        } catch (IOException ioe) {
          e3 = ioe;
        }
      }
    }

    public static class TestBadData extends Mapper<List<Entry<Key,Value>>,InputStream,List<Entry<Key,Value>>,InputStream> {
      @Override
      protected void map(List<Entry<Key,Value>> key, InputStream value, Context context) throws IOException, InterruptedException {
        byte[] b = new byte[20];
        try {
          assertEquals(key.size(), 2);
          entryEquals(key.get(0), baddata.get(0));
          entryEquals(key.get(1), baddata.get(1));
        } catch (AssertionError e) {
          e0 = e;
        }
        try {
          value.read(b);
          try {
            assertTrue(false);
          } catch (AssertionError e) {
            e1 = e;
          }
        } catch (Exception e) {}
        try {
          value.close();
          try {
            assertTrue(false);
          } catch (AssertionError e) {
            e2 = e;
          }
        } catch (Exception e) {}
      }
    }

    @Override
    public int run(String[] args) throws Exception {
      if (args.length != 5) {
        throw new IllegalArgumentException("Usage : " + CIFTester.class.getName() + " <instance name> <user> <pass> <table> <mapperClass>");
      }

      String instance = args[0];
      String user = args[1];
      String pass = args[2];
      String table = args[3];

      Job job = JobUtil.getJob(getConf());
      job.setJobName(this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());

      job.setInputFormatClass(ChunkInputFormat.class);

      ChunkInputFormat.setConnectorInfo(job, user, new PasswordToken(pass));
      ChunkInputFormat.setInputTableName(job, table);
      ChunkInputFormat.setScanAuthorizations(job, AUTHS);
      ChunkInputFormat.setMockInstance(job, instance);

      @SuppressWarnings("unchecked")
      Class<? extends Mapper<?,?,?,?>> forName = (Class<? extends Mapper<?,?,?,?>>) Class.forName(args[4]);
      job.setMapperClass(forName);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormatClass(NullOutputFormat.class);

      job.setNumReduceTasks(0);

      job.waitForCompletion(true);

      return job.isSuccessful() ? 0 : 1;
    }

    public static int main(String... args) throws Exception {
      return ToolRunner.run(new Configuration(), new CIFTester(), args);
    }
  }

  @Test
  public void test() throws Exception {
    MockInstance instance = new MockInstance("instance1");
    Connector conn = instance.getConnector("root", new PasswordToken(""));
    conn.tableOperations().create("test");
    BatchWriter bw = conn.createBatchWriter("test", new BatchWriterConfig());

    for (Entry<Key,Value> e : data) {
      Key k = e.getKey();
      Mutation m = new Mutation(k.getRow());
      m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
      bw.addMutation(m);
    }
    bw.close();

    assertEquals(0, CIFTester.main("instance1", "root", "", "test", CIFTester.TestMapper.class.getName()));
    assertNull(e1);
    assertNull(e2);
  }

  @Test
  public void testErrorOnNextWithoutClose() throws Exception {
    MockInstance instance = new MockInstance("instance2");
    Connector conn = instance.getConnector("root", new PasswordToken(""));
    conn.tableOperations().create("test");
    BatchWriter bw = conn.createBatchWriter("test", new BatchWriterConfig());

    for (Entry<Key,Value> e : data) {
      Key k = e.getKey();
      Mutation m = new Mutation(k.getRow());
      m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
      bw.addMutation(m);
    }
    bw.close();

    assertEquals(1, CIFTester.main("instance2", "root", "", "test", CIFTester.TestNoClose.class.getName()));
    assertNull(e1);
    assertNull(e2);
    assertNotNull(e3);
  }

  @Test
  public void testInfoWithoutChunks() throws Exception {
    MockInstance instance = new MockInstance("instance3");
    Connector conn = instance.getConnector("root", new PasswordToken(""));
    conn.tableOperations().create("test");
    BatchWriter bw = conn.createBatchWriter("test", new BatchWriterConfig());
    for (Entry<Key,Value> e : baddata) {
      Key k = e.getKey();
      Mutation m = new Mutation(k.getRow());
      m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
      bw.addMutation(m);
    }
    bw.close();

    assertEquals(0, CIFTester.main("instance3", "root", "", "test", CIFTester.TestBadData.class.getName()));
    assertNull(e0);
    assertNull(e1);
    assertNull(e2);
  }
}
