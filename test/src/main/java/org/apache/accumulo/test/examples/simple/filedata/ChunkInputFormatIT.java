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

package org.apache.accumulo.test.examples.simple.filedata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.simple.filedata.ChunkInputFormat;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class ChunkInputFormatIT extends AccumuloClusterHarness {

  // track errors in the map reduce job; jobs insert a dummy error for the map and cleanup tasks (to ensure test correctness),
  // so error tests should check to see if there is at least one error (could be more depending on the test) rather than zero
  private static Multimap<String,AssertionError> assertionErrors = ArrayListMultimap.create();

  private static final Authorizations AUTHS = new Authorizations("A", "B", "C", "D");

  private static List<Entry<Key,Value>> data;
  private static List<Entry<Key,Value>> baddata;

  private Connector conn;
  private String tableName;

  @Before
  public void setupInstance() throws Exception {
    conn = getConnector();
    tableName = getUniqueNames(1)[0];
    conn.securityOperations().changeUserAuthorizations(conn.whoami(), AUTHS);
  }

  @BeforeClass
  public static void setupClass() {
    System.setProperty("hadoop.tmp.dir", System.getProperty("user.dir") + "/target/hadoop-tmp");

    data = new ArrayList<Entry<Key,Value>>();
    ChunkInputStreamIT.addData(data, "a", "refs", "ida\0ext", "A&B", "ext");
    ChunkInputStreamIT.addData(data, "a", "refs", "ida\0name", "A&B", "name");
    ChunkInputStreamIT.addData(data, "a", "~chunk", 100, 0, "A&B", "asdfjkl;");
    ChunkInputStreamIT.addData(data, "a", "~chunk", 100, 1, "A&B", "");
    ChunkInputStreamIT.addData(data, "b", "refs", "ida\0ext", "A&B", "ext");
    ChunkInputStreamIT.addData(data, "b", "refs", "ida\0name", "A&B", "name");
    ChunkInputStreamIT.addData(data, "b", "~chunk", 100, 0, "A&B", "qwertyuiop");
    ChunkInputStreamIT.addData(data, "b", "~chunk", 100, 0, "B&C", "qwertyuiop");
    ChunkInputStreamIT.addData(data, "b", "~chunk", 100, 1, "A&B", "");
    ChunkInputStreamIT.addData(data, "b", "~chunk", 100, 1, "B&C", "");
    ChunkInputStreamIT.addData(data, "b", "~chunk", 100, 1, "D", "");
    baddata = new ArrayList<Entry<Key,Value>>();
    ChunkInputStreamIT.addData(baddata, "c", "refs", "ida\0ext", "A&B", "ext");
    ChunkInputStreamIT.addData(baddata, "c", "refs", "ida\0name", "A&B", "name");
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
        String table = context.getConfiguration().get("MRTester_tableName");
        assertNotNull(table);

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
              fail();
          }
        } catch (AssertionError e) {
          assertionErrors.put(table, e);
        } finally {
          value.close();
        }
        count++;
      }

      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        String table = context.getConfiguration().get("MRTester_tableName");
        assertNotNull(table);

        try {
          assertEquals(2, count);
        } catch (AssertionError e) {
          assertionErrors.put(table, e);
        }
      }
    }

    public static class TestNoClose extends Mapper<List<Entry<Key,Value>>,InputStream,List<Entry<Key,Value>>,InputStream> {
      int count = 0;

      @Override
      protected void map(List<Entry<Key,Value>> key, InputStream value, Context context) throws IOException, InterruptedException {
        String table = context.getConfiguration().get("MRTester_tableName");
        assertNotNull(table);

        byte[] b = new byte[5];
        int read;
        try {
          switch (count) {
            case 0:
              assertEquals(read = value.read(b), 5);
              assertEquals(new String(b, 0, read), "asdfj");
              break;
            default:
              fail();
          }
        } catch (AssertionError e) {
          assertionErrors.put(table, e);
        }
        count++;
        try {
          context.nextKeyValue();
          fail();
        } catch (IOException ioe) {
          assertionErrors.put(table + "_map_ioexception", new AssertionError(toString(), ioe));
        }
      }
    }

    public static class TestBadData extends Mapper<List<Entry<Key,Value>>,InputStream,List<Entry<Key,Value>>,InputStream> {
      @Override
      protected void map(List<Entry<Key,Value>> key, InputStream value, Context context) throws IOException, InterruptedException {
        String table = context.getConfiguration().get("MRTester_tableName");
        assertNotNull(table);

        byte[] b = new byte[20];
        try {
          assertEquals(key.size(), 2);
          entryEquals(key.get(0), baddata.get(0));
          entryEquals(key.get(1), baddata.get(1));
        } catch (AssertionError e) {
          assertionErrors.put(table, e);
        }
        try {
          assertFalse(value.read(b) > 0);
          try {
            fail();
          } catch (AssertionError e) {
            assertionErrors.put(table, e);
          }
        } catch (Exception e) {
          // expected, ignore
        }
        try {
          value.close();
          try {
            fail();
          } catch (AssertionError e) {
            assertionErrors.put(table, e);
          }
        } catch (Exception e) {
          // expected, ignore
        }
      }
    }

    @Override
    public int run(String[] args) throws Exception {
      if (args.length != 2) {
        throw new IllegalArgumentException("Usage : " + CIFTester.class.getName() + " <table> <mapperClass>");
      }

      String table = args[0];
      assertionErrors.put(table, new AssertionError("Dummy"));
      assertionErrors.put(table + "_map_ioexception", new AssertionError("Dummy_ioexception"));
      getConf().set("MRTester_tableName", table);

      Job job = Job.getInstance(getConf());
      job.setJobName(this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());

      job.setInputFormatClass(ChunkInputFormat.class);

      ChunkInputFormat.setZooKeeperInstance(job, getCluster().getClientConfig());
      ChunkInputFormat.setConnectorInfo(job, getAdminPrincipal(), getAdminToken());
      ChunkInputFormat.setInputTableName(job, table);
      ChunkInputFormat.setScanAuthorizations(job, AUTHS);

      @SuppressWarnings("unchecked")
      Class<? extends Mapper<?,?,?,?>> forName = (Class<? extends Mapper<?,?,?,?>>) Class.forName(args[1]);
      job.setMapperClass(forName);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormatClass(NullOutputFormat.class);

      job.setNumReduceTasks(0);

      job.waitForCompletion(true);

      return job.isSuccessful() ? 0 : 1;
    }

    public static int main(String... args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("mapreduce.cluster.local.dir", new File(System.getProperty("user.dir"), "target/mapreduce-tmp").getAbsolutePath());
      return ToolRunner.run(conf, new CIFTester(), args);
    }
  }

  @Test
  public void test() throws Exception {
    conn.tableOperations().create(tableName);
    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());

    for (Entry<Key,Value> e : data) {
      Key k = e.getKey();
      Mutation m = new Mutation(k.getRow());
      m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
      bw.addMutation(m);
    }
    bw.close();

    assertEquals(0, CIFTester.main(tableName, CIFTester.TestMapper.class.getName()));
    assertEquals(1, assertionErrors.get(tableName).size());
  }

  @Test
  public void testErrorOnNextWithoutClose() throws Exception {
    conn.tableOperations().create(tableName);
    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());

    for (Entry<Key,Value> e : data) {
      Key k = e.getKey();
      Mutation m = new Mutation(k.getRow());
      m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
      bw.addMutation(m);
    }
    bw.close();

    assertEquals(1, CIFTester.main(tableName, CIFTester.TestNoClose.class.getName()));
    assertEquals(1, assertionErrors.get(tableName).size());
    // this should actually exist, in addition to the dummy entry
    assertEquals(2, assertionErrors.get(tableName + "_map_ioexception").size());
  }

  @Test
  public void testInfoWithoutChunks() throws Exception {
    conn.tableOperations().create(tableName);
    BatchWriter bw = conn.createBatchWriter(tableName, new BatchWriterConfig());
    for (Entry<Key,Value> e : baddata) {
      Key k = e.getKey();
      Mutation m = new Mutation(k.getRow());
      m.put(k.getColumnFamily(), k.getColumnQualifier(), new ColumnVisibility(k.getColumnVisibility()), k.getTimestamp(), e.getValue());
      bw.addMutation(m);
    }
    bw.close();

    assertEquals(0, CIFTester.main(tableName, CIFTester.TestBadData.class.getName()));
    assertEquals(1, assertionErrors.get(tableName).size());
  }
}
