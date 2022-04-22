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

import static com.google.common.collect.MoreCollectors.onlyElement;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.Test;

/**
 * This tests deprecated mapreduce code in core jar
 */
@Deprecated(since = "2.0.0")
public class AccumuloOutputFormatIT extends AccumuloClusterHarness {
  private static AssertionError e1 = null;

  private static class MRTester extends Configured implements Tool {
    private static class TestMapper extends Mapper<Key,Value,Text,Mutation> {
      Key key = null;
      int count = 0;

      @Override
      protected void map(Key k, Value v, Context context) {
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
        Mutation m = new Mutation("total");
        m.put("", "", Integer.toString(count));
        context.write(new Text(), m);
      }
    }

    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 2) {
        throw new IllegalArgumentException(
            "Usage : " + MRTester.class.getName() + " <inputtable> <outputtable>");
      }

      String table1 = args[0];
      String table2 = args[1];

      Job job = Job.getInstance(getConf(),
          this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
      job.setJarByClass(this.getClass());

      job.setInputFormatClass(org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.class);

      ClientInfo ci = getClientInfo();
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setZooKeeperInstance(job,
          ci.getInstanceName(), ci.getZooKeepers());
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setConnectorInfo(job,
          ci.getPrincipal(), ci.getAuthenticationToken());
      org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat.setInputTableName(job, table1);

      job.setMapperClass(TestMapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormatClass(
          org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Mutation.class);

      org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat.setZooKeeperInstance(job,
          ci.getInstanceName(), ci.getZooKeepers());
      org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat.setConnectorInfo(job,
          ci.getPrincipal(), ci.getAuthenticationToken());
      org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat.setCreateTables(job, false);
      org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat.setDefaultTableName(job,
          table2);

      job.setNumReduceTasks(0);

      job.waitForCompletion(true);

      return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("mapreduce.framework.name", "local");
      conf.set("mapreduce.cluster.local.dir",
          new File(System.getProperty("user.dir"), "target/mapreduce-tmp").getAbsolutePath());
      assertEquals(0, ToolRunner.run(conf, new MRTester(), args));
    }
  }

  public static void insertData(AccumuloClient client, String table)
      throws TableNotFoundException, MutationsRejectedException {
    try (BatchWriter bw = client.createBatchWriter(table)) {
      for (int i = 0; i < 100; i++) {
        Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
        m.put("", "", String.format("%09x", i));
        bw.addMutation(m);
      }
    }
  }

  @Test
  public void testMR() throws Exception {
    String[] tableNames = getUniqueNames(2);
    String table1 = tableNames[0];
    String table2 = tableNames[1];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(table1);
      c.tableOperations().create(table2);
      insertData(c, table1);

      MRTester.main(new String[] {table1, table2});
      assertNull(e1);

      try (Scanner scanner = c.createScanner(table2, new Authorizations())) {
        int actual = scanner.stream().map(Entry::getValue).map(Value::get).map(String::new)
            .map(Integer::parseInt).collect(onlyElement());
        assertEquals(actual, 100);
      }
    }
  }
}
