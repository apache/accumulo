/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.hadoop.its.mapred;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.hadoop.mapred.AccumuloInputFormat;
import org.apache.accumulo.hadoop.mapred.AccumuloOutputFormat;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.Test;

public class AccumuloOutputFormatIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "1");
    cfg.setNumTservers(1);
  }

  // Prevent regression of ACCUMULO-3709.
  @Test
  public void testMapred() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // create a table and put some data in it
      final String tableName = testName();
      client.tableOperations().create(tableName);

      JobConf job = new JobConf();
      BatchWriterConfig batchConfig = new BatchWriterConfig();
      // no flushes!!!!!
      batchConfig.setMaxLatency(0, TimeUnit.MILLISECONDS);
      // use a single thread to ensure our update session times out
      batchConfig.setMaxWriteThreads(1);
      // set the max memory so that we ensure we don't flush on the write.
      batchConfig.setMaxMemory(Long.MAX_VALUE);
      AccumuloOutputFormat outputFormat = new AccumuloOutputFormat();
      Properties props = Accumulo.newClientProperties().from(getClientProperties())
          .batchWriterConfig(batchConfig).build();
      AccumuloOutputFormat.configure().clientProperties(props).store(job);
      RecordWriter<Text,Mutation> writer = outputFormat.getRecordWriter(null, job, "Test", null);

      try {
        for (int i = 0; i < 3; i++) {
          Mutation m = new Mutation(new Text(String.format("%08d", i)));
          for (int j = 0; j < 3; j++) {
            m.put("cf1", "cq" + j, i + "_" + j);
          }
          writer.write(new Text(tableName), m);
        }

      } catch (Exception e) {
        e.printStackTrace();
        // we don't want the exception to come from write
      }

      client.securityOperations().revokeTablePermission("root", tableName, TablePermission.WRITE);

      var ex = assertThrows(IOException.class, () -> writer.close(null));
      log.info(ex.getMessage(), ex);
      assertTrue(ex.getCause() instanceof MutationsRejectedException);
    }
  }

  private static AssertionError e1 = null;

  private static class MRTester extends Configured implements Tool {
    private static class TestMapper implements Mapper<Key,Value,Text,Mutation> {
      Key key = null;
      int count = 0;
      OutputCollector<Text,Mutation> finalOutput;

      @Override
      public void map(Key k, Value v, OutputCollector<Text,Mutation> output, Reporter reporter) {
        finalOutput = output;
        try {
          if (key != null) {
            assertEquals(key.getRow().toString(), new String(v.get()));
          }
          assertEquals(k.getRow(), new Text(String.format("%09x", count + 1)));
          assertEquals(new String(v.get()), String.format("%09x", count));
        } catch (AssertionError e) {
          e1 = e;
        }
        key = new Key(k);
        count++;
      }

      @Override
      public void configure(JobConf job) {}

      @Override
      public void close() throws IOException {
        Mutation m = new Mutation("total");
        m.put("", "", Integer.toString(count));
        finalOutput.collect(new Text(), m);
      }

    }

    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 6) {
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName()
            + " <user> <pass> <inputtable> <outputtable> <instanceName> <zooKeepers>");
      }

      String user = args[0];
      String pass = args[1];
      String table1 = args[2];
      String table2 = args[3];
      String instanceName = args[4];
      String zooKeepers = args[5];

      JobConf job = new JobConf(getConf());
      job.setJarByClass(this.getClass());

      job.setInputFormat(AccumuloInputFormat.class);

      Properties cp =
          Accumulo.newClientProperties().to(instanceName, zooKeepers).as(user, pass).build();

      AccumuloInputFormat.configure().clientProperties(cp).table(table1).auths(Authorizations.EMPTY)
          .store(job);

      job.setMapperClass(TestMapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormat(AccumuloOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Mutation.class);

      AccumuloOutputFormat.configure().clientProperties(cp).defaultTable(table2).store(job);

      job.setNumReduceTasks(0);

      return JobClient.runJob(job).isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("mapreduce.framework.name", "local");
      conf.set("mapreduce.cluster.local.dir",
          new File(System.getProperty("user.dir"), "target/mapreduce-tmp").getAbsolutePath());
      assertEquals(0, ToolRunner.run(conf, new MRTester(), args));
    }
  }

  @Test
  public void testMR() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      String instanceName = getCluster().getInstanceName();
      String table1 = instanceName + "_t1";
      String table2 = instanceName + "_t2";
      c.tableOperations().create(table1);
      c.tableOperations().create(table2);
      try (BatchWriter bw = c.createBatchWriter(table1)) {
        for (int i = 0; i < 100; i++) {
          Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
          m.put("", "", String.format("%09x", i));
          bw.addMutation(m);
        }
      }

      MRTester.main(new String[] {"root", ROOT_PASSWORD, table1, table2, instanceName,
          getCluster().getZooKeepers()});
      assertNull(e1);

      try (Scanner scanner = c.createScanner(table2, new Authorizations())) {
        int i = scanner.stream().map(Map.Entry::getValue).map(Value::get).map(String::new)
            .map(Integer::parseInt).collect(onlyElement());
        assertEquals(100, i);
      }
    }
  }
}
