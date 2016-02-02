/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.mapred;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapred.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
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
import org.junit.Test;

public class AccumuloOutputFormatIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "1");
    cfg.setNumTservers(1);
  }

  // Prevent regression of ACCUMULO-3709.
  @Test
  public void testMapred() throws Exception {
    Connector connector = getConnector();
    // create a table and put some data in it
    connector.tableOperations().create(testName.getMethodName());

    JobConf job = new JobConf();
    BatchWriterConfig batchConfig = new BatchWriterConfig();
    // no flushes!!!!!
    batchConfig.setMaxLatency(0, TimeUnit.MILLISECONDS);
    // use a single thread to ensure our update session times out
    batchConfig.setMaxWriteThreads(1);
    // set the max memory so that we ensure we don't flush on the write.
    batchConfig.setMaxMemory(Long.MAX_VALUE);
    AccumuloOutputFormat outputFormat = new AccumuloOutputFormat();
    AccumuloOutputFormat.setBatchWriterOptions(job, batchConfig);
    AccumuloOutputFormat.setZooKeeperInstance(job, cluster.getClientConfig());
    AccumuloOutputFormat.setConnectorInfo(job, "root", new PasswordToken(ROOT_PASSWORD));
    RecordWriter<Text,Mutation> writer = outputFormat.getRecordWriter(null, job, "Test", null);

    try {
      for (int i = 0; i < 3; i++) {
        Mutation m = new Mutation(new Text(String.format("%08d", i)));
        for (int j = 0; j < 3; j++) {
          m.put(new Text("cf1"), new Text("cq" + j), new Value((i + "_" + j).getBytes(UTF_8)));
        }
        writer.write(new Text(testName.getMethodName()), m);
      }

    } catch (Exception e) {
      e.printStackTrace();
      // we don't want the exception to come from write
    }

    connector.securityOperations().revokeTablePermission("root", testName.getMethodName(), TablePermission.WRITE);

    try {
      writer.close(null);
      fail("Did not throw exception");
    } catch (IOException ex) {
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
      public void map(Key k, Value v, OutputCollector<Text,Mutation> output, Reporter reporter) throws IOException {
        finalOutput = output;
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
        throw new IllegalArgumentException("Usage : " + MRTester.class.getName() + " <user> <pass> <inputtable> <outputtable> <instanceName> <zooKeepers>");
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

      ClientConfiguration clientConfig = new ClientConfiguration().withInstance(instanceName).withZkHosts(zooKeepers);

      AccumuloInputFormat.setConnectorInfo(job, user, new PasswordToken(pass));
      AccumuloInputFormat.setInputTableName(job, table1);
      AccumuloInputFormat.setZooKeeperInstance(job, clientConfig);

      job.setMapperClass(TestMapper.class);
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputFormat(AccumuloOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Mutation.class);

      AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(pass));
      AccumuloOutputFormat.setCreateTables(job, false);
      AccumuloOutputFormat.setDefaultTableName(job, table2);
      AccumuloOutputFormat.setZooKeeperInstance(job, clientConfig);

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

  @Test
  public void testMR() throws Exception {
    Connector c = getConnector();
    String instanceName = getCluster().getInstanceName();
    String table1 = instanceName + "_t1";
    String table2 = instanceName + "_t2";
    c.tableOperations().create(table1);
    c.tableOperations().create(table2);
    BatchWriter bw = c.createBatchWriter(table1, new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();

    MRTester.main(new String[] {"root", ROOT_PASSWORD, table1, table2, instanceName, getCluster().getZooKeepers()});
    assertNull(e1);

    Scanner scanner = c.createScanner(table2, new Authorizations());
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    assertTrue(iter.hasNext());
    Entry<Key,Value> entry = iter.next();
    assertEquals(Integer.parseInt(new String(entry.getValue().get())), 100);
    assertFalse(iter.hasNext());
  }

}
