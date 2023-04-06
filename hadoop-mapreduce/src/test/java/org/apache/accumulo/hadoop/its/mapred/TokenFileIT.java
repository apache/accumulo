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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.hadoop.mapred.AccumuloInputFormat;
import org.apache.accumulo.hadoop.mapred.AccumuloOutputFormat;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths not from user input")
public class TokenFileIT extends AccumuloClusterHarness {
  private static AssertionError e1 = null;

  private static class MRTokenFileTester extends Configured implements Tool {
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

    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
    @Override
    public int run(String[] args) throws Exception {

      if (args.length != 3) {
        throw new IllegalArgumentException("Usage : " + MRTokenFileTester.class.getName()
            + " <token file> <inputtable> <outputtable>");
      }

      String tokenFile = args[0];
      Properties cp = Accumulo.newClientProperties().from(Paths.get(tokenFile)).build();
      String table1 = args[1];
      String table2 = args[2];

      JobConf job = new JobConf(getConf());
      job.setJarByClass(this.getClass());

      job.setInputFormat(AccumuloInputFormat.class);

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

    @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
    public static void main(String[] args) throws Exception {
      Configuration conf = cluster.getServerContext().getHadoopConf();
      conf.set("hadoop.tmp.dir", new File(args[0]).getParent());
      conf.set("mapreduce.framework.name", "local");
      conf.set("mapreduce.cluster.local.dir",
          new File(System.getProperty("user.dir"), "target/mapreduce-tmp").getAbsolutePath());
      assertEquals(0, ToolRunner.run(conf, new MRTokenFileTester(), args));
    }
  }

  @TempDir
  private static File tempDir;

  @Test
  public void testMR() throws Exception {
    String[] tableNames = getUniqueNames(2);
    String table1 = tableNames[0];
    String table2 = tableNames[1];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(table1);
      c.tableOperations().create(table2);
      try (BatchWriter bw = c.createBatchWriter(table1)) {
        for (int i = 0; i < 100; i++) {
          Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
          m.put("", "", String.format("%09x", i));
          bw.addMutation(m);
        }
      }

      File tf = new File(tempDir, "client.properties");
      assertTrue(tf.createNewFile(), "Failed to create file: " + tf);
      try (PrintStream out = new PrintStream(tf)) {
        getClientProps().store(out, "Credentials for " + getClass().getName());
      }

      MRTokenFileTester.main(new String[] {tf.getAbsolutePath(), table1, table2});
      assertNull(e1);

      try (Scanner scanner = c.createScanner(table2, new Authorizations())) {
        int i = scanner.stream().map(Map.Entry::getValue).map(Value::get).map(String::new)
            .map(Integer::parseInt).collect(onlyElement());
        assertEquals(100, i);
      }
    }
  }
}
