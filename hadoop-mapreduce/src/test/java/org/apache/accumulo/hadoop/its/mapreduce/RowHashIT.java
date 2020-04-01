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
package org.apache.accumulo.hadoop.its.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.hadoop.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.MapReduceClientOpts;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.beust.jcommander.Parameter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class RowHashIT extends ConfigurableMacBase {

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  public static final String hadoopTmpDirArg =
      "-Dhadoop.tmp.dir=" + System.getProperty("user.dir") + "/target/hadoop-tmp";

  static final String tablename = "mapredf";
  static final String input_cf = "cf-HASHTYPE";
  static final String input_cq = "cq-NOTHASHED";
  static final String input_cfcq = input_cf + ":" + input_cq;
  static final String output_cq = "cq-MD4BASE64";

  @Test
  public void test() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      runTest(client, getCluster());
    }
  }

  @SuppressFBWarnings(value = "WEAK_MESSAGE_DIGEST_MD5", justification = "md5 is okay for testing")
  static void runTest(AccumuloClient c, MiniAccumuloClusterImpl cluster) throws AccumuloException,
      AccumuloSecurityException, TableExistsException, TableNotFoundException,
      MutationsRejectedException, IOException, InterruptedException, NoSuchAlgorithmException {
    c.tableOperations().create(tablename);
    BatchWriter bw = c.createBatchWriter(tablename, new BatchWriterConfig());
    for (int i = 0; i < 10; i++) {
      Mutation m = new Mutation("" + i);
      m.put(input_cf, input_cq, "row" + i);
      bw.addMutation(m);
    }
    bw.close();
    Process hash = cluster.exec(RowHash.class, Collections.singletonList(hadoopTmpDirArg), "-c",
        cluster.getClientPropsPath(), "-t", tablename, "--column", input_cfcq).getProcess();
    assertEquals(0, hash.waitFor());

    try (Scanner s = c.createScanner(tablename, Authorizations.EMPTY)) {
      s.fetchColumn(new Text(input_cf), new Text(output_cq));
      int i = 0;
      for (Entry<Key,Value> entry : s) {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] check = Base64.getEncoder().encode(md.digest(("row" + i).getBytes()));
        assertEquals(entry.getValue().toString(), new String(check));
        i++;
      }
    }
  }

  public static class RowHash extends Configured implements Tool {
    /**
     * The Mapper class that given a row number, will generate the appropriate output line.
     */
    public class HashDataMapper extends Mapper<Key,Value,Text,Mutation> {
      @Override
      public void map(Key row, Value data, Context context)
          throws IOException, InterruptedException {
        Mutation m = new Mutation(row.getRow());
        m.put(new Text("cf-HASHTYPE"), new Text("cq-MD5BASE64"),
            new Value(Base64.getEncoder().encode(MD5Hash.digest(data.toString()).getDigest())));
        context.write(null, m);
        context.progress();
      }

      @Override
      public void setup(Context job) {}
    }

    public class Opts extends MapReduceClientOpts {
      @Parameter(names = "--column", required = true)
      String column;
      @Parameter(names = {"-t", "--table"}, required = true, description = "table to use")
      String tableName;
    }

    @Override
    public int run(String[] args) throws Exception {
      Job job = Job.getInstance(getConf());
      job.setJobName(this.getClass().getName());
      job.setJarByClass(this.getClass());
      RowHash.Opts opts = new RowHash.Opts();
      opts.parseArgs(RowHash.class.getName(), args);
      job.setInputFormatClass(AccumuloInputFormat.class);

      String col = opts.column;
      int idx = col.indexOf(":");
      Text cf = new Text(idx < 0 ? col : col.substring(0, idx));
      Text cq = idx < 0 ? null : new Text(col.substring(idx + 1));
      if (cf.getLength() > 0)
        AccumuloInputFormat.configure().clientProperties(opts.getClientProps())
            .table(opts.tableName).auths(Authorizations.EMPTY)
            .fetchColumns(Collections.singleton(new IteratorSetting.Column(cf, cq))).store(job);

      job.setMapperClass(RowHash.HashDataMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Mutation.class);

      job.setNumReduceTasks(0);

      job.setOutputFormatClass(AccumuloOutputFormat.class);
      AccumuloOutputFormat.configure().clientProperties(opts.getClientProps()).store(job);

      job.waitForCompletion(true);
      return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
      ToolRunner.run(new Configuration(), new RowHash(), args);
    }
  }

}
