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
package org.apache.accumulo.test.mapreduce;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;

import org.apache.accumulo.core.cli.MapReduceClientOnRequiredTable;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;

public class RowHash extends Configured implements Tool {
  /**
   * The Mapper class that given a row number, will generate the appropriate output line.
   */
  public static class HashDataMapper extends Mapper<Key,Value,Text,Mutation> {
    @Override
    public void map(Key row, Value data, Context context) throws IOException, InterruptedException {
      Mutation m = new Mutation(row.getRow());
      m.put(new Text("cf-HASHTYPE"), new Text("cq-MD5BASE64"), new Value(Base64.getEncoder().encode(MD5Hash.digest(data.toString()).getDigest())));
      context.write(null, m);
      context.progress();
    }

    @Override
    public void setup(Context job) {}
  }

  private static class Opts extends MapReduceClientOnRequiredTable {
    @Parameter(names = "--column", required = true)
    String column;
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(this.getClass().getName());
    job.setJarByClass(this.getClass());
    Opts opts = new Opts();
    opts.parseArgs(RowHash.class.getName(), args);
    job.setInputFormatClass(AccumuloInputFormat.class);
    opts.setAccumuloConfigs(job);

    String col = opts.column;
    int idx = col.indexOf(":");
    Text cf = new Text(idx < 0 ? col : col.substring(0, idx));
    Text cq = idx < 0 ? null : new Text(col.substring(idx + 1));
    if (cf.getLength() > 0)
      AccumuloInputFormat.fetchColumns(job, Collections.singleton(new Pair<>(cf, cq)));

    job.setMapperClass(HashDataMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Mutation.class);

    job.setNumReduceTasks(0);

    job.setOutputFormatClass(AccumuloOutputFormat.class);

    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new RowHash(), args);
  }
}
