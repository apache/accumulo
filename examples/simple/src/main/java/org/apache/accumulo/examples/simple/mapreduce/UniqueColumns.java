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
package org.apache.accumulo.examples.simple.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;

/**
 * A simple map reduce job that computes the unique column families and column qualifiers in a table. This example shows one way to run against an offline
 * table.
 */
public class UniqueColumns extends Configured implements Tool {

  private static final Text EMPTY = new Text();

  public static class UMapper extends Mapper<Key,Value,Text,Text> {
    private Text temp = new Text();
    private static final Text CF = new Text("cf:");
    private static final Text CQ = new Text("cq:");

    @Override
    public void map(Key key, Value value, Context context) throws IOException, InterruptedException {
      temp.set(CF);
      ByteSequence cf = key.getColumnFamilyData();
      temp.append(cf.getBackingArray(), cf.offset(), cf.length());
      context.write(temp, EMPTY);

      temp.set(CQ);
      ByteSequence cq = key.getColumnQualifierData();
      temp.append(cq.getBackingArray(), cq.offset(), cq.length());
      context.write(temp, EMPTY);
    }
  }

  public static class UReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      context.write(key, EMPTY);
    }
  }

  static class Opts extends ClientOnRequiredTable {
    @Parameter(names = "--output", description = "output directory")
    String output;
    @Parameter(names = "--reducers", description = "number of reducers to use", required = true)
    int reducers;
    @Parameter(names = "--offline", description = "run against an offline table")
    boolean offline = false;
  }

  @Override
  public int run(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(UniqueColumns.class.getName(), args);

    String jobName = this.getClass().getSimpleName() + "_" + System.currentTimeMillis();

    Job job = JobUtil.getJob(getConf());
    job.setJobName(jobName);
    job.setJarByClass(this.getClass());

    String clone = opts.tableName;
    Connector conn = null;

    opts.setAccumuloConfigs(job);

    if (opts.offline) {
      /*
       * this example clones the table and takes it offline. If you plan to run map reduce jobs over a table many times, it may be more efficient to compact the
       * table, clone it, and then keep using the same clone as input for map reduce.
       */

      conn = opts.getConnector();
      clone = opts.tableName + "_" + jobName;
      conn.tableOperations().clone(opts.tableName, clone, true, new HashMap<String,String>(), new HashSet<String>());
      conn.tableOperations().offline(clone);

      AccumuloInputFormat.setOfflineTableScan(job, true);
      AccumuloInputFormat.setInputTableName(job, clone);
    }

    job.setInputFormatClass(AccumuloInputFormat.class);

    job.setMapperClass(UMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setCombinerClass(UReducer.class);
    job.setReducerClass(UReducer.class);

    job.setNumReduceTasks(opts.reducers);

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(opts.output));

    job.waitForCompletion(true);

    if (opts.offline) {
      conn.tableOperations().delete(clone);
    }

    return job.isSuccessful() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new UniqueColumns(), args);
    System.exit(res);
  }
}
