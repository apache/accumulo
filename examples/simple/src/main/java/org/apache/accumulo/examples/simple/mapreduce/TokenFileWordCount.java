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

import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A simple map reduce job that inserts word counts into accumulo. See the README for instructions on how to run this. This version does not use the ClientOpts
 * class to parse arguments as an example of using AccumuloInputFormat and AccumuloOutputFormat directly. See README.mapred for more details.
 *
 */
public class TokenFileWordCount extends Configured implements Tool {

  public static class MapClass extends Mapper<LongWritable,Text,Text,Mutation> {
    @Override
    public void map(LongWritable key, Text value, Context output) throws IOException {
      String[] words = value.toString().split("\\s+");

      for (String word : words) {

        Mutation mutation = new Mutation(new Text(word));
        mutation.put(new Text("count"), new Text("20080906"), new Value("1".getBytes()));

        try {
          output.write(null, mutation);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {

    String instance = args[0];
    String zookeepers = args[1];
    String user = args[2];
    String tokenFile = args[3];
    String input = args[4];
    String tableName = args[5];

    Job job = JobUtil.getJob(getConf());
    job.setJobName(TokenFileWordCount.class.getName());
    job.setJarByClass(this.getClass());

    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.setInputPaths(job, input);

    job.setMapperClass(MapClass.class);

    job.setNumReduceTasks(0);

    job.setOutputFormatClass(AccumuloOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Mutation.class);

    // AccumuloInputFormat not used here, but it uses the same functions.
    AccumuloOutputFormat.setZooKeeperInstance(job, ClientConfiguration.loadDefault().withInstance(instance).withZkHosts(zookeepers));
    AccumuloOutputFormat.setConnectorInfo(job, user, tokenFile);
    AccumuloOutputFormat.setCreateTables(job, true);
    AccumuloOutputFormat.setDefaultTableName(job, tableName);

    job.waitForCompletion(true);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TokenFileWordCount(), args);
    System.exit(res);
  }
}
