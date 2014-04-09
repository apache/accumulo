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
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

/**
 * Map job to ingest n-gram files from http://storage.googleapis.com/books/ngrams/books/datasetsv2.html
 */
public class NGramIngest extends Configured implements Tool {

  private static final Logger log = Logger.getLogger(NGramIngest.class);

  static class Opts extends ClientOnRequiredTable {
    @Parameter(names = "--input", required = true)
    String inputDirectory;
  }

  static class NGramMapper extends Mapper<LongWritable,Text,Text,Mutation> {

    @Override
    protected void map(LongWritable location, Text value, Context context) throws IOException, InterruptedException {
      String parts[] = value.toString().split("\\t");
      if (parts.length >= 4) {
        Mutation m = new Mutation(parts[0]);
        m.put(parts[1], String.format("%010d", Long.parseLong(parts[2])), new Value(parts[3].trim().getBytes()));
        context.write(null, m);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(getClass().getName(), args);

    Job job = JobUtil.getJob(getConf());
    job.setJobName(getClass().getSimpleName());
    job.setJarByClass(getClass());

    opts.setAccumuloConfigs(job);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(AccumuloOutputFormat.class);

    job.setMapperClass(NGramMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Mutation.class);

    job.setNumReduceTasks(0);
    job.setSpeculativeExecution(false);

    if (!opts.getConnector().tableOperations().exists(opts.tableName)) {
      log.info("Creating table " + opts.tableName);
      opts.getConnector().tableOperations().create(opts.tableName);
      SortedSet<Text> splits = new TreeSet<Text>();
      String numbers[] = "1 2 3 4 5 6 7 8 9".split("\\s");
      String lower[] = "a b c d e f g h i j k l m n o p q r s t u v w x y z".split("\\s");
      String upper[] = "A B C D E F G H I J K L M N O P Q R S T U V W X Y Z".split("\\s");
      for (String[] array : new String[][] {numbers, lower, upper}) {
        for (String s : array) {
          splits.add(new Text(s));
        }
      }
      opts.getConnector().tableOperations().addSplits(opts.tableName, splits);
    }

    TextInputFormat.addInputPath(job, new Path(opts.inputDirectory));
    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new NGramIngest(), args);
    if (res != 0)
      System.exit(res);
  }

}
