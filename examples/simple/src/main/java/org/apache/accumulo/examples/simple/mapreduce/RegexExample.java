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

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;

public class RegexExample extends Configured implements Tool {
  public static class RegexMapper extends Mapper<Key,Value,Key,Value> {
    @Override
    public void map(Key row, Value data, Context context) throws IOException, InterruptedException {
      context.write(row, data);
    }
  }

  static class Opts extends ClientOnRequiredTable {
    @Parameter(names = "--rowRegex")
    String rowRegex;
    @Parameter(names = "--columnFamilyRegex")
    String columnFamilyRegex;
    @Parameter(names = "--columnQualifierRegex")
    String columnQualifierRegex;
    @Parameter(names = "--valueRegex")
    String valueRegex;
    @Parameter(names = "--output", required = true)
    String destination;
  }

  @Override
  public int run(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(getClass().getName(), args);

    Job job = JobUtil.getJob(getConf());
    job.setJobName(getClass().getSimpleName());
    job.setJarByClass(getClass());

    job.setInputFormatClass(AccumuloInputFormat.class);
    opts.setAccumuloConfigs(job);

    IteratorSetting regex = new IteratorSetting(50, "regex", RegExFilter.class);
    RegExFilter.setRegexs(regex, opts.rowRegex, opts.columnFamilyRegex, opts.columnQualifierRegex, opts.valueRegex, false);
    AccumuloInputFormat.addIterator(job, regex);

    job.setMapperClass(RegexMapper.class);
    job.setMapOutputKeyClass(Key.class);
    job.setMapOutputValueClass(Value.class);

    job.setNumReduceTasks(0);

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(opts.destination));

    System.out.println("setRowRegex: " + opts.rowRegex);
    System.out.println("setColumnFamilyRegex: " + opts.columnFamilyRegex);
    System.out.println("setColumnQualifierRegex: " + opts.columnQualifierRegex);
    System.out.println("setValueRegex: " + opts.valueRegex);

    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RegexExample(), args);
    if (res != 0)
      System.exit(res);
  }
}
