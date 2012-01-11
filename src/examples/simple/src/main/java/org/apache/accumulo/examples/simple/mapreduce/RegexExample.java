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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RegexExample extends Configured implements Tool {
  public static class RegexMapper extends Mapper<Key,Value,Key,Value> {
    public void map(Key row, Value data, Context context) throws IOException, InterruptedException {
      context.write(row, data);
    }
  }
  
  public int run(String[] args) throws Exception {
    Job job = new Job(getConf(), this.getClass().getSimpleName());
    job.setJarByClass(this.getClass());
    
    job.setInputFormatClass(AccumuloInputFormat.class);
    AccumuloInputFormat.setZooKeeperInstance(job.getConfiguration(), args[0], args[1]);
    AccumuloInputFormat.setInputInfo(job.getConfiguration(), args[2], args[3].getBytes(), args[4], new Authorizations());
    
    IteratorSetting regex = new IteratorSetting(50, "regex", RegExFilter.class);
    RegExFilter.setRegexs(regex, args[5], args[6], args[7], args[8], false);
    AccumuloInputFormat.addIterator(job.getConfiguration(), regex);
    
    job.setMapperClass(RegexMapper.class);
    job.setMapOutputKeyClass(Key.class);
    job.setMapOutputValueClass(Value.class);
    
    job.setNumReduceTasks(0);
    
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(args[9]));
    
    System.out.println("setRowRegex: " + args[5]);
    System.out.println("setColumnFamilyRegex: " + args[6]);
    System.out.println("setColumnQualifierRegex: " + args[7]);
    System.out.println("setValueRegex: " + args[8]);
    
    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(CachedConfiguration.getInstance(), new RegexExample(), args);
    if (res != 0)
      System.exit(res);
  }
}
