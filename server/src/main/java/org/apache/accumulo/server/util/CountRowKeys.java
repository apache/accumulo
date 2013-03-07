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
package org.apache.accumulo.server.util;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.ServerConstants;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountRowKeys extends Configured implements Tool {
  private static class MyMapper extends Mapper<Key,Value,Text,NullWritable> {
    Text k = new Text();
    
    public void map(Key key, Value value, Context context) throws IOException, InterruptedException {
      context.write(key.getRow(k), NullWritable.get());
    }
  }
  
  private static class MyReducer extends Reducer<Text,NullWritable,Text,Text> {
    public enum Count {
      uniqueRows
    }
    
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException {
      context.getCounter(Count.uniqueRows).increment(1);
    }
  }
  
  @Override
  public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    if (args.length != 2) {
      System.out.println("Usage: CountRowKeys tableName outputPath");
      return 1;
    }
    
    Job job = new Job(getConf(), this.getClass().getName());
    job.setJarByClass(this.getClass());
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job, new Path(ServerConstants.getTablesDir() + "/" + args[0] + "/*/*/data"));
    
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    
    job.setReducerClass(MyReducer.class);
    
    TextOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(CachedConfiguration.getInstance(), new CountRowKeys(), args);
    if (res != 0)
      System.exit(res);
  }
}
