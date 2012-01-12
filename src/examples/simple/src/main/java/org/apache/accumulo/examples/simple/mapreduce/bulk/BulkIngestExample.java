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
package org.apache.accumulo.examples.simple.mapreduce.bulk;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Example map reduce job that bulk ingest data into an accumulo table. The expected input is text files containing tab separated key value pairs on each line.
 */
public class BulkIngestExample extends Configured implements Tool {
  public static class MapClass extends Mapper<LongWritable,Text,Text,Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
      // split on tab
      int index = -1;
      for (int i = 0; i < value.getLength(); i++) {
        if (value.getBytes()[i] == '\t') {
          index = i;
          break;
        }
      }
      
      if (index > 0) {
        outputKey.set(value.getBytes(), 0, index);
        outputValue.set(value.getBytes(), index + 1, value.getLength() - (index + 1));
        output.write(outputKey, outputValue);
      }
    }
  }
  
  public static class ReduceClass extends Reducer<Text,Text,Key,Value> {
    public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
      // be careful with the timestamp... if you run on a cluster
      // where the time is whacked you may not see your updates in
      // accumulo if there is already an existing value with a later
      // timestamp in accumulo... so make sure ntp is running on the
      // cluster or consider using logical time... one options is
      // to let accumulo set the time
      long timestamp = System.currentTimeMillis();
      
      int index = 0;
      for (Text value : values) {
        Key outputKey = new Key(key, new Text("foo"), new Text("" + index), timestamp);
        index++;
        
        Value outputValue = new Value(value.getBytes(), 0, value.getLength());
        output.write(outputKey, outputValue);
      }
    }
  }
  
  public int run(String[] args) {
    if (args.length != 7) {
      System.out.println("ERROR: Wrong number of parameters: " + args.length + " instead of 7.");
      return printUsage();
    }
    
    Configuration conf = getConf();
    PrintStream out = null;
    try {
      Job job = new Job(conf, "bulk ingest example");
      job.setJarByClass(this.getClass());
      
      job.setInputFormatClass(TextInputFormat.class);
      
      job.setMapperClass(MapClass.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      job.setReducerClass(ReduceClass.class);
      job.setOutputFormatClass(AccumuloFileOutputFormat.class);
      
      Instance instance = new ZooKeeperInstance(args[0], args[1]);
      String user = args[2];
      byte[] pass = args[3].getBytes();
      String tableName = args[4];
      String inputDir = args[5];
      String workDir = args[6];
      
      Connector connector = instance.getConnector(user, pass);
      
      TextInputFormat.setInputPaths(job, new Path(inputDir));
      AccumuloFileOutputFormat.setOutputPath(job, new Path(workDir + "/files"));
      
      FileSystem fs = FileSystem.get(conf);
      out = new PrintStream(new BufferedOutputStream(fs.create(new Path(workDir + "/splits.txt"))));
      
      Collection<Text> splits = connector.tableOperations().getSplits(tableName, 100);
      for (Text split : splits)
        out.println(new String(Base64.encodeBase64(TextUtil.getBytes(split))));
      
      job.setNumReduceTasks(splits.size() + 1);
      out.close();
      
      job.setPartitionerClass(RangePartitioner.class);
      RangePartitioner.setSplitFile(job, workDir + "/splits.txt");
      
      job.waitForCompletion(true);
      Path failures = new Path(workDir, "failures");
      fs.delete(failures, true);
      fs.mkdirs(new Path(workDir, "failures"));
      connector.tableOperations().importDirectory(tableName, workDir + "/files", workDir + "/failures", false);
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (out != null)
        out.close();
    }
    
    return 0;
  }
  
  private int printUsage() {
    System.out.println("accumulo " + this.getClass().getName() + " <instanceName> <zooKeepers> <username> <password> <table> <input dir> <work dir> ");
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(CachedConfiguration.getInstance(), new BulkIngestExample(), args);
    System.exit(res);
  }
}
