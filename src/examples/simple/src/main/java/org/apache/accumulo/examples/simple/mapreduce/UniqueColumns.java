package org.apache.accumulo.examples.simple.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
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

/**
 * A simple map reduce job that computes the unique column families and column qualifiers in a table.  This example shows one way to run against an offline table.
 */
public class UniqueColumns extends Configured implements Tool {
  
  private static final Text EMPTY = new Text();
  
  public static class UMapper extends Mapper<Key,Value,Text,Text> {    
    private Text temp = new Text();
    private static final Text CF = new Text("cf:");
    private static final Text CQ = new Text("cq:");
    
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
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      context.write(key, EMPTY);
    }
  }
  
  
  @Override
  public int run(String[] args) throws Exception {
    
    if (args.length != 8) {
      throw new IllegalArgumentException("Usage : " + UniqueColumns.class.getSimpleName()
          + " <instance name> <zookeepers> <user> <password> <table> <output directory> <num reducers> offline|online");
    }

    boolean scanOffline = args[7].equals("offline");
    String table = args[4];
    String jobName = this.getClass().getSimpleName() + "_" + System.currentTimeMillis();
    
    Job job = new Job(getConf(), jobName);
    job.setJarByClass(this.getClass());

    String clone = table;
    Connector conn = null;
    if (scanOffline) {
      /*
       * this example clones the table and takes it offline. If you plan to run map reduce jobs over a table many times, it may be more efficient to compact the
       * table, clone it, and then keep using the same clone as input for map reduce.
       */
      
      ZooKeeperInstance zki = new ZooKeeperInstance(args[0], args[1]);
      conn = zki.getConnector(args[2], args[3].getBytes());
      clone = table + "_" + jobName;
      conn.tableOperations().clone(table, clone, true, new HashMap<String,String>(), new HashSet<String>());
      conn.tableOperations().offline(clone);
      
      AccumuloInputFormat.setScanOffline(job.getConfiguration(), true);
    }
    

    
    job.setInputFormatClass(AccumuloInputFormat.class);
    AccumuloInputFormat.setZooKeeperInstance(job.getConfiguration(), args[0], args[1]);
    AccumuloInputFormat.setInputInfo(job.getConfiguration(), args[2], args[3].getBytes(), clone, new Authorizations());
    
    
    job.setMapperClass(UMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setCombinerClass(UReducer.class);
    job.setReducerClass(UReducer.class);

    job.setNumReduceTasks(Integer.parseInt(args[6]));

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path(args[5]));
    
    job.waitForCompletion(true);
    
    if (scanOffline) {
      conn.tableOperations().delete(clone);
    }

    return job.isSuccessful() ? 0 : 1;
  }
  
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(CachedConfiguration.getInstance(), new UniqueColumns(), args);
    System.exit(res);
  }
}
