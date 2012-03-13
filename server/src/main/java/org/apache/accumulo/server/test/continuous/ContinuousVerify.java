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
package org.apache.accumulo.server.test.continuous;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.test.continuous.ContinuousWalk.BadChecksumException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A map reduce job that verifies a table created by continuous ingest. It verifies that all referenced nodes are defined.
 */

public class ContinuousVerify extends Configured implements Tool {
  
  public static final VLongWritable DEF = new VLongWritable(-1);
  
  public static class CMapper extends Mapper<Key,Value,LongWritable,VLongWritable> {
    
    private LongWritable row = new LongWritable();
    private LongWritable ref = new LongWritable();
    private VLongWritable vrow = new VLongWritable();
    
    private long corrupt = 0;
    
    public void map(Key key, Value data, Context context) throws IOException, InterruptedException {
      long r = Long.parseLong(key.getRow().toString(), 16);
      if (r < 0)
        throw new IllegalArgumentException();
      
      try {
        ContinuousWalk.validate(key, data);
      } catch (BadChecksumException bce) {
        context.getCounter(Counts.CORRUPT).increment(1);
        if (corrupt < 1000) {
          System.out.println("ERROR Bad checksum : " + key);
        } else if (corrupt == 1000) {
          System.out.println("Too many bad checksums, not printing anymore!");
        }
        corrupt++;
        return;
      }
      
      row.set(r);
      
      context.write(row, DEF);
      byte[] val = data.get();
      
      int offset = ContinuousWalk.getPrevRowOffset(val);
      if (offset > 0) {
        ref.set(Long.parseLong(new String(val, offset, 16), 16));
        vrow.set(r);
        context.write(ref, vrow);
      }
    }
  }
  
  public static enum Counts {
    UNREFERENCED, UNDEFINED, REFERENCED, CORRUPT
  }
  
  public static class CReducer extends Reducer<LongWritable,VLongWritable,Text,Text> {
    private ArrayList<Long> refs = new ArrayList<Long>();
    
    public void reduce(LongWritable key, Iterable<VLongWritable> values, Context context) throws IOException, InterruptedException {
      
      int defCount = 0;
      
      refs.clear();
      for (VLongWritable type : values) {
        if (type.get() == -1) {
          defCount++;
        } else {
          refs.add(type.get());
        }
      }
      
      if (defCount == 0 && refs.size() > 0) {
        StringBuilder sb = new StringBuilder();
        String comma = "";
        for (Long ref : refs) {
          sb.append(comma);
          comma = ",";
          sb.append(new String(ContinuousIngest.genRow(ref)));
        }
        
        context.write(new Text(ContinuousIngest.genRow(key.get())), new Text(sb.toString()));
        context.getCounter(Counts.UNDEFINED).increment(1);
        
      } else if (defCount > 0 && refs.size() == 0) {
        context.getCounter(Counts.UNREFERENCED).increment(1);
      } else {
        context.getCounter(Counts.REFERENCED).increment(1);
      }
      
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 9) {
      throw new IllegalArgumentException("Usage : " + ContinuousVerify.class.getName()
          + " <instance name> <zookeepers> <user> <pass> <table> <output dir> <max mappers> <num reducers> <scan offline>");
    }
    
    String instance = args[0];
    String zookeepers = args[1];
    String user = args[2];
    String pass = args[3];
    String table = args[4];
    String outputdir = args[5];
    String maxMaps = args[6];
    String reducers = args[7];
    boolean scanOffline = Boolean.parseBoolean(args[8]);
    
    Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
    job.setJarByClass(this.getClass());
    
    String clone = table;
    Connector conn = null;
    if (scanOffline) {
      Random random = new Random();
      clone = table + "_" + String.format("%016x", Math.abs(random.nextLong()));
      ZooKeeperInstance zki = new ZooKeeperInstance(instance, zookeepers);
      conn = zki.getConnector(user, pass.getBytes());
      conn.tableOperations().clone(table, clone, true, new HashMap<String,String>(), new HashSet<String>());
      conn.tableOperations().offline(clone);
    }

    job.setInputFormatClass(AccumuloInputFormat.class);
    AccumuloInputFormat.setInputInfo(job.getConfiguration(), user, pass.getBytes(), clone, new Authorizations());
    AccumuloInputFormat.setZooKeeperInstance(job.getConfiguration(), instance, zookeepers);
    AccumuloInputFormat.setScanOffline(job.getConfiguration(), scanOffline);
    // set up ranges
    try {
      Set<Range> ranges = new ZooKeeperInstance(instance, zookeepers).getConnector(user, pass.getBytes()).tableOperations()
          .splitRangeByTablets(table, new Range(), Integer.parseInt(maxMaps));
      AccumuloInputFormat.setRanges(job.getConfiguration(), ranges);
      AccumuloInputFormat.disableAutoAdjustRanges(job.getConfiguration());
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    job.setMapperClass(CMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(VLongWritable.class);
    
    job.setReducerClass(CReducer.class);
    job.setNumReduceTasks(Integer.parseInt(reducers));
    
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", scanOffline);

    TextOutputFormat.setOutputPath(job, new Path(outputdir));
    
    job.waitForCompletion(true);
    
    if (scanOffline) {
      conn.tableOperations().delete(clone);
    }

    return job.isSuccessful() ? 0 : 1;
  }
  
  /**
   * 
   * @param args
   *          instanceName zookeepers username password table columns outputpath
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(CachedConfiguration.getInstance(), new ContinuousVerify(), args);
    if (res != 0)
      System.exit(res);
  }
}
