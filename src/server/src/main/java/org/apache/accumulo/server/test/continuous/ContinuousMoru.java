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
import java.util.Random;
import java.util.UUID;

import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A map only job that reads a table created by continuous ingest and creates doubly linked list. This map reduce job test ability of a map only job to read and
 * write to accumulo at the same time. This map reduce job mutates the table in such a way that it should not create any undefined nodes.
 * 
 */
public class ContinuousMoru extends Configured implements Tool {
  private static final String PREFIX = ContinuousMoru.class.getSimpleName() + ".";
  private static final String MAX_CQ = PREFIX + "MAX_CQ";
  private static final String MAX_CF = PREFIX + "MAX_CF";
  private static final String MAX = PREFIX + "MAX";
  private static final String MIN = PREFIX + "MIN";
  private static final String CI_ID = PREFIX + "CI_ID";
  
  static enum Counts {
    SELF_READ;
  }
  
  public static class CMapper extends Mapper<Key,Value,Text,Mutation> {
    
    private short max_cf;
    private short max_cq;
    private Random random;
    private String ingestInstanceId;
    private byte[] iiId;
    private long count;
    
    public void setup(Context context) throws IOException, InterruptedException {
      int max_cf = context.getConfiguration().getInt(MAX_CF, -1);
      int max_cq = context.getConfiguration().getInt(MAX_CQ, -1);
      
      if (max_cf > Short.MAX_VALUE || max_cq > Short.MAX_VALUE)
        throw new IllegalArgumentException();
      
      this.max_cf = (short) max_cf;
      this.max_cq = (short) max_cq;
      
      random = new Random();
      ingestInstanceId = context.getConfiguration().get(CI_ID);
      iiId = ingestInstanceId.getBytes();
      
      count = 0;
    }
    
    public void map(Key key, Value data, Context context) throws IOException, InterruptedException {
      
      ContinuousWalk.validate(key, data);
      
      if (WritableComparator.compareBytes(iiId, 0, iiId.length, data.get(), 0, iiId.length) != 0) {
        // only rewrite data not written by this M/R job
        byte[] val = data.get();
        
        int offset = ContinuousWalk.getPrevRowOffset(val);
        if (offset > 0) {
          long rowLong = Long.parseLong(new String(val, offset, 16), 16);
          Mutation m = ContinuousIngest.genMutation(rowLong, random.nextInt(max_cf), random.nextInt(max_cq), iiId, count++, key.getRowData().toArray(), random,
              true);
          context.write(null, m);
        }
        
      } else {
        context.getCounter(Counts.SELF_READ).increment(1);
      }
    }
  }
  
  @Override
  public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    if (args.length != 13) {
      throw new IllegalArgumentException("Usage : " + ContinuousMoru.class.getName()
          + " <instance name> <zookeepers> <user> <pass> <table> <min> <max> <max cf> <max cq> <max mem> <max latency> <num threads> <max maps>");
    }
    
    String instance = args[0];
    String zookeepers = args[1];
    String user = args[2];
    String pass = args[3];
    String table = args[4];
    String min = args[5];
    String max = args[6];
    String max_cf = args[7];
    String max_cq = args[8];
    String maxMem = args[9];
    String maxLatency = args[10];
    String numThreads = args[11];
    String maxMaps = args[12];
    
    Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
    job.setJarByClass(this.getClass());
    
    job.setInputFormatClass(AccumuloInputFormat.class);
    AccumuloInputFormat.setInputInfo(job.getConfiguration(), user, pass.getBytes(), table, new Authorizations());
    AccumuloInputFormat.setZooKeeperInstance(job.getConfiguration(), instance, zookeepers);
    
    // set up ranges
    try {
      AccumuloInputFormat.setRanges(job.getConfiguration(), new ZooKeeperInstance(instance, zookeepers).getConnector(user, pass.getBytes()).tableOperations()
          .splitRangeByTablets(table, new Range(), Integer.parseInt(maxMaps)));
      AccumuloInputFormat.disableAutoAdjustRanges(job.getConfiguration());
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    job.setMapperClass(CMapper.class);
    
    job.setNumReduceTasks(0);
    
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), user, pass.getBytes(), false, table);
    AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(), instance, zookeepers);
    AccumuloOutputFormat.setMaxLatency(job.getConfiguration(), (int) (Integer.parseInt(maxLatency) / 1000.0));
    AccumuloOutputFormat.setMaxMutationBufferSize(job.getConfiguration(), Long.parseLong(maxMem));
    AccumuloOutputFormat.setMaxWriteThreads(job.getConfiguration(), Integer.parseInt(numThreads));
    
    Configuration conf = job.getConfiguration();
    conf.setLong(MIN, Long.parseLong(min));
    conf.setLong(MAX, Long.parseLong(max));
    conf.setInt(MAX_CF, Integer.parseInt(max_cf));
    conf.setInt(MAX_CQ, Integer.parseInt(max_cq));
    conf.set(CI_ID, UUID.randomUUID().toString());
    
    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }
  
  /**
   * 
   * @param args
   *          instanceName zookeepers username password table columns outputpath
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(CachedConfiguration.getInstance(), new ContinuousMoru(), args);
    if (res != 0)
      System.exit(res);
  }
}
