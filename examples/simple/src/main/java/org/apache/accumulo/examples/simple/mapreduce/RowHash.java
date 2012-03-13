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
import java.util.Collections;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RowHash extends Configured implements Tool {
  /**
   * The Mapper class that given a row number, will generate the appropriate output line.
   */
  public static class HashDataMapper extends Mapper<Key,Value,Text,Mutation> {
    public void map(Key row, Value data, Context context) throws IOException, InterruptedException {
      Mutation m = new Mutation(row.getRow());
      m.put(new Text("cf-HASHTYPE"), new Text("cq-MD5BASE64"), new Value(Base64.encodeBase64(MD5Hash.digest(data.toString()).getDigest())));
      context.write(null, m);
      context.progress();
    }
    
    @Override
    public void setup(Context job) {}
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job(getConf(), this.getClass().getName());
    job.setJarByClass(this.getClass());
    
    job.setInputFormatClass(AccumuloInputFormat.class);
    AccumuloInputFormat.setZooKeeperInstance(job.getConfiguration(), args[0], args[1]);
    AccumuloInputFormat.setInputInfo(job.getConfiguration(), args[2], args[3].getBytes(), args[4], new Authorizations());
    
    String col = args[5];
    int idx = col.indexOf(":");
    Text cf = new Text(idx < 0 ? col : col.substring(0, idx));
    Text cq = idx < 0 ? null : new Text(col.substring(idx + 1));
    if (cf.getLength() > 0)
      AccumuloInputFormat.fetchColumns(job.getConfiguration(), Collections.singleton(new Pair<Text,Text>(cf, cq)));
    
    // AccumuloInputFormat.setLogLevel(job, Level.TRACE);
    
    job.setMapperClass(HashDataMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Mutation.class);
    
    job.setNumReduceTasks(0);
    
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(), args[0], args[1]);
    AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), args[2], args[3].getBytes(), true, args[6]);
    // AccumuloOutputFormat.setLogLevel(job, Level.TRACE);
    
    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(CachedConfiguration.getInstance(), new RowHash(), args);
    if (res != 0)
      System.exit(res);
  }
}
