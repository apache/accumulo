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
package org.apache.accumulo.examples.simple.helloworld;

import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.ContextFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Inserts 10K rows (50K entries) into accumulo with each row having 5 entries using an OutputFormat.
 */
public class InsertWithOutputFormat extends Configured implements Tool {
  // this is a tool because when you run a mapreduce, you will need to use the
  // ToolRunner
  // if you want libjars to be passed properly to the map and reduce tasks
  // even though this class isn't a mapreduce
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 5) {
      System.out.println("Usage: bin/tool.sh " + this.getClass().getName() + " <instance name> <zoo keepers> <username> <password> <tablename>");
      return 1;
    }
    Text tableName = new Text(args[4]);
    Job job = new Job(getConf());
    AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(), args[0], args[1]);
    AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), args[2], args[3].getBytes(), true, null);
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    
    // when running a mapreduce, you won't need to instantiate the output
    // format and record writer
    // mapreduce will do that for you, and you will just use
    // output.collect(tableName, mutation)
    TaskAttemptContext context = ContextFactory.createTaskAttemptContext(job);
    RecordWriter<Text,Mutation> rw = new AccumuloOutputFormat().getRecordWriter(context);
    
    Text colf = new Text("colfam");
    System.out.println("writing ...");
    for (int i = 0; i < 10000; i++) {
      Mutation m = new Mutation(new Text(String.format("row_%d", i)));
      for (int j = 0; j < 5; j++) {
        m.put(colf, new Text(String.format("colqual_%d", j)), new Value((String.format("value_%d_%d", i, j)).getBytes()));
      }
      rw.write(tableName, m); // repeat until done
      if (i % 100 == 0)
        System.out.println(i);
    }
    
    rw.close(context); // close when done
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(CachedConfiguration.getInstance(), new InsertWithOutputFormat(), args));
  }
}
