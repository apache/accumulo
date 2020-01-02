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

package org.apache.accumulo.test.continuous;

import java.io.BufferedOutputStream;
import java.io.PrintStream;
import java.util.Base64;
import java.util.Collection;
import java.util.UUID;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.MapReduceClientOnDefaultTable;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.KeyRangePartitioner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.validators.PositiveInteger;

/**
 * Bulk import a million random key value pairs. Same format as ContinuousIngest and can be verified
 * by running ContinuousVerify.
 */
public class BulkIngest extends Configured implements Tool {
  static class Opts extends ContinuousOpts {
    @Parameter(names = "--dir", description = "the bulk dir to use", required = true)
    String dir;

    @Parameter(names = "--reducers", description = "the number of reducers to use",
        validateWith = PositiveInteger.class)
    int reducers = 10;

    @Parameter(names = "--mapTasks", description = "the number of map tasks to use",
        validateWith = PositiveInteger.class)
    int mapTasks = 10;

    @Parameter(names = "--mapNodes",
        description = "the number of linked list key value nodes per mapper",
        validateWith = PositiveInteger.class)
    int mapNodes = 1000;
  }

  public static final Logger log = LoggerFactory.getLogger(BulkIngest.class);

  @Override
  public int run(String[] args) throws Exception {
    String ingestInstanceId = UUID.randomUUID().toString();

    Job job = Job.getInstance(getConf());
    job.setJobName("BulkIngest_" + ingestInstanceId);
    job.setJarByClass(BulkIngest.class);
    // very important to prevent guava conflicts
    job.getConfiguration().set("mapreduce.job.classloader", "true");
    FileSystem fs = FileSystem.get(job.getConfiguration());

    log.info(String.format("UUID %d %s", System.currentTimeMillis(), ingestInstanceId));

    job.setInputFormatClass(ContinuousInputFormat.class);

    // map the generated random longs to key values
    job.setMapOutputKeyClass(Key.class);
    job.setMapOutputValueClass(Value.class);

    Opts opts = new Opts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    MapReduceClientOnDefaultTable clientOpts = new MapReduceClientOnDefaultTable("ci");
    clientOpts.parseArgs(BulkIngest.class.getName(), args, bwOpts, opts);

    fs.mkdirs(new Path(opts.dir));

    // output RFiles for the import
    job.setOutputFormatClass(AccumuloFileOutputFormat.class);

    AccumuloFileOutputFormat.setOutputPath(job, new Path(opts.dir + "/files"));

    ContinuousInputFormat.configure(job.getConfiguration(), ingestInstanceId, opts);

    String tableName = clientOpts.getTableName();

    // create splits file for KeyRangePartitioner
    String splitsFile = opts.dir + "/splits.txt";

    // make sure splits file is closed before continuing
    try (PrintStream out =
        new PrintStream(new BufferedOutputStream(fs.create(new Path(splitsFile))))) {
      Collection<Text> splits =
          clientOpts.getConnector().tableOperations().listSplits(tableName, opts.reducers - 1);
      for (Text split : splits) {
        out.println(Base64.getEncoder().encodeToString(split.copyBytes()));
      }
      job.setNumReduceTasks(splits.size() + 1);
    }

    job.setPartitionerClass(KeyRangePartitioner.class);
    KeyRangePartitioner.setSplitFile(job, fs.getUri() + splitsFile);

    job.waitForCompletion(true);
    boolean success = job.isSuccessful();

    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new BulkIngest(), args);
    System.exit(ret);
  }
}
