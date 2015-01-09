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
package org.apache.accumulo.test.randomwalk.shard;

import java.util.Collection;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.partition.KeyRangePartitioner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

public class SortTool extends Configured implements Tool {
  protected final Logger log = Logger.getLogger(this.getClass());
  private String outputDir;
  private String seqFile;
  private String splitFile;
  private Collection<Text> splits;

  public SortTool(String seqFile, String outputDir, String splitFile, Collection<Text> splits) {
    this.outputDir = outputDir;
    this.seqFile = seqFile;
    this.splitFile = splitFile;
    this.splits = splits;
  }

  public int run(String[] args) throws Exception {
    @SuppressWarnings("deprecation")
    Job job = new Job(getConf(), this.getClass().getSimpleName());
    job.setJarByClass(this.getClass());

    if (job.getJar() == null) {
      log.error("M/R requires a jar file!  Run mvn package.");
      return 1;
    }

    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.setInputPaths(job, seqFile);

    job.setPartitionerClass(KeyRangePartitioner.class);
    KeyRangePartitioner.setSplitFile(job, splitFile);

    job.setMapOutputKeyClass(Key.class);
    job.setMapOutputValueClass(Value.class);

    job.setNumReduceTasks(splits.size() + 1);

    job.setOutputFormatClass(AccumuloFileOutputFormat.class);
    AccumuloFileOutputFormat.setOutputPath(job, new Path(outputDir));

    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }
}
