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
package org.apache.accumulo.examples.simple.filedata;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SummingArrayCombiner;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.simple.mapreduce.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;

/**
 * A MapReduce that computes a histogram of byte frequency for each file and stores the histogram alongside the file data. The {@link ChunkInputFormat} is used
 * to read the file data from Accumulo. See docs/examples/README.filedata for instructions.
 */
public class CharacterHistogram extends Configured implements Tool {
  public static final String VIS = "vis";

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new CharacterHistogram(), args));
  }

  public static class HistMapper extends Mapper<List<Entry<Key,Value>>,InputStream,Text,Mutation> {
    private ColumnVisibility cv;

    @Override
    public void map(List<Entry<Key,Value>> k, InputStream v, Context context) throws IOException, InterruptedException {
      Long[] hist = new Long[256];
      for (int i = 0; i < hist.length; i++)
        hist[i] = 0l;
      int b = v.read();
      while (b >= 0) {
        hist[b] += 1l;
        b = v.read();
      }
      v.close();
      Mutation m = new Mutation(k.get(0).getKey().getRow());
      m.put("info", "hist", cv, new Value(SummingArrayCombiner.STRING_ARRAY_ENCODER.encode(Arrays.asList(hist))));
      context.write(new Text(), m);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      cv = new ColumnVisibility(context.getConfiguration().get(VIS, ""));
    }
  }

  static class Opts extends ClientOnRequiredTable {
    @Parameter(names = "--vis")
    String visibilities = "";
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = JobUtil.getJob(getConf());
    job.setJobName(this.getClass().getSimpleName());
    job.setJarByClass(this.getClass());

    Opts opts = new Opts();
    opts.parseArgs(CharacterHistogram.class.getName(), args);

    job.setInputFormatClass(ChunkInputFormat.class);
    opts.setAccumuloConfigs(job);
    job.getConfiguration().set(VIS, opts.visibilities.toString());

    job.setMapperClass(HistMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Mutation.class);

    job.setNumReduceTasks(0);

    job.setOutputFormatClass(AccumuloOutputFormat.class);

    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }
}
