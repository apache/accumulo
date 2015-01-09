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

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.test.continuous.ContinuousWalk.BadChecksumException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.validators.PositiveInteger;

/**
 * A map reduce job that verifies a table created by continuous ingest. It verifies that all referenced nodes are defined.
 */

public class ContinuousVerify extends Configured implements Tool {

  // work around hadoop-1/hadoop-2 runtime incompatibility
  static private Method INCREMENT;
  static {
    try {
      INCREMENT = Counter.class.getMethod("increment", Long.TYPE);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  static void increment(Object obj) {
    try {
      INCREMENT.invoke(obj, 1L);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public static final VLongWritable DEF = new VLongWritable(-1);

  public static class CMapper extends Mapper<Key,Value,LongWritable,VLongWritable> {

    private static final Logger log = Logger.getLogger(CMapper.class);
    private LongWritable row = new LongWritable();
    private LongWritable ref = new LongWritable();
    private VLongWritable vrow = new VLongWritable();

    private long corrupt = 0;

    @Override
    public void map(Key key, Value data, Context context) throws IOException, InterruptedException {
      long r = Long.parseLong(key.getRow().toString(), 16);
      if (r < 0)
        throw new IllegalArgumentException();

      try {
        ContinuousWalk.validate(key, data);
      } catch (BadChecksumException bce) {
        increment(context.getCounter(Counts.CORRUPT));
        if (corrupt < 1000) {
          log.error("Bad checksum : " + key);
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
        ref.set(Long.parseLong(new String(val, offset, 16, UTF_8), 16));
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

    @Override
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
          sb.append(new String(ContinuousIngest.genRow(ref), UTF_8));
        }

        context.write(new Text(ContinuousIngest.genRow(key.get())), new Text(sb.toString()));
        increment(context.getCounter(Counts.UNDEFINED));

      } else if (defCount > 0 && refs.size() == 0) {
        increment(context.getCounter(Counts.UNREFERENCED));
      } else {
        increment(context.getCounter(Counts.REFERENCED));
      }

    }
  }

  static class Opts extends ClientOnDefaultTable {
    @Parameter(names = "--output", description = "location in HDFS to store the results; must not exist", required = true)
    String outputDir = "/tmp/continuousVerify";

    @Parameter(names = "--maxMappers", description = "the maximum number of mappers to use", required = true, validateWith = PositiveInteger.class)
    int maxMaps = 0;

    @Parameter(names = "--reducers", description = "the number of reducers to use", required = true, validateWith = PositiveInteger.class)
    int reducers = 0;

    @Parameter(names = "--offline", description = "perform the verification directly on the files while the table is offline")
    boolean scanOffline = false;

    public Opts() {
      super("ci");
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(this.getClass().getName(), args);

    @SuppressWarnings("deprecation")
    Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
    job.setJarByClass(this.getClass());

    job.setInputFormatClass(AccumuloInputFormat.class);
    opts.setAccumuloConfigs(job);

    Set<Range> ranges = null;
    String clone = opts.getTableName();
    Connector conn = null;

    if (opts.scanOffline) {
      Random random = new Random();
      clone = opts.getTableName() + "_" + String.format("%016x", (random.nextLong() & 0x7fffffffffffffffl));
      conn = opts.getConnector();
      conn.tableOperations().clone(opts.getTableName(), clone, true, new HashMap<String,String>(), new HashSet<String>());
      ranges = conn.tableOperations().splitRangeByTablets(opts.getTableName(), new Range(), opts.maxMaps);
      conn.tableOperations().offline(clone);
      AccumuloInputFormat.setInputTableName(job, clone);
      AccumuloInputFormat.setOfflineTableScan(job, true);
    } else {
      ranges = opts.getConnector().tableOperations().splitRangeByTablets(opts.getTableName(), new Range(), opts.maxMaps);
    }

    AccumuloInputFormat.setRanges(job, ranges);
    AccumuloInputFormat.setAutoAdjustRanges(job, false);

    job.setMapperClass(CMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(VLongWritable.class);

    job.setReducerClass(CReducer.class);
    job.setNumReduceTasks(opts.reducers);

    job.setOutputFormatClass(TextOutputFormat.class);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", opts.scanOffline);

    TextOutputFormat.setOutputPath(job, new Path(opts.outputDir));

    job.waitForCompletion(true);

    if (opts.scanOffline) {
      conn.tableOperations().delete(clone);
    }
    opts.stopTracing();
    return job.isSuccessful() ? 0 : 1;
  }

  /**
   *
   * @param args
   *          instanceName zookeepers username password table columns outputpath
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(CachedConfiguration.getInstance(), new ContinuousVerify(), args);
    if (res != 0)
      System.exit(res);
  }
}
