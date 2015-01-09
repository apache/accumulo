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
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.test.continuous.ContinuousIngest.BaseOpts;
import org.apache.accumulo.test.continuous.ContinuousIngest.ShortConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.validators.PositiveInteger;

/**
 * A map only job that reads a table created by continuous ingest and creates doubly linked list. This map reduce job tests the ability of a map only job to
 * read and write to accumulo at the same time. This map reduce job mutates the table in such a way that it should not create any undefined nodes.
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

    private static final ColumnVisibility EMPTY_VIS = new ColumnVisibility();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      int max_cf = context.getConfiguration().getInt(MAX_CF, -1);
      int max_cq = context.getConfiguration().getInt(MAX_CQ, -1);

      if (max_cf > Short.MAX_VALUE || max_cq > Short.MAX_VALUE)
        throw new IllegalArgumentException();

      this.max_cf = (short) max_cf;
      this.max_cq = (short) max_cq;

      random = new Random();
      ingestInstanceId = context.getConfiguration().get(CI_ID);
      iiId = ingestInstanceId.getBytes(UTF_8);

      count = 0;
    }

    @Override
    public void map(Key key, Value data, Context context) throws IOException, InterruptedException {

      ContinuousWalk.validate(key, data);

      if (WritableComparator.compareBytes(iiId, 0, iiId.length, data.get(), 0, iiId.length) != 0) {
        // only rewrite data not written by this M/R job
        byte[] val = data.get();

        int offset = ContinuousWalk.getPrevRowOffset(val);
        if (offset > 0) {
          long rowLong = Long.parseLong(new String(val, offset, 16, UTF_8), 16);
          Mutation m = ContinuousIngest.genMutation(rowLong, random.nextInt(max_cf), random.nextInt(max_cq), EMPTY_VIS, iiId, count++, key.getRowData()
              .toArray(), random, true);
          context.write(null, m);
        }

      } else {
        ContinuousVerify.increment(context.getCounter(Counts.SELF_READ));
      }
    }
  }

  static class Opts extends BaseOpts {
    @Parameter(names = "--maxColF", description = "maximum column family value to use", converter = ShortConverter.class)
    short maxColF = Short.MAX_VALUE;

    @Parameter(names = "--maxColQ", description = "maximum column qualifier value to use", converter = ShortConverter.class)
    short maxColQ = Short.MAX_VALUE;

    @Parameter(names = "--maxMappers", description = "the maximum number of mappers to use", required = true, validateWith = PositiveInteger.class)
    int maxMaps = 0;
  }

  @Override
  public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException, AccumuloSecurityException {
    Opts opts = new Opts();
    BatchWriterOpts bwOpts = new BatchWriterOpts();
    opts.parseArgs(ContinuousMoru.class.getName(), args, bwOpts);

    @SuppressWarnings("deprecation")
    Job job = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
    job.setJarByClass(this.getClass());

    job.setInputFormatClass(AccumuloInputFormat.class);
    opts.setAccumuloConfigs(job);

    // set up ranges
    try {
      Set<Range> ranges = opts.getConnector().tableOperations().splitRangeByTablets(opts.getTableName(), new Range(), opts.maxMaps);
      AccumuloInputFormat.setRanges(job, ranges);
      AccumuloInputFormat.setAutoAdjustRanges(job, false);
    } catch (Exception e) {
      throw new IOException(e);
    }

    job.setMapperClass(CMapper.class);

    job.setNumReduceTasks(0);

    job.setOutputFormatClass(AccumuloOutputFormat.class);
    AccumuloOutputFormat.setBatchWriterOptions(job, bwOpts.getBatchWriterConfig());

    Configuration conf = job.getConfiguration();
    conf.setLong(MIN, opts.min);
    conf.setLong(MAX, opts.max);
    conf.setInt(MAX_CF, opts.maxColF);
    conf.setInt(MAX_CQ, opts.maxColQ);
    conf.set(CI_ID, UUID.randomUUID().toString());

    job.waitForCompletion(true);
    opts.stopTracing();
    return job.isSuccessful() ? 0 : 1;
  }

  /**
   *
   * @param args
   *          instanceName zookeepers username password table columns outputpath
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(CachedConfiguration.getInstance(), new ContinuousMoru(), args);
    if (res != 0)
      System.exit(res);
  }
}
