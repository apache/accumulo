/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.examples.simple.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.util.HadoopCompatUtil;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.beust.jcommander.Parameter;


/**
 * Generate the *almost* official terasort input data set. (See below) The user specifies the number of rows and the output directory and this class runs a
 * map/reduce program to generate the data. The format of the data is:
 * <ul>
 * <li>(10 bytes key) (10 bytes rowid) (78 bytes filler) \r \n
 * <li>The keys are random characters from the set ' ' .. '~'.
 * <li>The rowid is the right justified row id as a int.
 * <li>The filler consists of 7 runs of 10 characters from 'A' to 'Z'.
 * </ul>
 *
 * This TeraSort is slightly modified to allow for variable length key sizes and value sizes. The row length isn't variable. To generate a terabyte of data in
 * the same way TeraSort does use 10000000000 rows and 10/10 byte key length and 78/78 byte value length. Along with the 10 byte row id and \r\n this gives you
 * 100 byte row * 10000000000 rows = 1tb. Min/Max ranges for key and value parameters are inclusive/inclusive respectively.
 *
 *
 */
public class TeraSortIngest extends Configured implements Tool {
  /**
   * An input format that assigns ranges of longs to each mapper.
   */
  static class RangeInputFormat extends InputFormat<LongWritable,NullWritable> {
    /**
     * An input split consisting of a range on numbers.
     */
    static class RangeInputSplit extends InputSplit implements Writable {
      long firstRow;
      long rowCount;

      public RangeInputSplit() {}

      public RangeInputSplit(long offset, long length) {
        firstRow = offset;
        rowCount = length;
      }

      @Override
      public long getLength() throws IOException {
        return 0;
      }

      @Override
      public String[] getLocations() throws IOException {
        return new String[] {};
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        firstRow = WritableUtils.readVLong(in);
        rowCount = WritableUtils.readVLong(in);
      }

      @Override
      public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, firstRow);
        WritableUtils.writeVLong(out, rowCount);
      }
    }

    /**
     * A record reader that will generate a range of numbers.
     */
    static class RangeRecordReader extends RecordReader<LongWritable,NullWritable> {
      long startRow;
      long finishedRows;
      long totalRows;

      LongWritable currentKey;

      public RangeRecordReader(RangeInputSplit split) {
        startRow = split.firstRow;
        finishedRows = 0;
        totalRows = split.rowCount;
      }

      @Override
      public void close() throws IOException {}

      @Override
      public float getProgress() throws IOException {
        return finishedRows / (float) totalRows;
      }

      @Override
      public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return new LongWritable(startRow + finishedRows);
      }

      @Override
      public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return NullWritable.get();
      }

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {}

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (finishedRows < totalRows) {
          ++finishedRows;
          return true;
        }
        return false;
      }
    }

    @Override
    public RecordReader<LongWritable,NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
      // reporter.setStatus("Creating record reader");
      return new RangeRecordReader((RangeInputSplit) split);
    }

    /**
     * Create the desired number of splits, dividing the number of rows between the mappers.
     */
    @Override
    public List<InputSplit> getSplits(JobContext job) {
      long totalRows = HadoopCompatUtil.getConfiguration(job).getLong(NUMROWS, 0);
      int numSplits = HadoopCompatUtil.getConfiguration(job).getInt(NUMSPLITS, 1);
      long rowsPerSplit = totalRows / numSplits;
      System.out.println("Generating " + totalRows + " using " + numSplits + " maps with step of " + rowsPerSplit);
      ArrayList<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
      long currentRow = 0;
      for (int split = 0; split < numSplits - 1; ++split) {
        splits.add(new RangeInputSplit(currentRow, rowsPerSplit));
        currentRow += rowsPerSplit;
      }
      splits.add(new RangeInputSplit(currentRow, totalRows - currentRow));
      System.out.println("Done Generating.");
      return splits;
    }

  }

  private static String NUMSPLITS = "terasort.overridesplits";
  private static String NUMROWS = "terasort.numrows";

  static class RandomGenerator {
    private long seed = 0;
    private static final long mask32 = (1l << 32) - 1;
    /**
     * The number of iterations separating the precomputed seeds.
     */
    private static final int seedSkip = 128 * 1024 * 1024;
    /**
     * The precomputed seed values after every seedSkip iterations. There should be enough values so that a 2**32 iterations are covered.
     */
    private static final long[] seeds = new long[] {0L, 4160749568L, 4026531840L, 3892314112L, 3758096384L, 3623878656L, 3489660928L, 3355443200L, 3221225472L,
        3087007744L, 2952790016L, 2818572288L, 2684354560L, 2550136832L, 2415919104L, 2281701376L, 2147483648L, 2013265920L, 1879048192L, 1744830464L,
        1610612736L, 1476395008L, 1342177280L, 1207959552L, 1073741824L, 939524096L, 805306368L, 671088640L, 536870912L, 402653184L, 268435456L, 134217728L,};

    /**
     * Start the random number generator on the given iteration.
     *
     * @param initalIteration
     *          the iteration number to start on
     */
    RandomGenerator(long initalIteration) {
      int baseIndex = (int) ((initalIteration & mask32) / seedSkip);
      seed = seeds[baseIndex];
      for (int i = 0; i < initalIteration % seedSkip; ++i) {
        next();
      }
    }

    RandomGenerator() {
      this(0);
    }

    long next() {
      seed = (seed * 3141592621l + 663896637) & mask32;
      return seed;
    }
  }

  /**
   * The Mapper class that given a row number, will generate the appropriate output line.
   */
  public static class SortGenMapper extends Mapper<LongWritable,NullWritable,Text,Mutation> {
    private Text table = null;
    private int minkeylength = 0;
    private int maxkeylength = 0;
    private int minvaluelength = 0;
    private int maxvaluelength = 0;

    private Text key = new Text();
    private Text value = new Text();
    private RandomGenerator rand;
    private byte[] keyBytes; // = new byte[12];
    private byte[] spaces = "          ".getBytes();
    private byte[][] filler = new byte[26][];
    {
      for (int i = 0; i < 26; ++i) {
        filler[i] = new byte[10];
        for (int j = 0; j < 10; ++j) {
          filler[i][j] = (byte) ('A' + i);
        }
      }
    }

    /**
     * Add a random key to the text
     */
    private Random random = new Random();

    private void addKey() {
      int range = random.nextInt(maxkeylength - minkeylength + 1);
      int keylen = range + minkeylength;
      int keyceil = keylen + (4 - (keylen % 4));
      keyBytes = new byte[keyceil];

      long temp = 0;
      for (int i = 0; i < keyceil / 4; i++) {
        temp = rand.next() / 52;
        keyBytes[3 + 4 * i] = (byte) (' ' + (temp % 95));
        temp /= 95;
        keyBytes[2 + 4 * i] = (byte) (' ' + (temp % 95));
        temp /= 95;
        keyBytes[1 + 4 * i] = (byte) (' ' + (temp % 95));
        temp /= 95;
        keyBytes[4 * i] = (byte) (' ' + (temp % 95));
      }
      key.set(keyBytes, 0, keylen);
    }

    /**
     * Add the rowid to the row.
     */
    private Text getRowIdString(long rowId) {
      Text paddedRowIdString = new Text();
      byte[] rowid = Integer.toString((int) rowId).getBytes();
      int padSpace = 10 - rowid.length;
      if (padSpace > 0) {
        paddedRowIdString.append(spaces, 0, 10 - rowid.length);
      }
      paddedRowIdString.append(rowid, 0, Math.min(rowid.length, 10));
      return paddedRowIdString;
    }

    /**
     * Add the required filler bytes. Each row consists of 7 blocks of 10 characters and 1 block of 8 characters.
     *
     * @param rowId
     *          the current row number
     */
    private void addFiller(long rowId) {
      int base = (int) ((rowId * 8) % 26);

      // Get Random var
      Random random = new Random(rand.seed);

      int range = random.nextInt(maxvaluelength - minvaluelength + 1);
      int valuelen = range + minvaluelength;

      while (valuelen > 10) {
        value.append(filler[(base + valuelen) % 26], 0, 10);
        valuelen -= 10;
      }

      if (valuelen > 0)
        value.append(filler[(base + valuelen) % 26], 0, valuelen);
    }

    @Override
    public void map(LongWritable row, NullWritable ignored, Context context) throws IOException, InterruptedException {
      context.setStatus("Entering");
      long rowId = row.get();
      if (rand == null) {
        // we use 3 random numbers per a row
        rand = new RandomGenerator(rowId * 3);
      }
      addKey();
      value.clear();
      // addRowId(rowId);
      addFiller(rowId);

      // New
      Mutation m = new Mutation(key);
      m.put(new Text("c"), // column family
          getRowIdString(rowId), // column qual
          new Value(value.toString().getBytes())); // data

      context.setStatus("About to add to accumulo");
      context.write(table, m);
      context.setStatus("Added to accumulo " + key.toString());
    }

    @Override
    public void setup(Context job) {
      minkeylength = job.getConfiguration().getInt("cloudgen.minkeylength", 0);
      maxkeylength = job.getConfiguration().getInt("cloudgen.maxkeylength", 0);
      minvaluelength = job.getConfiguration().getInt("cloudgen.minvaluelength", 0);
      maxvaluelength = job.getConfiguration().getInt("cloudgen.maxvaluelength", 0);
      table = new Text(job.getConfiguration().get("cloudgen.tablename"));
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new TeraSortIngest(), args);
  }

  static class Opts extends ClientOnRequiredTable {
    @Parameter(names = "--count", description = "number of rows to ingest", required = true)
    long numRows;
    @Parameter(names = {"-nk", "--minKeySize"}, description = "miniumum key size", required = true)
    int minKeyLength;
    @Parameter(names = {"-xk", "--maxKeySize"}, description = "maximum key size", required = true)
    int maxKeyLength;
    @Parameter(names = {"-nv", "--minValueSize"}, description = "minimum key size", required = true)
    int minValueLength;
    @Parameter(names = {"-xv", "--maxValueSize"}, description = "maximum key size", required = true)
    int maxValueLength;
    @Parameter(names = "--splits", description = "number of splits to create in the table")
    int splits = 0;
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = JobUtil.getJob(getConf());
    job.setJobName("TeraSortCloud");
    job.setJarByClass(this.getClass());
    Opts opts = new Opts();
    opts.parseArgs(TeraSortIngest.class.getName(), args);

    job.setInputFormatClass(RangeInputFormat.class);
    job.setMapperClass(SortGenMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Mutation.class);

    job.setNumReduceTasks(0);

    job.setOutputFormatClass(AccumuloOutputFormat.class);
    opts.setAccumuloConfigs(job);
    BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(10L * 1000 * 1000);
    AccumuloOutputFormat.setBatchWriterOptions(job, bwConfig);

    Configuration conf = job.getConfiguration();
    conf.setLong(NUMROWS, opts.numRows);
    conf.setInt("cloudgen.minkeylength", opts.minKeyLength);
    conf.setInt("cloudgen.maxkeylength", opts.maxKeyLength);
    conf.setInt("cloudgen.minvaluelength", opts.minValueLength);
    conf.setInt("cloudgen.maxvaluelength", opts.maxValueLength);
    conf.set("cloudgen.tablename", opts.tableName);

    if (args.length > 10)
      conf.setInt(NUMSPLITS, opts.splits);

    job.waitForCompletion(true);
    return job.isSuccessful() ? 0 : 1;
  }
}
