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
package org.apache.accumulo.server.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.logger.IdentityReducer;
import org.apache.accumulo.server.logger.LogFileKey;
import org.apache.accumulo.server.logger.LogFileValue;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/* Borrows from the Apache sort example program */
public class LogSort extends Configured implements Tool {
  
  private static final Logger log = Logger.getLogger(LogSort.class);
  public static final String INSTANCE_ID_PROPERTY = "accumulo.instance.id";
  private Job job = null;
  
  public static String getJobName() {
    return "LogSort_" + HdfsZooInstance.getInstance().getInstanceID();
  }
  
  private void printUsage() {
    System.out.println("accumulo " + this.getClass().getName() + " [-r <reducers>] [-q queue] [-p pool] <input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
  }
  
  public static class SortCommit extends FileOutputCommitter {
    
    final private Path outputPath;
    final private FileSystem outputFileSystem;
    
    public SortCommit(Path outputPath, TaskAttemptContext context) throws IOException {
      super(outputPath, context);
      this.outputPath = outputPath;
      outputFileSystem = outputPath.getFileSystem(context.getConfiguration());
    }
    
    @Override
    public void abortTask(TaskAttemptContext context) {
      try {
        super.abortTask(context);
        outputFileSystem.delete(outputPath, true);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    
    @Override
    public void cleanupJob(JobContext context) throws IOException {
      super.cleanupJob(context);
      int parts = 0;
      if (outputFileSystem.exists(outputPath)) {
        for (FileStatus status : outputFileSystem.listStatus(outputPath)) {
          if (status.getPath().getName().startsWith("part")) {
            parts++;
          }
        }
      }
      if (parts != context.getNumReduceTasks() || !outputFileSystem.createNewFile(new Path(outputPath, "finished"))) {
        log.error("Unable to create finished flag file");
        outputFileSystem.delete(outputPath, true);
      }
    }
  }
  
  /**
   * The main driver for sort program. Invoke this method to submit the map/reduce job.
   */
  public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    if (job != null)
      throw new RuntimeException("job has already run");
    
    // parse options
    int num_reduces = 1;
    String queueName = "default";
    String poolName = "recovery";
    List<String> otherArgs = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-r".equals(args[i]))
          num_reduces = Integer.parseInt(args[++i]);
        else if ("-q".equals(args[i]))
          queueName = args[++i];
        else if ("-p".equals(args[i]))
          poolName = args[++i];
        else
          otherArgs.add(args[i]);
      } catch (NumberFormatException e) {
        log.error("Integer expected instead of " + args[i], e);
        printUsage();
        return 1;
      } catch (ArrayIndexOutOfBoundsException e) {
        log.error("Required parameter missing from " + args[i - 1], e);
        printUsage();
        return 1;
      }
    }
    
    // validate arguments
    if (otherArgs.size() != 2) {
      log.error("Wrong number of parameters: " + otherArgs.size() + " instead of 2.", new Exception());
      printUsage();
      return 0;
    }
    
    // create job
    job = new Job(getConf(), getJobName());
    job.setJarByClass(this.getClass());
    
    // set input
    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.setInputPaths(job, otherArgs.get(0));
    
    // set identity mappers
    job.setMapperClass(Mapper.class);
    job.setOutputKeyClass(LogFileKey.class);
    job.setOutputValueClass(LogFileValue.class);
    
    // set custom partitioner
    job.setPartitionerClass(RoundRobinPartitioner.class);
    
    // set identity reducer
    job.setReducerClass(IdentityReducer.class);
    job.setNumReduceTasks(num_reduces);
    
    // set output
    job.setOutputFormatClass(LoggerMapFileOutputFormat.class);
    LoggerMapFileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
    
    // submit the job to the job queue
    job.getConfiguration().set("mapred.job.queue.name", queueName);
    job.getConfiguration().set("mapred.job.pool.name", poolName);
    job.getConfiguration().set(INSTANCE_ID_PROPERTY, HdfsZooInstance.getInstance().getInstanceID());
    log.info("Running on some nodes to sort from " + SequenceFileInputFormat.getInputPaths(job)[0] + " into " + AccumuloFileOutputFormat.getOutputPath(job)
        + " with " + num_reduces + " reduces.");
    return 0;
  }
  
  public static Job startSort(boolean background, String[] args) throws Exception {
    LogSort sort = new LogSort();
    ToolRunner.run(CachedConfiguration.getInstance(), sort, args);
    if (background)
      sort.job.submit();
    else
      sort.job.waitForCompletion(true);
    return sort.job;
  }
  
  public static void main(String[] args) throws Exception {
    long startTime = System.currentTimeMillis();
    log.info("Job started");
    Job job = startSort(false, args);
    log.info("The job finished after " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds.");
    if (!job.isSuccessful())
      System.exit(1);
  }
  
  private static class LoggerMapFileOutputFormat extends FileOutputFormat<WritableComparable<?>,Writable> {
    @Override
    public RecordWriter<WritableComparable<?>,Writable> getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException {
      // get the path of the temporary output file
      Path file = getDefaultWorkFile(job, "");
      
      FileSystem fs = file.getFileSystem(job.getConfiguration());
      CompressionCodec codec = null;
      CompressionType compressionType = CompressionType.NONE;
      if (getCompressOutput(job)) {
        // find the kind of compression to do
        compressionType = SequenceFileOutputFormat.getOutputCompressionType(job);
        
        // find the right codec
        Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, DefaultCodec.class);
        codec = ReflectionUtils.newInstance(codecClass, job.getConfiguration());
      }
      
      Progressable progress = new Progressable() {
        @Override
        public void progress() {
          job.progress();
        }
      };
      final MapFile.Writer out = new MapFile.Writer(job.getConfiguration(), fs, file.toString(), job.getOutputKeyClass().asSubclass(WritableComparable.class),
          job.getOutputValueClass().asSubclass(Writable.class), compressionType, codec, progress);
      return new RecordWriter<WritableComparable<?>,Writable>() {
        
        @Override
        public void write(WritableComparable<?> key, Writable value) throws IOException {
          out.append(key, value);
        }
        
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
          out.close();
        }
      };
      
    }
    
    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
      return new SortCommit(getOutputPath(context), context);
    }
  }
}
