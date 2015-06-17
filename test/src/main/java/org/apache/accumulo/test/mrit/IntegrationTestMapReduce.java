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
package org.apache.accumulo.test.mrit;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTestMapReduce extends Configured implements Tool {

  private static final Logger log = LoggerFactory.getLogger(IntegrationTestMapReduce.class);

  public static class TestMapper extends Mapper<LongWritable,Text,IntWritable,Text> {

    @Override
    protected void map(LongWritable key, Text value, final Mapper<LongWritable,Text,IntWritable,Text>.Context context) throws IOException, InterruptedException {
      String className = value.toString();
      if (className.trim().isEmpty()) {
        return;
      }
      log.info("Running test {}", className);
      Class<? extends Object> test = null;
      try {
        test = Class.forName(className);
      } catch (ClassNotFoundException e) {
        log.debug("Error finding class {}", className, e);
        context.write(new IntWritable(-1), new Text(e.toString()));
        return;
      }
      JUnitCore core = new JUnitCore();
      core.addListener(new RunListener() {

        @Override
        public void testStarted(Description description) throws Exception {
          log.info("Starting {}", description);
          context.progress();
        }

        @Override
        public void testFinished(Description description) throws Exception {
          log.info("Finished {}", description);
          context.progress();
        }

        @Override
        public void testFailure(Failure failure) throws Exception {
          log.info("Test failed: {}", failure.getDescription(), failure.getException());
          context.progress();
        }

      });
      context.setStatus(test.getSimpleName());
      try {
        Result result = core.run(test);
        if (result.wasSuccessful()) {
          log.info("{} was successful", className);
          context.write(new IntWritable(0), value);
        } else {
          log.info("{} failed", className);
          context.write(new IntWritable(1), value);
        }
      } catch (Exception e) {
        // most likely JUnit issues, like no tests to run
        log.info("Test failed: {}", className, e);
      }
    }
  }

  public static class TestReducer extends Reducer<IntWritable,Text,IntWritable,Text> {

    @Override
    protected void reduce(IntWritable code, Iterable<Text> tests, Reducer<IntWritable,Text,IntWritable,Text>.Context context) throws IOException,
        InterruptedException {
      StringBuffer result = new StringBuffer();
      for (Text test : tests) {
        result.append(test);
        result.append("\n");
      }
      context.write(code, new Text(result.toString()));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    // read a list of tests from the input, and print out the results
    if (args.length != 2) {
      System.err.println("Wrong number of args: <input> <output>");
    }
    Configuration conf = getConf();
    Job job = Job.getInstance(conf, "accumulo integration test runner");
    // read one line at a time
    job.setInputFormatClass(NLineInputFormat.class);
    conf.setInt(NLineInputFormat.LINES_PER_MAP, 1);

    // run the test
    job.setJarByClass(IntegrationTestMapReduce.class);
    job.setMapperClass(TestMapper.class);

    // group test by result code
    job.setReducerClass(TestReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new IntegrationTestMapReduce(), args));
  }

}
