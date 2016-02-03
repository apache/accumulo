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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
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

/**
 * Run the Integration Tests as a Map-Reduce job.
 * <p>
 * Each of the Integration tests takes 30s to 20m to run. Using a larger cluster, all the tests can be run in parallel and finish much faster.
 * <p>
 * To run the tests, you first need a list of the tests. A simple way to get a list, is to scan the accumulo-test jar file for them.
 *
 * <pre>
 * $ jar -tf lib/accumulo-test.jar | grep IT.class | tr / . | sed -e 's/.class$//' &gt;tests
 * </pre>
 *
 * Put the list of tests into HDFS:
 *
 * <pre>
 * $ hadoop fs -mkdir /tmp
 * $ hadoop fs -put tests /tmp/tests
 * </pre>
 *
 * Run the class below as a map-reduce job, giving it the lists of tests, and a place to store the results.
 *
 * <pre>
 * $ yarn jar lib/accumulo-test-mrit.jar -libjars lib/native/libaccumulo.so /tmp/tests /tmp/results
 * </pre>
 *
 * The result is a list of IT classes that pass or fail. Those classes that fail will be annotated with the particular test that failed within the class.
 */

public class IntegrationTestMapReduce extends Configured implements Tool {

  private static final Logger log = LoggerFactory.getLogger(IntegrationTestMapReduce.class);

  private static boolean isMapReduce = false;

  public static boolean isMapReduce() {
    return isMapReduce;
  }

  public static class TestMapper extends Mapper<LongWritable,Text,Text,Text> {

    static final Text FAIL = new Text("FAIL");
    static final Text PASS = new Text("PASS");
    static final Text ERROR = new Text("ERROR");

    public static enum TestCounts {
      PASS, FAIL, ERROR
    }

    @Override
    protected void map(LongWritable key, Text value, final Mapper<LongWritable,Text,Text,Text>.Context context) throws IOException, InterruptedException {
      isMapReduce = true;
      String className = value.toString();
      if (className.trim().isEmpty()) {
        return;
      }
      final List<String> failures = new ArrayList<>();
      Class<? extends Object> test = null;
      try {
        test = Class.forName(className);
      } catch (ClassNotFoundException e) {
        log.debug("Error finding class {}", className, e);
        context.getCounter(TestCounts.ERROR).increment(1);
        context.write(ERROR, new Text(e.toString()));
        return;
      }
      log.info("Running test {}", className);
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
          failures.add(failure.getDescription().getMethodName());
          context.progress();
        }

      });
      context.setStatus(test.getSimpleName());
      try {
        Result result = core.run(test);
        if (result.wasSuccessful()) {
          log.info("{} was successful", className);
          context.getCounter(TestCounts.PASS).increment(1);
          context.write(PASS, value);
        } else {
          log.info("{} failed", className);
          context.getCounter(TestCounts.FAIL).increment(1);
          context.write(FAIL, new Text(className + "(" + StringUtils.join(failures, ", ") + ")"));
        }
      } catch (Exception e) {
        // most likely JUnit issues, like no tests to run
        log.info("Test failed: {}", className, e);
      }
    }
  }

  public static class TestReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text code, Iterable<Text> tests, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
      StringBuilder result = new StringBuilder("\n");
      for (Text test : tests) {
        result.append("   ");
        result.append(test.toString());
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
      return 1;
    }
    Configuration conf = getConf();
    Job job = Job.getInstance(conf, "accumulo integration test runner");
    conf = job.getConfiguration();

    // some tests take more than 10 minutes
    conf.setLong(MRJobConfig.TASK_TIMEOUT, 20 * 60 * 1000);

    // minicluster uses a lot of ram
    conf.setInt(MRJobConfig.MAP_MEMORY_MB, 4000);

    // hadoop puts an ancient version of jline on the classpath
    conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

    // no need to run a test multiple times
    job.setSpeculativeExecution(false);

    // read one line at a time
    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.setNumLinesPerSplit(job, 1);

    // run the test
    job.setJarByClass(IntegrationTestMapReduce.class);
    job.setMapperClass(TestMapper.class);

    // group test by result code
    job.setReducerClass(TestReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new IntegrationTestMapReduce(), args));
  }

}
