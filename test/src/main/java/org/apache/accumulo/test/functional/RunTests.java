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
package org.apache.accumulo.test.functional;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.server.util.reflection.CounterUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

/**
 * Runs the functional tests via map-reduce.
 * 
 * First, be sure everything is compiled.
 * 
 * Second, get a list of the tests you want to run:
 * 
 * <pre>
 *  $ python test/system/auto/run.py -l > tests
 * </pre>
 * 
 * Put the list of tests into HDFS:
 * 
 * <pre>
 *  $ hadoop fs -put tests /user/hadoop/tests
 * </pre>
 * 
 * Run the map-reduce job:
 * 
 * <pre>
 *  $ ./bin/accumulo accumulo.test.functional.RunTests --tests /user/hadoop/tests --output /user/hadoop/results
 * </pre>
 * 
 * Note that you will need to have some configuration in conf/accumulo-site.xml (to locate zookeeper). The map-reduce jobs will not use your local accumulo
 * instance.
 * 
 */
public class RunTests extends Configured implements Tool {
  
  static final public String JOB_NAME = "Functional Test Runner";
  private static final Logger log = Logger.getLogger(RunTests.class);
  
  private Job job = null;

  private static final int DEFAULT_TIMEOUT_FACTOR = 1;

  static class Opts extends Help {
    @Parameter(names="--tests", description="newline separated list of tests to run", required=true)
    String testFile;
    @Parameter(names="--output", description="destination for the results of tests in HDFS", required=true)
    String outputPath;
    @Parameter(names="--timeoutFactor", description="Optional scaling factor for timeout for both mapred.task.timeout and -f flag on run.py", required=false)
    Integer intTimeoutFactor = DEFAULT_TIMEOUT_FACTOR;
  }
  
  static final String TIMEOUT_FACTOR = RunTests.class.getName() + ".timeoutFactor";

  static public class TestMapper extends Mapper<LongWritable,Text,Text,Text> {
    
    private static final String REDUCER_RESULT_START = "::::: ";
    private static final int RRS_LEN = REDUCER_RESULT_START.length();
    private Text result = new Text();
    String mapperTimeoutFactor = null;

    private static enum Outcome {
      SUCCESS, FAILURE, ERROR, UNEXPECTED_SUCCESS, EXPECTED_FAILURE
    }
    private static final Map<Character, Outcome> OUTCOME_COUNTERS;
    static {
      OUTCOME_COUNTERS = new java.util.HashMap<Character, Outcome>();
      OUTCOME_COUNTERS.put('S', Outcome.SUCCESS);
      OUTCOME_COUNTERS.put('F', Outcome.FAILURE);
      OUTCOME_COUNTERS.put('E', Outcome.ERROR);
      OUTCOME_COUNTERS.put('T', Outcome.UNEXPECTED_SUCCESS);
      OUTCOME_COUNTERS.put('G', Outcome.EXPECTED_FAILURE);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      List<String> cmd = Arrays.asList("/usr/bin/python", "test/system/auto/run.py", "-m", "-f", mapperTimeoutFactor, "-t", value.toString());
      log.info("Running test " + cmd);
      ProcessBuilder pb = new ProcessBuilder(cmd);
      pb.directory(new File(context.getConfiguration().get("accumulo.home")));
      pb.redirectErrorStream(true);
      Process p = pb.start();
      p.getOutputStream().close();
      InputStream out = p.getInputStream();
      InputStreamReader outr = new InputStreamReader(out, Constants.UTF8);
      BufferedReader br = new BufferedReader(outr);
      String line;
      try {
        while ((line = br.readLine()) != null) {
          log.info("More: " + line);
          if (line.startsWith(REDUCER_RESULT_START)) {
            String resultLine = line.substring(RRS_LEN);
            if (resultLine.length() > 0) {
              Outcome outcome = OUTCOME_COUNTERS.get(resultLine.charAt(0));
              if (outcome != null) {
                CounterUtils.increment(context.getCounter(outcome));
              }
            }
            String taskAttemptId = context.getTaskAttemptID().toString();
            result.set(taskAttemptId + " " + resultLine);
            context.write(value, result);
          }
        }
      } catch (Exception ex) {
        log.error(ex);
        context.progress();
      }

      p.waitFor();
    }
    
    @Override
    protected void setup(Mapper<LongWritable,Text,Text,Text>.Context context) throws IOException, InterruptedException {
      mapperTimeoutFactor = Integer.toString(context.getConfiguration().getInt(TIMEOUT_FACTOR, DEFAULT_TIMEOUT_FACTOR));
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    job = new Job(getConf(), JOB_NAME);
    job.setJarByClass(this.getClass());
    Opts opts = new Opts();
    opts.parseArgs(RunTests.class.getName(), args);
    
    // this is like 1-2 tests per mapper
    Configuration conf = job.getConfiguration();
    conf.setInt("mapred.max.split.size", 40);
    conf.set("accumulo.home", System.getenv("ACCUMULO_HOME"));

    // Taking third argument as scaling factor to setting mapred.task.timeout
    // and TIMEOUT_FACTOR
    conf.setInt("mapred.task.timeout", opts.intTimeoutFactor * 8 * 60 * 1000);
    conf.setInt(TIMEOUT_FACTOR, opts.intTimeoutFactor);
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    
    // set input
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.setInputPaths(job, new Path(opts.testFile));
    
    // set output
    job.setOutputFormatClass(TextOutputFormat.class);
    FileSystem fs = FileSystem.get(conf);
    Path destination = new Path(opts.outputPath);
    if (fs.exists(destination)) {
      log.info("Deleting existing output directory " + opts.outputPath);
      fs.delete(destination, true);
    }
    TextOutputFormat.setOutputPath(job, destination);
    
    // configure default reducer: put the results into one file
    job.setNumReduceTasks(1);
    
    // set mapper
    job.setMapperClass(TestMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    // don't do anything with the results (yet) a summary would be nice
    job.setNumReduceTasks(0);
    
    // submit the job
    log.info("Starting tests");
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    RunTests tests = new RunTests();
    ToolRunner.run(new Configuration(), tests, args);
    tests.job.waitForCompletion(true);
    if (!tests.job.isSuccessful())
      System.exit(1);
  }
  
}
