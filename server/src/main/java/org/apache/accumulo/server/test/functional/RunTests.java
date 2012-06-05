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
package org.apache.accumulo.server.test.functional;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

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
 *  $ ./bin/accumulo accumulo.server.test.functional.RunTests /user/hadoop/tests /user/hadoop/results
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
  
  static public class TestMapper extends Mapper<LongWritable,Text,Text,Text> {
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      List<String> cmd = Arrays.asList("/usr/bin/python", "test/system/auto/run.py", "-t", value.toString());
      log.info("Running test " + cmd);
      ProcessBuilder pb = new ProcessBuilder(cmd);
      pb.directory(new File(context.getConfiguration().get("accumulo.home")));
      pb.redirectErrorStream(true);
      Process p = pb.start();
      p.getOutputStream().close();
      InputStream out = p.getInputStream();
      byte[] buffer = new byte[1024];
      int len = 0;
      Text result = new Text();
      try {
        while ((len = out.read(buffer)) > 0) {
          log.info("More: " + new String(buffer, 0, len));
          result.append(buffer, 0, len);
        }
      } catch (Exception ex) {
        log.error(ex, ex);
      }
      p.waitFor();
      context.write(value, result);
    }
    
  }
  
  @Override
  public int run(String[] args) throws Exception {
    job = new Job(getConf(), JOB_NAME);
    job.setJarByClass(this.getClass());
    
    // this is like 1-2 tests per mapper
    Configuration conf = job.getConfiguration();
    conf.setInt("mapred.max.split.size", 40);
    conf.set("accumulo.home", System.getenv("ACCUMULO_HOME"));
    conf.setInt("mapred.task.timeout", 8 * 60 * 1000);
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    
    // set input
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.setInputPaths(job, new Path(args[0]));
    
    // set output
    job.setOutputFormatClass(TextOutputFormat.class);
    FileSystem fs = FileSystem.get(conf);
    Path destination = new Path(args[1]);
    if (fs.exists(destination)) {
      log.info("Deleting existing output directory " + args[1]);
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
  
  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    RunTests tests = new RunTests();
    ToolRunner.run(new Configuration(), tests, args);
    tests.job.waitForCompletion(true);
    if (!tests.job.isSuccessful())
      System.exit(1);
  }
  
}
