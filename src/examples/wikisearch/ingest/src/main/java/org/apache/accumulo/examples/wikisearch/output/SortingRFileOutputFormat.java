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
package org.apache.accumulo.examples.wikisearch.output;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SortingRFileOutputFormat extends OutputFormat<Text,Mutation> {
  
  // private static final Logger log = Logger.getLogger(SortingRFileOutputFormat.class);
  
  public static final String PATH_NAME = "sortingrfileoutputformat.path";
  public static final String MAX_BUFFER_SIZE = "sortingrfileoutputformat.max.buffer.size";
  
  public static void setPathName(Configuration conf, String path) {
    conf.set(PATH_NAME, path);
  }
  
  public static String getPathName(Configuration conf) {
    return conf.get(PATH_NAME);
  }
  
  public static void setMaxBufferSize(Configuration conf, long maxBufferSize) {
    conf.setLong(MAX_BUFFER_SIZE, maxBufferSize);
  }
  
  public static long getMaxBufferSize(Configuration conf) {
    return conf.getLong(MAX_BUFFER_SIZE, -1);
  }
  
  @Override
  public void checkOutputSpecs(JobContext job) throws IOException, InterruptedException {
    // TODO make sure the path is writable?
    // TODO make sure the max buffer size is set and is reasonable
  }
  
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException {
    return new OutputCommitter() {
      
      @Override
      public void setupTask(TaskAttemptContext arg0) throws IOException {
        // TODO Auto-generated method stub
        
      }
      
      @Override
      public void setupJob(JobContext arg0) throws IOException {
        // TODO Auto-generated method stub
        
      }
      
      @Override
      public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
        // TODO Auto-generated method stub
        return false;
      }
      
      @Override
      public void commitTask(TaskAttemptContext arg0) throws IOException {
        // TODO Auto-generated method stub
        
      }
      
      @Override
      public void cleanupJob(JobContext arg0) throws IOException {
        // TODO Auto-generated method stub
        
      }
      
      @Override
      public void abortTask(TaskAttemptContext arg0) throws IOException {
        // TODO Auto-generated method stub
        
      }
    };
  }
  
  @Override
  public RecordWriter<Text,Mutation> getRecordWriter(TaskAttemptContext attempt) throws IOException, InterruptedException {
    
    // grab the configuration
    final Configuration conf = attempt.getConfiguration();
    // create a filename
    final String filenamePrefix = getPathName(conf);
    final String taskID = attempt.getTaskAttemptID().toString();
    // grab the max size
    final long maxSize = getMaxBufferSize(conf);
    // grab the FileSystem
    final FileSystem fs = FileSystem.get(conf);
    // create a default AccumuloConfiguration
    final AccumuloConfiguration acuconf = AccumuloConfiguration.getDefaultConfiguration();
    
    return new BufferingRFileRecordWriter(maxSize, acuconf, conf, filenamePrefix, taskID, fs);
  }
  
}
