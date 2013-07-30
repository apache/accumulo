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

package org.apache.accumulo.utils.metanalysis;

import java.io.EOFException;
import java.io.IOException;

import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Input format for Accumulo write ahead logs
 */
public class LogFileInputFormat extends FileInputFormat<LogFileKey,LogFileValue> {
  
  private static class LogFileRecordReader extends RecordReader<LogFileKey,LogFileValue> {
    
    private FSDataInputStream fsdis;
    private LogFileKey key;
    private LogFileValue value;
    private long length;
    
    @Override
    public void close() throws IOException {
      fsdis.close();
    }
    
    @Override
    public LogFileKey getCurrentKey() throws IOException, InterruptedException {
      return key;
    }
    
    @Override
    public LogFileValue getCurrentValue() throws IOException, InterruptedException {
      return value;
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
      float progress = (length - fsdis.getPos()) / (float) length;
      if (progress < 0)
        return 0;
      return progress;
    }
    
    @Override
    public void initialize(InputSplit is, TaskAttemptContext context) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit) is;
      
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      
      key = new LogFileKey();
      value = new LogFileValue();
      
      fsdis = fs.open(fileSplit.getPath());
      FileStatus status = fs.getFileStatus(fileSplit.getPath());
      length = status.getLen();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (key == null)
        return false;
      
      try {
        key.readFields(fsdis);
        value.readFields(fsdis);
        return true;
      } catch (EOFException ex) {
        key = null;
        value = null;
        return false;
      }
    }
    
  }

  
  @Override
  public RecordReader<LogFileKey,LogFileValue> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
    return new LogFileRecordReader();
  }
  
  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }
  
}
