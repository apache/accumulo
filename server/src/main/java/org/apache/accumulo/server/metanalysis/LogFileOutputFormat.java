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
package org.apache.accumulo.server.metanalysis;

import java.io.IOException;

import org.apache.accumulo.server.logger.LogFileKey;
import org.apache.accumulo.server.logger.LogFileValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Output format for Accumulo write ahead logs.
 */
public class LogFileOutputFormat extends FileOutputFormat<LogFileKey,LogFileValue> {
  
  private static class LogFileRecordWriter extends RecordWriter<LogFileKey,LogFileValue> {
    
    private FSDataOutputStream out;
    
    /**
     * @param outputPath
     * @throws IOException
     */
    public LogFileRecordWriter(Path outputPath) throws IOException {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      
      out = fs.create(outputPath);
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
      out.close();
    }
    
    @Override
    public void write(LogFileKey key, LogFileValue val) throws IOException, InterruptedException {
      key.write(out);
      val.write(out);
    }
    
  }

  @Override
  public RecordWriter<LogFileKey,LogFileValue> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    Path outputPath = getDefaultWorkFile(context, "");
    return new LogFileRecordWriter(outputPath);
  }
  
}
