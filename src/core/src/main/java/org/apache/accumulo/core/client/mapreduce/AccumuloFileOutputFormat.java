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
package org.apache.accumulo.core.client.mapreduce;

import java.io.IOException;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class allows MapReduce jobs to use the Accumulo data file format for output of data
 * 
 * The user must specify the output path that does not exist following via static method calls to this class:
 * 
 * AccumuloFileOutputFormat.setOutputPath(job, outputDirectory)
 * 
 * Other methods from FileOutputFormat to configure options are ignored Compression is using the DefaultCodec and is always on
 */
public class AccumuloFileOutputFormat extends FileOutputFormat<Key,Value> {
  private static final String PREFIX = AccumuloOutputFormat.class.getSimpleName();
  public static final String FILE_TYPE = PREFIX + ".file_type";
  private static final String INSTANCE_HAS_BEEN_SET = PREFIX + ".instanceConfigured";
  private static final String INSTANCE_NAME = PREFIX + ".instanceName";
  private static final String ZOOKEEPERS = PREFIX + ".zooKeepers";
  
  @Override
  public RecordWriter<Key,Value> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
    // get the path of the temporary output file
    Configuration conf = job.getConfiguration();
    
    String extension = conf.get(FILE_TYPE);
    if (extension == null || extension.isEmpty())
      extension = RFile.EXTENSION;
    
    AccumuloConfiguration acuConf = AccumuloConfiguration.getDefaultConfiguration();
    if (conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
      acuConf = getInstance(job).getConfiguration();
    conf.setInt("io.seqfile.compress.blocksize", (int) acuConf.getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE));
    Path file = this.getDefaultWorkFile(job, "." + extension);
    
    final FileSKVWriter out = FileOperations.getInstance().openWriter(file.toString(), file.getFileSystem(conf), conf,
        AccumuloConfiguration.getDefaultConfiguration());
    out.startDefaultLocalityGroup();
    
    return new RecordWriter<Key,Value>() {
      @Override
      public void write(Key key, Value value) throws IOException {
        out.append(key, value);
      }
      
      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        out.close();
      }
    };
  }
  
  public static void setFileType(JobContext job, String type) {
    job.getConfiguration().set(FILE_TYPE, type);
  }
  
  public static void setZooKeeperInstance(JobContext job, String instanceName, String zooKeepers) {
    Configuration conf = job.getConfiguration();
    if (conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IllegalStateException("Instance info can only be set once per job");
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(instanceName, zooKeepers);
    conf.set(INSTANCE_NAME, instanceName);
    conf.set(ZOOKEEPERS, zooKeepers);
  }
  
  protected static Instance getInstance(JobContext job) {
    Configuration conf = job.getConfiguration();
    return new ZooKeeperInstance(conf.get(INSTANCE_NAME), conf.get(ZOOKEEPERS));
  }
}
