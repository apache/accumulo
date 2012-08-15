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
    final Configuration conf = job.getConfiguration();
    
    String extension = conf.get(FILE_TYPE);
    if (extension == null || extension.isEmpty())
      extension = RFile.EXTENSION;
    
    final Path file = this.getDefaultWorkFile(job, "." + extension);
    
    return new RecordWriter<Key,Value>() {
      FileSKVWriter out = null;
      
      @Override
      public void write(Key key, Value value) throws IOException {
        if (out == null) {
          out = FileOperations.getInstance().openWriter(file.toString(), file.getFileSystem(conf), conf, AccumuloConfiguration.getDefaultConfiguration());
          out.startDefaultLocalityGroup();
        }
        out.append(key, value);
      }
      
      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (out != null)
          out.close();
      }
    };
  }
  
  public static void setFileType(Configuration conf, String type) {
    conf.set(FILE_TYPE, type);
  }
  
  /**
   * @deprecated since 1.5, use {@link #setCompressedBlockSize(Configuration, long)} instead
   */
  public static void setBlockSize(Configuration conf, int blockSize) {
    long bs = blockSize;
    setCompressedBlockSize(conf, bs);
  }
  
  /**
   * @param conf
   * @param instanceName
   * @param zooKeepers
   */
  public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
    if (conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IllegalStateException("Instance info can only be set once per job");
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(instanceName, zooKeepers);
    conf.set(INSTANCE_NAME, instanceName);
    conf.set(ZOOKEEPERS, zooKeepers);
  }
  
  /**
   * @param conf
   * @return The Accumulo instance.
   */
  protected static Instance getInstance(Configuration conf) {
    return new ZooKeeperInstance(conf.get(INSTANCE_NAME), conf.get(ZOOKEEPERS));
  }
  
  public static void setReplication(Configuration conf, int replication) {
    conf.setInt(Property.TABLE_FILE_REPLICATION.getKey(), replication);
  }
  
  public static void setDFSBlockSize(Configuration conf, long blockSize) {
    conf.setLong(Property.TABLE_FILE_BLOCK_SIZE.getKey(), blockSize);
  }
  
  public static void setCompressedBlockSize(Configuration conf, long cblockSize) {
    conf.setLong(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), cblockSize);
  }
  
  public static void setCompressedBlockSizeIndex(Configuration conf, long cblockSizeIndex) {
    conf.setLong(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey(), cblockSizeIndex);
  }
  
  public static void setCompressionType(Configuration conf, String compression) {
    conf.set(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), compression);
  }
}
