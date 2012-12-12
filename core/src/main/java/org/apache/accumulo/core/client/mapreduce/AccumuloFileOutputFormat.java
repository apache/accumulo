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
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class allows MapReduce jobs to write output in the Accumulo data file format.<br />
 * Care should be taken to write only sorted data (sorted by {@link Key}), as this is an important requirement of Accumulo data files.
 * 
 * <p>
 * The output path to be created must be specified via {@link AccumuloFileOutputFormat#setOutputPath(Job, Path)}. This is inherited from
 * {@link FileOutputFormat#setOutputPath(Job, Path)}. Other methods from {@link FileOutputFormat} are not supported and may be ignored or cause failures. Using
 * other Hadoop configuration options that affect the behavior of the underlying files directly in the Job's configuration may work, but are not directly
 * supported at this time.
 */
public class AccumuloFileOutputFormat extends FileOutputFormat<Key,Value> {
  private static final String PREFIX = AccumuloOutputFormat.class.getSimpleName() + ".";
  private static final String ACCUMULO_PROPERTY_PREFIX = PREFIX + "accumuloProperties.";
  
  /**
   * This helper method provides an AccumuloConfiguration object constructed from the Accumulo defaults, and overridden with Accumulo properties that have been
   * stored in the Job's configuration
   * 
   * @since 1.5.0
   */
  protected static AccumuloConfiguration getAccumuloConfiguration(JobContext context) {
    ConfigurationCopy acuConf = new ConfigurationCopy(AccumuloConfiguration.getDefaultConfiguration());
    for (Entry<String,String> entry : context.getConfiguration())
      if (entry.getKey().startsWith(ACCUMULO_PROPERTY_PREFIX))
        acuConf.set(Property.getPropertyByKey(entry.getKey().substring(ACCUMULO_PROPERTY_PREFIX.length())), entry.getValue());
    return acuConf;
  }
  
  /**
   * The supported Accumulo properties we set in this OutputFormat, that change the behavior of the RecordWriter.<br />
   * These properties correspond to the supported public static setter methods available to this class.
   * 
   * @since 1.5.0
   */
  protected static boolean isSupportedAccumuloProperty(Property property) {
    switch (property) {
      case TABLE_FILE_COMPRESSION_TYPE:
      case TABLE_FILE_COMPRESSED_BLOCK_SIZE:
      case TABLE_FILE_BLOCK_SIZE:
      case TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX:
      case TABLE_FILE_REPLICATION:
        return true;
      default:
        return false;
    }
  }
  
  /**
   * Helper for transforming Accumulo configuration properties into something that can be stored safely inside the Hadoop Job configuration.
   * 
   * @since 1.5.0
   */
  protected static <T> void setAccumuloProperty(Job job, Property property, T value) {
    if (isSupportedAccumuloProperty(property)) {
      String val = String.valueOf(value);
      if (property.getType().isValidFormat(val))
        job.getConfiguration().set(ACCUMULO_PROPERTY_PREFIX + property.getKey(), val);
      else
        throw new IllegalArgumentException("Value is not appropriate for property type '" + property.getType() + "'");
    } else
      throw new IllegalArgumentException("Unsupported configuration property " + property.getKey());
  }
  
  /**
   * @param compressionType
   *          The type of compression to use. One of "none", "gz", "lzo", or "snappy". Specifying a compression may require additional libraries to be available
   *          to your Job.
   * @since 1.5.0
   */
  public static void setCompressionType(Job job, String compressionType) {
    setAccumuloProperty(job, Property.TABLE_FILE_COMPRESSION_TYPE, compressionType);
  }
  
  /**
   * Sets the size for data blocks within each file.<br />
   * Data blocks are a span of key/value pairs stored in the file that are compressed and indexed as a group.
   * 
   * <p>
   * Making this value smaller may increase seek performance, but at the cost of increasing the size of the indexes (which can also affect seek performance).
   * 
   * @since 1.5.0
   */
  public static void setDataBlockSize(Job job, long dataBlockSize) {
    setAccumuloProperty(job, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE, dataBlockSize);
  }
  
  /**
   * Sets the size for file blocks in the file system; file blocks are managed, and replicated, by the underlying file system
   * 
   * @since 1.5.0
   */
  public static void setFileBlockSize(Job job, long fileBlockSize) {
    setAccumuloProperty(job, Property.TABLE_FILE_BLOCK_SIZE, fileBlockSize);
  }
  
  /**
   * Sets the size for index blocks within each file; smaller blocks means a deeper index hierarchy within the file, while larger blocks mean a more shallow
   * index hierarchy within the file. This can affect the performance of queries.
   * 
   * @since 1.5.0
   */
  public static void setIndexBlockSize(Job job, long indexBlockSize) {
    setAccumuloProperty(job, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX, indexBlockSize);
  }
  
  /**
   * Sets the file system replication factor for the resulting file, overriding the file system default.
   * 
   * @since 1.5.0
   */
  public static void setReplication(Job job, int replication) {
    setAccumuloProperty(job, Property.TABLE_FILE_REPLICATION, replication);
  }
  
  @Override
  public RecordWriter<Key,Value> getRecordWriter(TaskAttemptContext context) throws IOException {
    // get the path of the temporary output file
    final Configuration conf = context.getConfiguration();
    final AccumuloConfiguration acuConf = getAccumuloConfiguration(context);
    
    final String extension = acuConf.get(Property.TABLE_FILE_TYPE);
    final Path file = this.getDefaultWorkFile(context, "." + extension);
    
    return new RecordWriter<Key,Value>() {
      FileSKVWriter out = null;
      
      @Override
      public void close(TaskAttemptContext context) throws IOException {
        if (out != null)
          out.close();
      }
      
      @Override
      public void write(Key key, Value value) throws IOException {
        if (out == null) {
          out = FileOperations.getInstance().openWriter(file.toString(), file.getFileSystem(conf), conf, acuConf);
          out.startDefaultLocalityGroup();
        }
        out.append(key, value);
      }
    };
  }
  
  // ----------------------------------------------------------------------------------------------------
  // Everything below this line is deprecated and should go away in future versions
  // ----------------------------------------------------------------------------------------------------
  
  /**
   * @deprecated since 1.5.0;
   */
  @SuppressWarnings("unused")
  @Deprecated
  private static final String FILE_TYPE = PREFIX + "file_type";
  
  /**
   * @deprecated since 1.5.0;
   */
  @SuppressWarnings("unused")
  @Deprecated
  private static final String BLOCK_SIZE = PREFIX + "block_size";
  
  /**
   * @deprecated since 1.5.0;
   */
  @Deprecated
  private static final String INSTANCE_HAS_BEEN_SET = PREFIX + "instanceConfigured";
  
  /**
   * @deprecated since 1.5.0;
   */
  @Deprecated
  private static final String INSTANCE_NAME = PREFIX + "instanceName";
  
  /**
   * @deprecated since 1.5.0;
   */
  @Deprecated
  private static final String ZOOKEEPERS = PREFIX + "zooKeepers";
  
  /**
   * @deprecated since 1.5.0; Retrieve the relevant block size from {@link #getAccumuloConfiguration(JobContext)}
   */
  @Deprecated
  protected static void handleBlockSize(Configuration conf) {
    conf.setInt("io.seqfile.compress.blocksize",
        (int) AccumuloConfiguration.getDefaultConfiguration().getMemoryInBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE));
  }
  
  /**
   * @deprecated since 1.5.0; This method does nothing. Only 'rf' type is supported.
   */
  @Deprecated
  public static void setFileType(Configuration conf, String type) {}
  
  /**
   * @deprecated since 1.5.0; Use {@link #setFileBlockSize(Job, long)}, {@link #setDataBlockSize(Job, long)}, or {@link #setIndexBlockSize(Job, long)} instead.
   */
  @Deprecated
  public static void setBlockSize(Configuration conf, int blockSize) {
    conf.set(ACCUMULO_PROPERTY_PREFIX + Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), String.valueOf(blockSize));
  }
  
  /**
   * @deprecated since 1.5.0; This OutputFormat does not communicate with Accumulo. If this is needed, subclasses must implement their own configuration.
   */
  @Deprecated
  public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
    if (conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
      throw new IllegalStateException("Instance info can only be set once per job");
    conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
    
    ArgumentChecker.notNull(instanceName, zooKeepers);
    conf.set(INSTANCE_NAME, instanceName);
    conf.set(ZOOKEEPERS, zooKeepers);
  }
  
  /**
   * @deprecated since 1.5.0; This OutputFormat does not communicate with Accumulo. If this is needed, subclasses must implement their own configuration.
   */
  @Deprecated
  protected static Instance getInstance(Configuration conf) {
    return new ZooKeeperInstance(conf.get(INSTANCE_NAME), conf.get(ZOOKEEPERS));
  }
  
}
