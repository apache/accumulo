/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.hadoop.mapreduce;

import java.io.IOException;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoopImpl.mapreduce.FileOutputFormatBuilderImpl;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.ConfiguratorBase;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.FileOutputConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class allows MapReduce jobs to write output in the Accumulo data file format.<br>
 * Care should be taken to write only sorted data (sorted by {@link Key}), as this is an important
 * requirement of Accumulo data files. The output path to be created must be specified via
 * {@link #configure()}, which uses a fluent API. For Example:
 *
 * <pre>
 * AccumuloFileOutputFormat.configure()
 *      .outputPath(path)
 *      .fileBlockSize(b)
 *      .compression(type)
 *      .summarizers(sc1, sc2).store(job));
 * </pre>
 *
 * For all available options see {@link FileOutputFormatBuilder}. Methods inherited from
 * {@link FileOutputFormat} are not supported and may be ignored or cause failures. Using other
 * Hadoop configuration options that affect the behavior of the underlying files directly in the
 * Job's configuration may work, but are not directly supported at this time.
 *
 * @since 2.0
 */
public class AccumuloFileOutputFormat extends FileOutputFormat<Key,Value> {
  private static final Class<AccumuloFileOutputFormat> CLASS = AccumuloFileOutputFormat.class;

  @Override
  public RecordWriter<Key,Value> getRecordWriter(TaskAttemptContext context) throws IOException {
    // get the path of the temporary output file
    final Configuration conf = context.getConfiguration();
    final AccumuloConfiguration acuConf = FileOutputConfigurator
        .getAccumuloConfiguration(AccumuloFileOutputFormat.class, context.getConfiguration());

    final String extension = acuConf.get(Property.TABLE_FILE_TYPE);
    final Path file = this.getDefaultWorkFile(context, "." + extension);
    final int visCacheSize = ConfiguratorBase.getVisibilityCacheSize(conf);

    return new RecordWriter<>() {
      RFileWriter out = null;

      @Override
      public void close(TaskAttemptContext context) throws IOException {
        if (out != null) {
          out.close();
        }
      }

      @Override
      public void write(Key key, Value value) throws IOException {
        if (out == null) {
          out = RFile.newWriter().to(file.toString()).withFileSystem(file.getFileSystem(conf))
              .withTableProperties(acuConf).withVisibilityCacheSize(visCacheSize).build();
          out.startDefaultLocalityGroup();
        }
        out.append(key, value);
      }
    };
  }

  /**
   * Sets all the information required for this map reduce job.
   */
  public static FileOutputFormatBuilder.PathParams<Job> configure() {
    return new FileOutputFormatBuilderImpl<>(CLASS);
  }

}
