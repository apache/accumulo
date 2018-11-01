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
package org.apache.accumulo.hadoop.mapred;

import static org.apache.accumulo.hadoopImpl.mapred.AccumuloFileOutputFormatImpl.setCompressionType;
import static org.apache.accumulo.hadoopImpl.mapred.AccumuloFileOutputFormatImpl.setDataBlockSize;
import static org.apache.accumulo.hadoopImpl.mapred.AccumuloFileOutputFormatImpl.setFileBlockSize;
import static org.apache.accumulo.hadoopImpl.mapred.AccumuloFileOutputFormatImpl.setIndexBlockSize;
import static org.apache.accumulo.hadoopImpl.mapred.AccumuloFileOutputFormatImpl.setReplication;
import static org.apache.accumulo.hadoopImpl.mapred.AccumuloFileOutputFormatImpl.setSampler;
import static org.apache.accumulo.hadoopImpl.mapred.AccumuloFileOutputFormatImpl.setSummarizers;

import java.io.IOException;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoop.mapreduce.FileOutputInfo;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.ConfiguratorBase;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.FileOutputConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * This class allows MapReduce jobs to write output in the Accumulo data file format.<br>
 * Care should be taken to write only sorted data (sorted by {@link Key}), as this is an important
 * requirement of Accumulo data files.
 *
 * <p>
 * The output path to be created must be specified via {@link #setInfo(JobConf, FileOutputInfo)}
 * using {@link FileOutputInfo#builder()}.outputPath(path). For all available options see
 * {@link FileOutputInfo#builder()}
 * <p>
 * Methods inherited from {@link FileOutputFormat} are not supported and may be ignored or cause
 * failures. Using other Hadoop configuration options that affect the behavior of the underlying
 * files directly in the Job's configuration may work, but are not directly supported at this time.
 */
public class AccumuloFileOutputFormat extends FileOutputFormat<Key,Value> {

  @Override
  public RecordWriter<Key,Value> getRecordWriter(FileSystem ignored, JobConf job, String name,
      Progressable progress) throws IOException {
    // get the path of the temporary output file
    final Configuration conf = job;
    final AccumuloConfiguration acuConf = FileOutputConfigurator
        .getAccumuloConfiguration(AccumuloFileOutputFormat.class, job);

    final String extension = acuConf.get(Property.TABLE_FILE_TYPE);
    final Path file = new Path(getWorkOutputPath(job),
        getUniqueName(job, "part") + "." + extension);
    final int visCacheSize = ConfiguratorBase.getVisibilityCacheSize(conf);

    return new RecordWriter<Key,Value>() {
      RFileWriter out = null;

      @Override
      public void close(Reporter reporter) throws IOException {
        if (out != null)
          out.close();
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
  public static void setInfo(JobConf job, FileOutputInfo info) {
    setOutputPath(job, info.getOutputPath());
    if (info.getCompressionType().isPresent())
      setCompressionType(job, info.getCompressionType().get());
    if (info.getDataBlockSize().isPresent())
      setDataBlockSize(job, info.getDataBlockSize().get());
    if (info.getFileBlockSize().isPresent())
      setFileBlockSize(job, info.getFileBlockSize().get());
    if (info.getIndexBlockSize().isPresent())
      setIndexBlockSize(job, info.getIndexBlockSize().get());
    if (info.getReplication().isPresent())
      setReplication(job, info.getReplication().get());
    if (info.getSampler().isPresent())
      setSampler(job, info.getSampler().get());
    if (info.getSummarizers().size() > 0)
      setSummarizers(job, info.getSummarizers().toArray(new SummarizerConfiguration[0]));
  }

}
