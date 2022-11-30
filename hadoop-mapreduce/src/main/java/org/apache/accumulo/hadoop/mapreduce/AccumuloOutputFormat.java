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
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.hadoopImpl.mapreduce.AccumuloRecordWriter;
import org.apache.accumulo.hadoopImpl.mapreduce.OutputFormatBuilderImpl;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.OutputConfigurator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * This class allows MapReduce jobs to use Accumulo as the sink for data. This {@link OutputFormat}
 * accepts keys and values of type {@link Text} (for a table name) and {@link Mutation} from the Map
 * and Reduce functions. Configured with fluent API using {@link AccumuloOutputFormat#configure()}.
 * Here is an example with all possible options:
 *
 * <pre>
 * AccumuloOutputFormat.configure().clientProperties(props).batchWriterOptions(bwConfig)
 *     .defaultTable(name).createTables(true) // disabled by default
 *     .simulationMode(true) // disabled by default
 *     .store(job);
 * </pre>
 *
 * @since 2.0
 */
public class AccumuloOutputFormat extends OutputFormat<Text,Mutation> {
  private static final Class<AccumuloOutputFormat> CLASS = AccumuloOutputFormat.class;

  @Override
  public void checkOutputSpecs(JobContext job) throws IOException {
    OutputConfigurator.checkJobStored(CLASS, job.getConfiguration());
    Properties clientProps = OutputConfigurator.getClientProperties(CLASS, job.getConfiguration());
    AuthenticationToken token = ClientProperty.getAuthenticationToken(clientProps);
    try (AccumuloClient c = Accumulo.newClient().from(clientProps).build()) {
      if (!c.securityOperations().authenticateUser(c.whoami(), token)) {
        throw new IOException("Unable to authenticate user");
      }
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new IOException(e);
    }
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new NullOutputFormat<Text,Mutation>().getOutputCommitter(context);
  }

  @Override
  public RecordWriter<Text,Mutation> getRecordWriter(TaskAttemptContext attempt)
      throws IOException {
    try {
      return new AccumuloRecordWriter(attempt);
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  /**
   * Sets all the information required for this map reduce job.
   */
  public static OutputFormatBuilder.ClientParams<Job> configure() {
    return new OutputFormatBuilderImpl<>(CLASS);
  }

}
