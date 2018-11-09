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
package org.apache.accumulo.hadoop.mapreduce;

import static org.apache.accumulo.hadoopImpl.mapreduce.AbstractInputFormat.getClientInfo;

import java.io.IOException;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.hadoopImpl.mapreduce.AccumuloOutputFormatImpl;
import org.apache.accumulo.hadoopImpl.mapreduce.OutputFormatBuilderImpl;
import org.apache.hadoop.io.Text;
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
 * AccumuloOutputFormat.configure().clientInfo(clientInfo).batchWriterOptions(bwConfig)
 *     .defaultTableName(name).enableCreateTables() // disabled by default
 *     .enableSimulationMode() // disabled by default
 *     .store(job);
 * </pre>
 *
 * @since 2.0
 */
public class AccumuloOutputFormat extends OutputFormat<Text,Mutation> {

  @Override
  public void checkOutputSpecs(JobContext job) throws IOException {
    try {
      // if the instance isn't configured, it will complain here
      ClientInfo clientInfo = getClientInfo(job);
      String principal = clientInfo.getPrincipal();
      AuthenticationToken token = clientInfo.getAuthenticationToken();
      AccumuloClient c = Accumulo.newClient().from(clientInfo).build();

      if (!c.securityOperations().authenticateUser(principal, token))
        throw new IOException("Unable to authenticate user");
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
      return new AccumuloOutputFormatImpl.AccumuloRecordWriter(attempt);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Sets all the information required for this map reduce job.
   */
  public static OutputFormatBuilder.ClientParams configure() {
    return new OutputFormatBuilderImpl();
  }

}
