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

import static org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.getClientInfo;
import static org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setBatchWriterOptions;
import static org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setClientInfo;
import static org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setCreateTables;
import static org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setDefaultTableName;
import static org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl.setSimulationMode;

import java.io.IOException;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.hadoop.mapreduce.OutputInfo;
import org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

/**
 * This class allows MapReduce jobs to use Accumulo as the sink for data. This {@link OutputFormat}
 * accepts keys and values of type {@link Text} (for a table name) and {@link Mutation} from the Map
 * and Reduce functions.
 *
 * The user must specify the following via static configurator method:
 *
 * <ul>
 * <li>{@link AccumuloOutputFormat#setInfo(JobConf, OutputInfo)}
 * </ul>
 */
public class AccumuloOutputFormat implements OutputFormat<Text,Mutation> {

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    try {
      // if the instance isn't configured, it will complain here
      ClientInfo clientInfo = getClientInfo(job);
      String principal = clientInfo.getPrincipal();
      AuthenticationToken token = clientInfo.getAuthenticationToken();
      AccumuloClient c = Accumulo.newClient().usingClientInfo(clientInfo).build();
      if (!c.securityOperations().authenticateUser(principal, token))
        throw new IOException("Unable to authenticate user");
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new IOException(e);
    }
  }

  @Override
  public RecordWriter<Text,Mutation> getRecordWriter(FileSystem ignored, JobConf job, String name,
      Progressable progress) throws IOException {
    try {
      return new AccumuloOutputFormatImpl.AccumuloRecordWriter(job);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static void setInfo(JobConf job, OutputInfo info) {
    setClientInfo(job, info.getClientInfo());
    if (info.getBatchWriterOptions().isPresent())
      setBatchWriterOptions(job, info.getBatchWriterOptions().get());
    if (info.getDefaultTableName().isPresent())
      setDefaultTableName(job, info.getDefaultTableName().get());
    setCreateTables(job, info.isCreateTables());
    setSimulationMode(job, info.isSimulationMode());
  }

}
