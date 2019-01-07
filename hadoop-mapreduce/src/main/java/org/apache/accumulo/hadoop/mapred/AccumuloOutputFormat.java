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

import java.io.IOException;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.hadoop.mapreduce.OutputFormatBuilder;
import org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl;
import org.apache.accumulo.hadoopImpl.mapreduce.OutputFormatBuilderImpl;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

/**
 * @see org.apache.accumulo.hadoop.mapreduce.AccumuloOutputFormat
 *
 * @since 2.0
 */
public class AccumuloOutputFormat implements OutputFormat<Text,Mutation> {

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    ClientInfo clientInfo = AccumuloOutputFormatImpl.getClientInfo(job);
    String principal = clientInfo.getPrincipal();
    AuthenticationToken token = clientInfo.getAuthenticationToken();
    try (AccumuloClient c = Accumulo.newClient().from(clientInfo.getProperties()).build()) {
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

  public static OutputFormatBuilder.ClientParams<JobConf> configure() {
    return new OutputFormatBuilderImpl<JobConf>();
  }
}
