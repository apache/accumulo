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
package org.apache.accumulo.core.cli;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.hadoop.mapreduce.Job;

import com.beust.jcommander.Parameter;

public class MapReduceClientOnRequiredTable extends MapReduceClientOpts {

  @Parameter(names = {"-t", "--table"}, required = true, description = "table to use")
  private String tableName;

  @Parameter(names = {"-tf", "--tokenFile"}, description = "File in hdfs containing the user's authentication token create with \"bin/accumulo create-token\"")
  private String tokenFile = "";

  @Override
  public void setAccumuloConfigs(Job job) throws AccumuloSecurityException {
    super.setAccumuloConfigs(job);

    final String principal = getPrincipal(), tableName = getTableName();

    if (tokenFile.isEmpty()) {
      AuthenticationToken token = getToken();
      AccumuloInputFormat.setConnectorInfo(job, principal, token);
      AccumuloOutputFormat.setConnectorInfo(job, principal, token);
    } else {
      AccumuloInputFormat.setConnectorInfo(job, principal, tokenFile);
      AccumuloOutputFormat.setConnectorInfo(job, principal, tokenFile);
    }
    AccumuloInputFormat.setInputTableName(job, tableName);
    AccumuloInputFormat.setScanAuthorizations(job, auths);
    AccumuloOutputFormat.setCreateTables(job, true);
    AccumuloOutputFormat.setDefaultTableName(job, tableName);
  }

  public String getTableName() {
    return tableName;
  }
}
