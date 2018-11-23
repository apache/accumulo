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
package org.apache.accumulo.hadoopImpl.mapreduce.lib;

import org.apache.accumulo.core.client.ClientInfo;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.hadoop.mapreduce.AccumuloOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.beust.jcommander.Parameter;

public class MapReduceClientOnDefaultTable extends MapReduceClientOpts {
  @Parameter(names = "--table", description = "table to use")
  public String tableName;

  public MapReduceClientOnDefaultTable(String table) {
    this.tableName = table;
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public void setAccumuloConfigs(Job job) {
    final String tableName = getTableName();
    final ClientInfo info = getClientInfo();
    AccumuloInputFormat.configure().clientInfo(info).table(tableName).scanAuths(auths).store(job);
    AccumuloOutputFormat.configure().clientInfo(info).defaultTableName(tableName)
        .enableCreateTables().store(job);
  }

}
