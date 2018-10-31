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

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.hadoop.mapreduce.OutputInfo;
import org.apache.accumulo.hadoopImpl.mapred.AccumuloOutputFormatImpl;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

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
public class AccumuloOutputFormat extends AccumuloOutputFormatImpl {

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
