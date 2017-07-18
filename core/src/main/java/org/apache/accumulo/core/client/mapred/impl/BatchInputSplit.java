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
package org.apache.accumulo.core.client.mapred.impl;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.mapred.InputSplit;

/**
 * The Class BatchInputSplit. Encapsulates Accumulo ranges for use in Map Reduce jobs. Can contain several Ranges per InputSplit.
 */
public class BatchInputSplit extends org.apache.accumulo.core.client.mapreduce.impl.BatchInputSplit implements InputSplit {

  public BatchInputSplit() {
    super();
  }

  public BatchInputSplit(BatchInputSplit split) throws IOException {
    super(split);
  }

  public BatchInputSplit(String table, Table.ID tableId, Collection<Range> ranges, String[] location) {
    super(table, tableId, ranges, location);
  }
}
