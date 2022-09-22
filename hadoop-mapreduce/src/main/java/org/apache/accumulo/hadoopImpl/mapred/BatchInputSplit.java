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
package org.apache.accumulo.hadoopImpl.mapred;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.hadoop.mapred.InputSplit;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * The Class BatchInputSplit. Encapsulates Accumulo ranges for use in Map Reduce jobs. Can contain
 * several Ranges per InputSplit.
 */
@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification = "Intended to share code between mapred and mapreduce")
public class BatchInputSplit extends org.apache.accumulo.hadoopImpl.mapreduce.BatchInputSplit
    implements InputSplit {

  public BatchInputSplit() {}

  public BatchInputSplit(BatchInputSplit split) throws IOException {
    super(split);
  }

  public BatchInputSplit(String table, TableId tableId, Collection<Range> ranges,
      String[] location) {
    super(table, tableId, ranges, location);
  }
}
