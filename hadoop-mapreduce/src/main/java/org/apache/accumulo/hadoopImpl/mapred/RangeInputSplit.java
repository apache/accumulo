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

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.mapred.InputSplit;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * The Class RangeInputSplit. Encapsulates an Accumulo range for use in Map Reduce jobs.
 */
@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification = "Intended to share code between mapred and mapreduce")
public class RangeInputSplit extends org.apache.accumulo.hadoopImpl.mapreduce.RangeInputSplit
    implements InputSplit {

  public RangeInputSplit() {}

  public RangeInputSplit(RangeInputSplit split) throws IOException {
    super(split);
  }

  protected RangeInputSplit(String table, String tableId, Range range, String[] locations) {
    super(table, tableId, range, locations);
  }
}
