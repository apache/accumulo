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
package org.apache.accumulo.core.client.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * AccumuloInputFormat which returns an "empty" RangeInputSplit
 */
public class EmptySplitsAccumuloInputFormat extends AccumuloInputFormat {

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    List<InputSplit> oldSplits = super.getSplits(context);
    List<InputSplit> newSplits = new ArrayList<InputSplit>(oldSplits.size());

    // Copy only the necessary information
    for (InputSplit oldSplit : oldSplits) {
      org.apache.accumulo.core.client.mapreduce.RangeInputSplit newSplit = new org.apache.accumulo.core.client.mapreduce.RangeInputSplit(
          (org.apache.accumulo.core.client.mapreduce.RangeInputSplit) oldSplit);
      newSplits.add(newSplit);
    }

    return newSplits;
  }
}
