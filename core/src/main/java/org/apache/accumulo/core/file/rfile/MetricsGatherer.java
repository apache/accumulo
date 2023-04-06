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
package org.apache.accumulo.core.file.rfile;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * Interface used to gather metrics from RFiles.
 *
 * @param <T> Type used to return metrics in getMetrics(). This does not impact collection of
 *        metrics at all, is only used in that method.
 */
public interface MetricsGatherer<T> {

  /**
   * Initialize the gatherer when it is registered with the RFile Reader
   *
   * @param cf Map of the LocalityGroup names to their column families
   */
  void init(Map<String,ArrayList<ByteSequence>> cf);

  /**
   * Start a new LocalityGroup. This method is used when the RFile seeks to the next LocalityGroup.
   *
   * @param cf Text object of the column family of the first entry in the locality group
   */
  void startLocalityGroup(Text cf);

  /**
   * Collect and store metrics for the given entry.
   *
   * @param key Key object of the entry you are collecting metrics from
   *
   * @param val Value object of the entry you are collecting metrics from
   *
   */
  void addMetric(Key key, Value val);

  /**
   * Start a new block within a LocalityGroup. This method is used when the RFile moves on the the
   * next block in the LocalityGroup.
   */
  void startBlock();

  /**
   * Print the results of the metrics gathering by locality group in the format: Metric name Number
   * of keys Percentage of keys Number of blocks Percentage of blocks
   *
   * @param hash Boolean to determine whether the values being printed should be hashed
   * @param metricWord String of the name of the metric that was collected
   * @param out PrintStream of where the information should be written to
   */
  void printMetrics(boolean hash, String metricWord, PrintStream out);

  /**
   * @return the metrics gathered
   */
  T getMetrics();

}
