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

package org.apache.accumulo.core.client.summary;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * <p>
 * Instance of this interface can be configured for Accumulo tables. When Accumulo compacts files, it will create an instance and call collect() for each
 * Key/Value the compaction writes. When the compaction finishes, summarize() will be called and the returned map will be stored in the file created by the
 * compaction.
 * 
 * <p>
 * Inorder to merge summary information from multiple files, an instance of the class used to create the summary information will be created and merge will be
 * called.
 */
public interface KeyValueSummarizer {
  /**
   * @return A unique id that users can reference when requesting a summary.
   */
  String getId();

  // TODO consider adding a version number

  /**
   * Incrementally build summary information.
   */
  void collect(Key k, Value v);

  public static interface SummaryConsumer {
    public void consume(String summary, long value);
  }

  /**
   * Report summaries for all key values collected before this point.
   */
  void summarize(SummaryConsumer sc);

  /**
   * Reset and clear back to an initial state before anything was collected
   */
  public void reset();

  /**
   * Merge summary 2 into summary 1. This method is called to merge information previously obtained from calling {@link #summarize(SummaryConsumer)}.
   */
  void merge(Map<String,Long> summary1, Map<String,Long> summary2);
}
