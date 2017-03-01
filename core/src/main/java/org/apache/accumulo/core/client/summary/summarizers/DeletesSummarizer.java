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

package org.apache.accumulo.core.client.summary.summarizers;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * This summarizer tracks the total number of delete Keys seen and the total number of keys seen.
 *
 * <p>
 * This summarizer is used by org.apache.accumulo.tserver.compaction.strategies.TooManyDeletesCompactionStrategy to make compaction decisions based on the
 * number of deletes.
 *
 * @since 2.0.0
 * @see TableOperations#addSummarizers(String, org.apache.accumulo.core.client.summary.SummarizerConfiguration...)
 */
public class DeletesSummarizer implements Summarizer {

  /**
   * The name of the statistics for the number of deletes.
   */
  public static final String DELETES_STAT = "deletes";

  /**
   * The name of the statistics for the total number of keys.
   */
  public static final String TOTAL_STAT = "total";

  @Override
  public Collector collector(SummarizerConfiguration sc) {
    return new Collector() {

      long total = 0;
      long deletes = 0;

      @Override
      public void accept(Key k, Value v) {
        total++;
        if (k.isDeleted()) {
          deletes++;
        }
      }

      @Override
      public void summarize(StatisticConsumer sc) {
        sc.accept(DELETES_STAT, deletes);
        sc.accept(TOTAL_STAT, total);
      }
    };
  }

  @Override
  public Combiner combiner(SummarizerConfiguration sc) {
    return (m1, m2) -> m2.forEach((k, v) -> m1.merge(k, v, Long::sum));
  }
}
