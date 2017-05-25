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

import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

//checkstyle and the formatter are in conflict, so turn off the formatter
//@formatter:off
/**
 * <p>
 * Instances of this interface can be configured for Accumulo tables. When Accumulo compacts files, it will use this Factory to create {@link Collector} and
 * {@link Combiner} objects to generate summary information about the data in the file.
 *
 * <p>
 * In order to merge summary information from multiple files, Accumulo will use this factory to create a {@link Combiner} object.
 *
 * <p>
 * Below is an example of a very simple summarizer that will compute the number of deletes, total number of keys, min timestamp and max timestamp.
 *
 * <pre>
 * <code>
 *   public class BasicSummarizer implements Summarizer {
 *
 *     public static final String DELETES_STAT = &quot;deletes&quot;;
 *     public static final String MIN_STAMP_STAT = &quot;minStamp&quot;;
 *     public static final String MAX_STAMP_STAT = &quot;maxStamp&quot;;
 *     public static final String TOTAL_STAT = &quot;total&quot;;
 *
 *     &#064;Override
 *     public Collector collector(SummarizerConfiguration sc) {
 *       return new Collector() {
 *
 *         private long minStamp = Long.MAX_VALUE;
 *         private long maxStamp = Long.MIN_VALUE;
 *         private long deletes = 0;
 *         private long total = 0;
 *
 *         &#064;Override
 *         public void accept(Key k, Value v) {
 *           if (k.getTimestamp() &lt; minStamp) {
 *             minStamp = k.getTimestamp();
 *           }
 *
 *           if (k.getTimestamp() &gt; maxStamp) {
 *             maxStamp = k.getTimestamp();
 *           }
 *
 *           if (k.isDeleted()) {
 *             deletes++;
 *           }
 *
 *           total++;
 *         }
 *
 *         &#064;Override
 *         public void summarize(StatisticConsumer sc) {
 *           sc.accept(MIN_STAMP_STAT, minStamp);
 *           sc.accept(MAX_STAMP_STAT, maxStamp);
 *           sc.accept(DELETES_STAT, deletes);
 *           sc.accept(TOTAL_STAT, total);
 *         }
 *       };
 *     }
 *
 *     &#064;Override
 *     public Combiner combiner(SummarizerConfiguration sc) {
 *       return (stats1, stats2) -&gt; {
 *         stats1.merge(DELETES_STAT, stats2.get(DELETES_STAT), Long::sum);
 *         stats1.merge(TOTAL_STAT, stats2.get(TOTAL_STAT), Long::sum);
 *         stats1.merge(MIN_STAMP_STAT, stats2.get(MIN_STAMP_STAT), Long::min);
 *         stats1.merge(MAX_STAMP_STAT, stats2.get(MAX_STAMP_STAT), Long::max);
 *       };
 *     }
 *   }
 * </code>
 * </pre>
 *
 * <p>
 * Below is an example summarizer that counts the log of the value length.
 *
 * <pre>
 * <code>
 * public class ValueLogLengthSummarizer implements Summarizer {
 *
 *  &#64;Override
 *  public Collector collector(SummarizerConfiguration sc) {
 *
 *    return new Collector(){
 *
 *      long[] counts = new long[32];
 *
 *      &#64;Override
 *      public void accept(Key k, Value v) {
 *        int idx;
 *        if(v.getSize() == 0)
 *          idx = 0;
 *        else
 *          idx = IntMath.log2(v.getSize(), RoundingMode.UP);  //IntMath is from Guava
 *
 *        counts[idx]++;
 *      }
 *
 *      &#64;Override
 *      public void summarize(StatisticConsumer sc) {
 *        for (int i = 0; i &lt; counts.length; i++) {
 *          if(counts[i] &gt; 0) {
 *            sc.accept(""+(1&lt;&lt;i), counts[i]);
 *          }
 *        }
 *      }
 *    };
 *  }
 *
 *  &#64;Override
 *  public Combiner combiner(SummarizerConfiguration sc) {
 *    return (m1, m2) -&gt; m2.forEach((k,v) -&gt; m1.merge(k, v, Long::sum));
 *  }
 * }
 * </code>
 * </pre>
 *
 * <p>
 * The reason a Summarizer is a factory for a Collector and Combiner is to make it very clear in the API that Accumulo uses them independently at different
 * times. Therefore its not advisable to share internal state between the Collector and Combiner. The example implementation shows that the Collectors design
 * allows for very efficient collection of specialized summary information. Creating {@link String} + {@link Long} pairs is deferred until the summarize method
 * is called.
 *
 * <p>
 * Summary data can be used by Compaction Strategies to decide which files to compact.
 *
 * <p>
 * Summary data is persisted, so ideally the same summarizer class with the same options should always produce the same results.  If you need to change the behavior
 * of a summarizer, then consider doing this by adding a new option.  If the same summarizer is configured twice with different options, then Accumulo will store and
 * merge each one separately.  This can allow old and new behavior to coexists simultaneously.
 *
 * @since 2.0.0
 *
 * @see TableOperations#summaries(String)
 * @see TableOperations#addSummarizers(String, SummarizerConfiguration...)
 * @see TableOperations#listSummarizers(String)
 * @see TableOperations#removeSummarizers(String, java.util.function.Predicate)
 * @see RFile#summaries()
 * @see SummarizerConfiguration
 */
 //@formatter:on
public interface Summarizer {

  public static interface StatisticConsumer {
    public void accept(String statistic, long value);
  }

  /**
   * When Accumulo calls methods in this interface, it will call {@link #accept(Key, Value)} zero or more times and then call
   * {@link Collector#summarize(Summarizer.StatisticConsumer)} once. After calling {@link Collector#summarize(Summarizer.StatisticConsumer)}, it will not use
   * the collector again.
   *
   * @since 2.0.0
   */
  public static interface Collector {
    /**
     * During compactions, Accumulo passes each Key Value written to the file to this method.
     */
    void accept(Key k, Value v);

    /**
     * After Accumulo has written some Key Values, it will call this method to generate some statistics about what was previously passed to
     * {@link #accept(Key, Value)}.
     *
     * <p>
     * In order for summary data to be useful for decision making about data, it needs to be quickly accessible. In order to be quickly accessible, it needs to
     * fit in the tablet server cache as described in {@link TableOperations#summaries(String)} and the compaction strategy documentation. Therefore its
     * advisable to generate small summaries. If the summary data generated is too large it will not be stored. The maximum summary size is set using the per
     * table property {@code table.file.summary.maxSize}. The number of files that exceeded the summary size is reported by
     * {@link Summary.FileStatistics#getLarge()}.
     *
     * @param sc
     *          Emit statistics to this Object.
     */
    public void summarize(StatisticConsumer sc);
  }

  /**
   * A Combiner is used to merge statistics emitted from {@link Collector#summarize(Summarizer.StatisticConsumer)} and from previous invocations of itself.
   *
   * @since 2.0.0
   */
  public static interface Combiner {
    /**
     * This method should merge the statistics in the second map into the first map. Both maps may have statistics produced by a {@link Collector} or previous
     * calls to this method.
     *
     * <p>
     * If first map is too large after this call, then it may not be stored. See the comment on {@link Collector#summarize(Summarizer.StatisticConsumer)}
     */
    public void merge(Map<String,Long> statistics1, Map<String,Long> statistics2);
  }

  /**
   * Factory method that creates a {@link Collector} based on configuration. Each {@link Collector} created by this method should be independent and have its
   * own internal state. Accumulo uses a Collector to generate summary statistics about a sequence of key values written to a file.
   */
  public Collector collector(SummarizerConfiguration sc);

  /**
   * Factory method that creates a {@link Combiner}. Accumulo will only use the created Combiner to merge data from {@link Collector}s created using the same
   * {@link SummarizerConfiguration}.
   */
  public Combiner combiner(SummarizerConfiguration sc);
}
