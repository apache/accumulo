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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.summary.summarizers.VisibilitySummarizer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.mutable.MutableLong;

//checkstyle and formatter are in conflict
//@formatter:off
/**
 * This class counts arbitrary keys while defending against too many keys and keys that are too long.
 *
 * <p>
 * During collection and summarization this class will use the functions from {@link #converter()} and {@link #encoder()}. For each key/value the function from
 * {@link #converter()} will be called to create zero or more counter objects. A counter associated with each counter object will be incremented, as long as
 * there are not too many counters and the counter object is not too long.
 *
 * <p>
 * When {@link Summarizer.Collector#summarize(Summarizer.StatisticConsumer)} is called, the function from {@link #encoder()} will be used to convert counter
 * objects to strings. These strings will be used to emit statistics. Overriding {@link #encoder()} is optional. One reason to override is if the counter object
 * contains binary or special data. For example, a function that base64 encodes counter objects could be created.
 *
 * <p>
 * If the counter key type is mutable, then consider overriding {@link #copier()}.
 *
 * <p>
 * The function returned by {@link #converter()} will be called frequently and should be very efficient. The function returned by {@link #encoder()} will be
 * called less frequently and can be more expensive. The reason these two functions exists is to avoid the conversion to string for each key value, if that
 * conversion is unnecessary.
 *
 * <p>
 * Below is an example implementation that counts column visibilities. This example avoids converting column visibility to string for each key/value. This
 * example shows the source code for {@link VisibilitySummarizer}.
 *
 * <pre>
 * <code>
 *   public class VisibilitySummarizer extends CountingSummarizer&lt;ByteSequence&gt; {
 *     &#064;Override
 *     protected UnaryOperator&lt;ByteSequence&gt; copier() {
 *       // ByteSequences are mutable, so override and provide a copy function
 *       return ArrayByteSequence::new;
 *     }
 *
 *     &#064;Override
 *     protected Converter&lt;ByteSequence&gt; converter() {
 *       return (key, val, consumer) -&gt; consumer.accept(key.getColumnVisibilityData());
 *     }
 *   }
 * </code>
 * </pre>
 *
 * @param <K>
 *          The counter key type. This type must have good implementations of {@link Object#hashCode()} and {@link Object#equals(Object)}.
 * @see CounterSummary
 * @since 2.0.0
 */
//@formatter:on
public abstract class CountingSummarizer<K> implements Summarizer {

  /**
   * A configuration option for specifying the maximum number of unique counters an instance of this summarizer should track. If not specified, a default of
   * {@value #MAX_COUNTER_DEFAULT} will be used.
   */
  public static final String MAX_COUNTERS_OPT = "maxCounters";

  /**
   * A configuration option for specifying the maximum length of an individual counter key. If not specified, a default of {@value #MAX_CKL_DEFAULT} will be
   * used.
   */
  public static final String MAX_COUNTER_LEN_OPT = "maxCounterLen";

  /**
   * A configuration option to determine if delete keys should be counted. If set to true then delete keys will not be passed to the {@link Converter} and the
   * statistic {@value #DELETES_IGNORED_STAT} will track the number of deleted ignored. This options defaults to {@value #INGNORE_DELETES_DEFAULT}.
   */
  public static final String INGNORE_DELETES_OPT = "ignoreDeletes";

  /**
   * This prefixes all counters when emitting statistics in {@link Summarizer.Collector#summarize(Summarizer.StatisticConsumer)}.
   */
  public static final String COUNTER_STAT_PREFIX = "c:";

  /**
   * This is the name of the statistic that tracks how many counters objects were ignored because the number of unique counters was exceeded. The max number of
   * unique counters is specified by {@link #MAX_COUNTERS_OPT}.
   */
  public static final String TOO_MANY_STAT = "tooMany";

  /**
   * This is the name of the statistic that tracks how many counter objects were ignored because they were too long. The maximum lenght is specified by
   * {@link #MAX_COUNTER_LEN_OPT}.
   */
  public static final String TOO_LONG_STAT = "tooLong";

  /**
   * This is the name of the statistic that tracks the total number of counter objects emitted by the {@link Converter}. This includes emitted Counter objects
   * that were ignored.
   */
  public static final String EMITTED_STAT = "emitted";

  /**
   * This is the name of the statistic that tracks the total number of deleted keys seen. This statistic is only incremented when the
   * {@value #INGNORE_DELETES_OPT} option is set to true.
   */
  public static final String DELETES_IGNORED_STAT = "deletesIgnored";

  /**
   * This tracks the total number of key/values seen by the {@link Summarizer.Collector}
   */
  public static final String SEEN_STAT = "seen";

  // this default can not be changed as persisted summary data depends on it. See the documentation about persistence in the Summarizer class javadoc.
  public static final String MAX_COUNTER_DEFAULT = "1024";

  // this default can not be changed as persisted summary data depends on it
  public static final String MAX_CKL_DEFAULT = "128";

  // this default can not be changed as persisted summary data depends on it
  public static final String INGNORE_DELETES_DEFAULT = "true";

  private static final String[] ALL_STATS = new String[] {TOO_LONG_STAT, TOO_MANY_STAT, EMITTED_STAT, SEEN_STAT, DELETES_IGNORED_STAT};

  private int maxCounters;
  private int maxCounterKeyLen;
  private boolean ignoreDeletes;

  private void init(SummarizerConfiguration conf) {
    maxCounters = Integer.parseInt(conf.getOptions().getOrDefault(MAX_COUNTERS_OPT, MAX_COUNTER_DEFAULT));
    maxCounterKeyLen = Integer.parseInt(conf.getOptions().getOrDefault(MAX_COUNTER_LEN_OPT, MAX_CKL_DEFAULT));
    ignoreDeletes = Boolean.parseBoolean(conf.getOptions().getOrDefault(INGNORE_DELETES_OPT, INGNORE_DELETES_DEFAULT));
  }

  /**
   * A function that converts key values to zero or more counter objects.
   *
   * @since 2.0.0
   */
  public static interface Converter<K> {
    /**
     * @param consumer
     *          emit counter objects derived from key and value to this consumer
     */
    public void convert(Key k, Value v, Consumer<K> consumer);
  }

  /**
   *
   * @return A function that is used to convert each key value to zero or more counter objects. Each function returned should be independent.
   */
  protected abstract Converter<K> converter();

  /**
   * @return A function that is used to convert counter objects to String. The default function calls {@link Object#toString()} on the counter object.
   */
  protected Function<K,String> encoder() {
    return Object::toString;
  }

  /**
   * Override this if your key type is mutable and subject to change.
   *
   * @return a function that used to copy the counter object. This function is only used when the collector has never seen the counter object before. In this
   *         case the collector needs to possibly copy the counter object before using as map key. The default implementation is the
   *         {@link UnaryOperator#identity()} function.
   */
  protected UnaryOperator<K> copier() {
    return UnaryOperator.identity();
  }

  @Override
  public Collector collector(SummarizerConfiguration sc) {
    init(sc);
    return new Collector() {

      // Map used for computing summary incrementally uses ByteSequence for key which is more efficient than converting String for each Key. The
      // conversion to String is deferred until the summary is requested.

      private Map<K,MutableLong> counters = new HashMap<>();
      private long tooMany = 0;
      private long tooLong = 0;
      private long seen = 0;
      private long emitted = 0;
      private long deleted = 0;
      private Converter<K> converter = converter();
      private Function<K,String> encoder = encoder();
      private UnaryOperator<K> copier = copier();

      private void incrementCounter(K counter) {
        emitted++;

        MutableLong ml = counters.get(counter);
        if (ml == null) {
          if (counters.size() >= maxCounters) {
            // no need to store this counter in the map and get() it... just use instance variable
            tooMany++;
          } else {
            // we have never seen this key before, check if its too long
            if (encoder.apply(counter).length() >= maxCounterKeyLen) {
              tooLong++;
            } else {
              counters.put(copier.apply(counter), new MutableLong(1));
            }
          }
        } else {
          // using mutable long allows calling put() to be avoided
          ml.increment();
        }
      }

      @Override
      public void accept(Key k, Value v) {
        seen++;
        if (ignoreDeletes && k.isDeleted()) {
          deleted++;
        } else {
          converter.convert(k, v, this::incrementCounter);
        }
      }

      @Override
      public void summarize(StatisticConsumer sc) {
        StringBuilder sb = new StringBuilder(COUNTER_STAT_PREFIX);

        for (Entry<K,MutableLong> entry : counters.entrySet()) {
          sb.setLength(COUNTER_STAT_PREFIX.length());
          sb.append(encoder.apply(entry.getKey()));
          sc.accept(sb.toString(), entry.getValue().longValue());
        }

        sc.accept(TOO_MANY_STAT, tooMany);
        sc.accept(TOO_LONG_STAT, tooLong);
        sc.accept(EMITTED_STAT, emitted);
        sc.accept(SEEN_STAT, seen);
        sc.accept(DELETES_IGNORED_STAT, deleted);
      }
    };
  }

  @Override
  public Combiner combiner(SummarizerConfiguration sc) {
    init(sc);
    return new Combiner() {

      @Override
      public void merge(Map<String,Long> summary1, Map<String,Long> summary2) {

        for (String key : ALL_STATS) {
          summary1.merge(key, summary2.getOrDefault(key, 0l), Long::sum);
        }

        for (Entry<String,Long> entry : summary2.entrySet()) {
          String k2 = entry.getKey();
          Long v2 = entry.getValue();

          if (k2.startsWith(COUNTER_STAT_PREFIX)) {
            summary1.merge(k2, v2, Long::sum);
          }
        }

        if (summary1.size() - ALL_STATS.length > maxCounters) {
          // find the keys with the lowest counts to remove
          List<String> keysToRemove = summary1.entrySet().stream().filter(e -> e.getKey().startsWith(COUNTER_STAT_PREFIX)) // filter out non counters
              .sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue())) // sort descending by count
              .skip(maxCounters) // skip most frequent
              .map(e -> e.getKey()).collect(Collectors.toList()); // collect the least frequent counters in a list

          long removedCount = 0;
          for (String key : keysToRemove) {
            removedCount += summary1.remove(key);
          }

          summary1.merge(TOO_MANY_STAT, removedCount, Long::sum);
        }
      }
    };
  }
}
