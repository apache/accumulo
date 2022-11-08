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
package org.apache.accumulo.core.iterators;

import static java.util.concurrent.TimeUnit.HOURS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iteratorsImpl.conf.ColumnSet;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

/**
 * A SortedKeyValueIterator that combines the Values for different versions (timestamp) of a Key
 * within a row into a single Value. Combiner will replace one or more versions of a Key and their
 * Values with the most recent Key and a Value which is the result of the reduce method. An
 * {@link Column} which only specifies a column family will combine all Keys in that column family
 * individually. Similarly, a {@link Column} which specifies a column family and column qualifier
 * will combine all Keys in column family and qualifier individually. Combination is only ever
 * performed on multiple versions and not across column qualifiers or column visibilities.
 *
 * <p>
 * Implementations must provide a reduce method:
 * {@code public Value reduce(Key key, Iterator<Value> iter)}.
 *
 * <p>
 * This reduce method will be passed the most recent Key and an iterator over the Values for all
 * non-deleted versions of that Key. A combiner will not combine keys that differ by more than the
 * timestamp.
 *
 * <p>
 * This class and its implementations do not automatically filter out unwanted columns from those
 * being combined, thus it is generally recommended to use a {@link Combiner} implementation with
 * the {@link ScannerBase#fetchColumnFamily(Text)} or {@link ScannerBase#fetchColumn(Text, Text)}
 * methods.
 *
 * <p>
 * WARNING : Using deletes with Combiners may not work as intended. See
 * {@link #setReduceOnFullCompactionOnly(IteratorSetting, boolean)}
 */
public abstract class Combiner extends WrappingIterator implements OptionDescriber {

  static final Logger sawDeleteLog =
      LoggerFactory.getLogger(Combiner.class.getName() + ".SawDelete");

  protected static final String COLUMNS_OPTION = "columns";
  protected static final String ALL_OPTION = "all";
  protected static final String REDUCE_ON_FULL_COMPACTION_ONLY_OPTION =
      "reduceOnFullCompactionOnly";

  private boolean isMajorCompaction;
  private boolean reduceOnFullCompactionOnly;

  /**
   * A Java Iterator that iterates over the Values for a given Key from a source
   * SortedKeyValueIterator.
   */
  public static class ValueIterator implements Iterator<Value> {
    Key topKey;
    SortedKeyValueIterator<Key,Value> source;
    boolean hasNext;

    /**
     * Constructs an iterator over Values whose Keys are versions of the current topKey of the
     * source SortedKeyValueIterator.
     *
     * @param source
     *          The {@code SortedKeyValueIterator<Key,Value>} from which to read data.
     */
    public ValueIterator(SortedKeyValueIterator<Key,Value> source) {
      this.source = source;
      topKey = new Key(source.getTopKey());
      hasNext = _hasNext();
    }

    private boolean _hasNext() {
      return source.hasTop() && !source.getTopKey().isDeleted()
          && topKey.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public Value next() {
      if (!hasNext)
        throw new NoSuchElementException();
      Value topValue = new Value(source.getTopValue());
      try {
        source.next();
        hasNext = _hasNext();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return topValue;
    }

    /**
     * This method is unsupported in this iterator.
     *
     * @throws UnsupportedOperationException
     *           when called
     */
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  Key topKey;
  Value topValue;

  @Override
  public Key getTopKey() {
    if (topKey == null)
      return super.getTopKey();
    return topKey;
  }

  @Override
  public Value getTopValue() {
    if (topKey == null)
      return super.getTopValue();
    return topValue;
  }

  @Override
  public boolean hasTop() {
    return topKey != null || super.hasTop();
  }

  @Override
  public void next() throws IOException {
    if (topKey != null) {
      topKey = null;
      topValue = null;
    } else {
      super.next();
    }

    findTop();
  }

  private Key workKey = new Key();

  @VisibleForTesting
  static final Cache<String,Boolean> loggedMsgCache =
      CacheBuilder.newBuilder().expireAfterWrite(1, HOURS).maximumSize(10000).build();

  private void sawDelete() {
    if (isMajorCompaction && !reduceOnFullCompactionOnly) {
      try {
        loggedMsgCache.get(this.getClass().getName(), () -> {
          sawDeleteLog.error(
              "Combiner of type {} saw a delete during a"
                  + " partial compaction. This could cause undesired results. See"
                  + " ACCUMULO-2232. Will not log subsequent occurrences for at least 1 hour.",
              Combiner.this.getClass().getSimpleName());
          // the value is not used and does not matter
          return Boolean.TRUE;
        });
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Sets the topKey and topValue based on the top key of the source. If the column of the source
   * top key is in the set of combiners, topKey will be the top key of the source and topValue will
   * be the result of the reduce method. Otherwise, topKey and topValue will be unchanged. (They are
   * always set to null before this method is called.)
   */
  private void findTop() throws IOException {
    // check if aggregation is needed
    if (super.hasTop()) {
      workKey.set(super.getTopKey());
      if (combineAllColumns || combiners.contains(workKey)) {
        if (workKey.isDeleted()) {
          sawDelete();
          return;
        }
        topKey = workKey;
        Iterator<Value> viter = new ValueIterator(getSource());
        topValue = reduce(topKey, viter);
        while (viter.hasNext())
          viter.next();
      }
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    // do not want to seek to the middle of a value that should be combined...

    Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);

    super.seek(seekRange, columnFamilies, inclusive);
    findTop();

    if (range.getStartKey() != null) {
      while (hasTop()
          && getTopKey().equals(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
          && getTopKey().getTimestamp() > range.getStartKey().getTimestamp()) {
        // the value has a more recent time stamp, so pass it up
        // log.debug("skipping "+getTopKey());
        next();
      }

      while (hasTop() && range.beforeStartKey(getTopKey())) {
        next();
      }
    }
  }

  /**
   * Reduces a list of Values into a single Value.
   *
   * @param key
   *          The most recent version of the Key being reduced.
   *
   * @param iter
   *          An iterator over the Values for different versions of the key.
   *
   * @return The combined Value.
   */
  public abstract Value reduce(Key key, Iterator<Value> iter);

  private ColumnSet combiners;
  private boolean combineAllColumns;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);

    combineAllColumns = false;
    if (options.containsKey(ALL_OPTION)) {
      combineAllColumns = Boolean.parseBoolean(options.get(ALL_OPTION));
    }

    if (!combineAllColumns) {
      if (!options.containsKey(COLUMNS_OPTION))
        throw new IllegalArgumentException("Must specify " + COLUMNS_OPTION + " option");

      String encodedColumns = options.get(COLUMNS_OPTION);
      if (encodedColumns.isEmpty())
        throw new IllegalArgumentException("The " + COLUMNS_OPTION + " must not be empty");

      combiners = new ColumnSet(Lists.newArrayList(Splitter.on(",").split(encodedColumns)));
    }

    isMajorCompaction = env.getIteratorScope() == IteratorScope.majc;

    String rofco = options.get(REDUCE_ON_FULL_COMPACTION_ONLY_OPTION);
    if (rofco != null) {
      reduceOnFullCompactionOnly = Boolean.parseBoolean(rofco);
    } else {
      reduceOnFullCompactionOnly = false;
    }

    if (reduceOnFullCompactionOnly && isMajorCompaction && !env.isFullMajorCompaction()) {
      // adjust configuration so that no columns are combined for a partial major compaction
      combineAllColumns = false;
      combiners = new ColumnSet();
    }

  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    Combiner newInstance;
    try {
      newInstance = this.getClass().getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    newInstance.setSource(getSource().deepCopy(env));
    newInstance.combiners = combiners;
    newInstance.combineAllColumns = combineAllColumns;
    newInstance.isMajorCompaction = isMajorCompaction;
    newInstance.reduceOnFullCompactionOnly = reduceOnFullCompactionOnly;
    return newInstance;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = new IteratorOptions("comb",
        "Combiners apply reduce functions to multiple versions of values with otherwise equal keys",
        null, null);
    io.addNamedOption(ALL_OPTION,
        "set to true to apply Combiner to every column, otherwise leave blank. if true, "
            + COLUMNS_OPTION + " option will be ignored.");
    io.addNamedOption(COLUMNS_OPTION,
        "<col fam>[:<col qual>]{,<col fam>[:<col qual>]} escape non-alphanum chars using %<hex>.");
    io.addNamedOption(REDUCE_ON_FULL_COMPACTION_ONLY_OPTION,
        "If true, only reduce on full major compactions.  Defaults to false. ");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (options.containsKey(ALL_OPTION)) {
      try {
        combineAllColumns = Boolean.parseBoolean(options.get(ALL_OPTION));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "bad boolean " + ALL_OPTION + ":" + options.get(ALL_OPTION));
      }
      if (combineAllColumns)
        return true;
    }
    if (!options.containsKey(COLUMNS_OPTION))
      throw new IllegalArgumentException(
          "options must include " + ALL_OPTION + " or " + COLUMNS_OPTION);

    String encodedColumns = options.get(COLUMNS_OPTION);
    if (encodedColumns.isEmpty())
      throw new IllegalArgumentException("empty columns specified in option " + COLUMNS_OPTION);

    for (String columns : Splitter.on(",").split(encodedColumns)) {
      if (!ColumnSet.isValidEncoding(columns))
        throw new IllegalArgumentException("invalid column encoding " + encodedColumns);
    }

    return true;
  }

  /**
   * A convenience method to set which columns a combiner should be applied to. For each column
   * specified, all versions of a Key which match that @{link IteratorSetting.Column} will be
   * combined individually in each row. This method is likely to be used in conjunction with
   * {@link ScannerBase#fetchColumnFamily(Text)} or {@link ScannerBase#fetchColumn(Text,Text)}.
   *
   * @param is
   *          iterator settings object to configure
   * @param columns
   *          a list of columns to encode as the value for the combiner column configuration
   */
  public static void setColumns(IteratorSetting is, List<IteratorSetting.Column> columns) {
    String sep = "";
    StringBuilder sb = new StringBuilder();

    for (Column col : columns) {
      sb.append(sep);
      sep = ",";
      sb.append(ColumnSet.encodeColumns(col.getFirst(), col.getSecond()));
    }

    is.addOption(COLUMNS_OPTION, sb.toString());
  }

  /**
   * A convenience method to set the "all columns" option on a Combiner. This will combine all
   * columns individually within each row.
   *
   * @param is
   *          iterator settings object to configure
   * @param combineAllColumns
   *          if true, the columns option is ignored and the Combiner will be applied to all columns
   */
  public static void setCombineAllColumns(IteratorSetting is, boolean combineAllColumns) {
    is.addOption(ALL_OPTION, Boolean.toString(combineAllColumns));
  }

  /**
   * Combiners may not work correctly with deletes. Sometimes when Accumulo compacts the files in a
   * tablet, it only compacts a subset of the files. If a delete marker exists in one of the files
   * that is not being compacted, then data that should be deleted may be combined. See
   * <a href="https://issues.apache.org/jira/browse/ACCUMULO-2232">ACCUMULO-2232</a> for more
   * information. For correctness deletes should not be used with columns that are combined OR this
   * option should be set to true.
   *
   * <p>
   * When this method is set to true all data is passed through during partial major compactions and
   * no reducing is done. Reducing is only done during scan and full major compactions, when deletes
   * can be correctly handled. Only reducing on full major compactions may have negative performance
   * implications, leaving lots of work to be done at scan time.
   *
   * <p>
   * When this method is set to false, combiners will log an error if a delete is seen during any
   * compaction. This can be suppressed by adjusting logging configuration. Errors will not be
   * logged more than once an hour per Combiner, regardless of how many deletes are seen.
   *
   * <p>
   * This method was added in 1.6.4 and 1.7.1. If you want your code to work in earlier versions of
   * 1.6 and 1.7 then do not call this method. If not set this property defaults to false in order
   * to maintain compatibility.
   *
   * @since 1.6.5 1.7.1 1.8.0
   */

  public static void setReduceOnFullCompactionOnly(IteratorSetting is,
      boolean reduceOnFullCompactionOnly) {
    is.addOption(REDUCE_ON_FULL_COMPACTION_ONLY_OPTION,
        Boolean.toString(reduceOnFullCompactionOnly));
  }

}
