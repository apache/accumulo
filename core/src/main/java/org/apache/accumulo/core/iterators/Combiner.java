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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

/**
 * A SortedKeyValueIterator that combines the Values for different versions (timestamp) of a Key within a row into a single Value. Combiner will replace one or
 * more versions of a Key and their Values with the most recent Key and a Value which is the result of the reduce method. An {@link Column} which only specifies
 * a column family will combine all Keys in that column family individually. Similarly, a {@link Column} which specifies a column family and column qualifier
 * will combine all Keys in column family and qualifier individually. Combination is only ever performed on multiple versions and not across column qualifiers
 * or column visibilities.
 *
 * Implementations must provide a reduce method: {@code public Value reduce(Key key, Iterator<Value> iter)}.
 *
 * This reduce method will be passed the most recent Key and an iterator over the Values for all non-deleted versions of that Key. A combiner will not combine
 * keys that differ by more than the timestamp.
 *
 * This class and its implementations do not automatically filter out unwanted columns from those being combined, thus it is generally recommended to use a
 * {@link Combiner} implementation with the {@link ScannerBase#fetchColumnFamily(Text)} or {@link ScannerBase#fetchColumn(Text, Text)} methods.
 */
public abstract class Combiner extends WrappingIterator implements OptionDescriber {
  static final Logger log = Logger.getLogger(Combiner.class);
  protected static final String COLUMNS_OPTION = "columns";
  protected static final String ALL_OPTION = "all";
  protected static final String DELETE_HANDLING_ACTION_OPTION = "deleteHandlingAction";

  private boolean isPartialCompaction;
  private DeleteHandlingAction deleteHandlingAction;

  /**
   * A Java Iterator that iterates over the Values for a given Key from a source SortedKeyValueIterator.
   */
  public static class ValueIterator implements Iterator<Value> {
    Key topKey;
    SortedKeyValueIterator<Key,Value> source;
    boolean hasNext;

    /**
     * Constructs an iterator over Values whose Keys are versions of the current topKey of the source SortedKeyValueIterator.
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
      return source.hasTop() && !source.getTopKey().isDeleted() && topKey.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
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
        throw new RuntimeException(e);
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

  private static final Cache<String,Long> loggedMsgCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).maximumSize(10000).build();

  private void sawDelete() {
    if (isPartialCompaction) {
      switch (deleteHandlingAction) {
        case LOG_ERROR:
          try {
            loggedMsgCache.get(this.getClass().getName(), new Callable<Long>() {
              @Override
              public Long call() throws Exception {
                log.error("Combiner of type " + this.getClass().getSimpleName()
                    + " saw a delete during a partial compaction.  This could cause undesired results.  See ACCUMULO-2232.  Will not log subsequent occurences for at least 1 hour.");
                //the value is not used and does not matter
                return 42L;
              }
            });
          } catch (ExecutionException e) {
            throw new RuntimeException(e);
          }
          break;
        case THROW_EXCEPTION:
          throw new IllegalStateException("Saw a delete during a partial compaction.  This could cause undesired results.  See ACCUMULO-2232.");
        case IGNORE:
          break;
        case REDUCE_ON_FULL_COMPACTION_ONLY:
        default:
          // unexpected
          throw new IllegalStateException("Unexpected case occurred in code, please file a bug " + deleteHandlingAction);
      }
    }
  }

  /**
   * Sets the topKey and topValue based on the top key of the source. If the column of the source top key is in the set of combiners, topKey will be the top key
   * of the source and topValue will be the result of the reduce method. Otherwise, topKey and topValue will be unchanged. (They are always set to null before
   * this method is called.)
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
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    // do not want to seek to the middle of a value that should be combined...

    Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);

    super.seek(seekRange, columnFamilies, inclusive);
    findTop();

    if (range.getStartKey() != null) {
      while (hasTop() && getTopKey().equals(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
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
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);

    combineAllColumns = false;
    if (options.containsKey(ALL_OPTION)) {
      combineAllColumns = Boolean.parseBoolean(options.get(ALL_OPTION));
      if (combineAllColumns)
        return;
    }

    if (!options.containsKey(COLUMNS_OPTION))
      throw new IllegalArgumentException("Must specify " + COLUMNS_OPTION + " option");

    String encodedColumns = options.get(COLUMNS_OPTION);
    if (encodedColumns.length() == 0)
      throw new IllegalArgumentException("The " + COLUMNS_OPTION + " must not be empty");

    combiners = new ColumnSet(Lists.newArrayList(Splitter.on(",").split(encodedColumns)));

    isPartialCompaction = ((env.getIteratorScope() == IteratorScope.majc) && !env.isFullMajorCompaction());

    String dhaOpt = options.get(DELETE_HANDLING_ACTION_OPTION);
    if (dhaOpt != null) {
      try{
        deleteHandlingAction = DeleteHandlingAction.valueOf(dhaOpt);
      } catch(IllegalArgumentException iae) {
        throw new IllegalAccessError(dhaOpt+" is not a legal option for "+DELETE_HANDLING_ACTION_OPTION);
      }
    } else {
      deleteHandlingAction = DeleteHandlingAction.LOG_ERROR;
    }

    if (deleteHandlingAction == DeleteHandlingAction.REDUCE_ON_FULL_COMPACTION_ONLY && isPartialCompaction) {
      // adjust configuration so that no columns are combined for a partial maror compaction
      combineAllColumns = false;
      combiners = new ColumnSet();
    }

  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    // TODO test
    Combiner newInstance;
    try {
      newInstance = this.getClass().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    newInstance.setSource(getSource().deepCopy(env));
    newInstance.combiners = combiners;
    newInstance.combineAllColumns = combineAllColumns;
    newInstance.isPartialCompaction = isPartialCompaction;
    newInstance.deleteHandlingAction = deleteHandlingAction;
    return newInstance;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = new IteratorOptions("comb", "Combiners apply reduce functions to multiple versions of values with otherwise equal keys", null, null);
    io.addNamedOption(ALL_OPTION, "set to true to apply Combiner to every column, otherwise leave blank. if true, " + COLUMNS_OPTION
        + " option will be ignored.");
    io.addNamedOption(COLUMNS_OPTION, "<col fam>[:<col qual>]{,<col fam>[:<col qual>]} escape non-alphanum chars using %<hex>.");
    io.addNamedOption(DELETE_HANDLING_ACTION_OPTION, "How to handle deletes during a parital compaction.  Legal values are : "+ Arrays.asList(DeleteHandlingAction.values())+" the default is "+DeleteHandlingAction.LOG_ERROR.name());
    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (options.containsKey(ALL_OPTION)) {
      try {
        combineAllColumns = Boolean.parseBoolean(options.get(ALL_OPTION));
      } catch (Exception e) {
        throw new IllegalArgumentException("bad boolean " + ALL_OPTION + ":" + options.get(ALL_OPTION));
      }
      if (combineAllColumns)
        return true;
    }
    if (!options.containsKey(COLUMNS_OPTION))
      throw new IllegalArgumentException("options must include " + ALL_OPTION + " or " + COLUMNS_OPTION);

    String encodedColumns = options.get(COLUMNS_OPTION);
    if (encodedColumns.length() == 0)
      throw new IllegalArgumentException("empty columns specified in option " + COLUMNS_OPTION);

    for (String columns : Splitter.on(",").split(encodedColumns)) {
      if (!ColumnSet.isValidEncoding(columns))
        throw new IllegalArgumentException("invalid column encoding " + encodedColumns);
    }

    String dhaOpt = options.get(DELETE_HANDLING_ACTION_OPTION);
    if (dhaOpt != null) {
      try{
        DeleteHandlingAction.valueOf(dhaOpt);
      } catch(IllegalArgumentException iae) {
        throw new IllegalAccessError(dhaOpt+" is not a legal option for "+DELETE_HANDLING_ACTION_OPTION);
      }
    }

    return true;
  }

  /**
   * A convenience method to set which columns a combiner should be applied to. For each column specified, all versions of a Key which match that @{link
   * IteratorSetting.Column} will be combined individually in each row. This method is likely to be used in conjunction with
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
   * A convenience method to set the "all columns" option on a Combiner. This will combine all columns individually within each row.
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
   * @since 1.6.4 1.7.1 1.8.0
   */
  public static enum DeleteHandlingAction {
    /**
     * Do nothing when a a delete is observed by a combiner during a partial major compaction.
     */
    IGNORE,

    /**
     * Log an error when a a delete is observed by a combiner during a partial major compaction. An error is not logged for each delete entry seen. Once a
     * combiner has seen a delete during a partial compaction and logged an error, it will not do so again for at least an hour.
     */
    LOG_ERROR,

    /**
     * Throw an exception when a a delete is observed by a combiner during a partial major compaction.
     */
    THROW_EXCEPTION,

    /**
     * Pass all data through during partial major compactions, no reducing is done. With this option reducing is only done during scan and full major
     * compactions, when deletes can be correctly handled.
     */
    REDUCE_ON_FULL_COMPACTION_ONLY
  }

  /**
   * Combiners may not work correctly with deletes. Sometimes when Accumulo compacts the files in a tablet, it only compacts a subset of the files. If a delete
   * marker exists in one of the files that is not being compacted, then data that should be delted may be combined. See
   * <a href="https://issues.apache.org/jira/browse/ACCUMULO-2232">ACCUMULO-2232</a> for more information.
   *
   * <p>
   * This method allows users to configure how they want to handle the combination of delete markers, combiners, and partial compactions. The default behavior
   * is {@link DeleteHandlingAction#LOG_ERROR}. See the javadoc on each {@link DeleteHandlingAction} enum for a description of each option.
   *
   * <p>
   * This method was added in 1.6.4 and 1.7.1.  If you want your code to work in earlier versions of 1.6 and 1.7 then do not call this method.
   *
   * @since 1.6.4 1.7.1 1.8.0
   */
  public static void setDeleteHandlingAction(IteratorSetting is, DeleteHandlingAction action) {
    is.addOption(DELETE_HANDLING_ACTION_OPTION, action.name());
  }
}
