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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.log4j.Logger;

/**
 * A SortedKeyValueIterator that combines the Values for different versions of a Key into a single Value. Combiner will replace one or more versions of a Key
 * and their Values with the most recent Key and a Value which is the result of the reduce method.
 * 
 * Subclasses must implement a reduce method: public Value reduce(Key key, Iterator<Value> iter);
 * 
 * This reduce method will be passed the most recent Key and an iterator over the Values for all non-deleted versions of that Key.
 */
public abstract class Combiner extends WrappingIterator implements OptionDescriber {
  static final Logger log = Logger.getLogger(Combiner.class);
  protected static final String COLUMNS_OPTION = "columns";
  protected static final String ALL_OPTION = "all";
  
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
     *          The SortedKeyValueIterator<Key,Value> from which to read data.
     */
    public ValueIterator(SortedKeyValueIterator<Key,Value> source) {
      this.source = source;
      topKey = source.getTopKey();
      hasNext = _hasNext();
    }
    
    private boolean _hasNext() {
      return source.hasTop() && !source.getTopKey().isDeleted() && topKey.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
    }
    
    /**
     * @return <tt>true</tt> if there is another Value
     * 
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
      return hasNext;
    }
    
    /**
     * @return the next Value
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public Value next() {
      if (!hasNext)
        throw new NoSuchElementException();
      Value topValue = source.getTopValue();
      try {
        source.next();
        hasNext = _hasNext();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return topValue;
    }
    
    /**
     * unsupported
     * 
     * @see java.util.Iterator#remove()
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
        if (workKey.isDeleted())
          return;
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
    
    combiners = new ColumnSet(Arrays.asList(encodedColumns.split(",")));
  }
  
  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = new IteratorOptions("comb", "Combiners apply reduce functions to values with identical keys", null, null);
    io.addNamedOption(ALL_OPTION, "set to true to apply Combiner to every column, otherwise leave blank. if true, " + COLUMNS_OPTION
        + " option will be ignored.");
    io.addNamedOption(COLUMNS_OPTION, "<col fam>[:<col qual>]{,<col fam>[:<col qual>]} escape non-alphanum chars using %<hex>.");
    return io;
  }
  
  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (options.containsKey(ALL_OPTION)) {
      combineAllColumns = Boolean.parseBoolean(options.get(ALL_OPTION));
      if (combineAllColumns)
        return true;
    }
    if (!options.containsKey(COLUMNS_OPTION))
      return false;
    
    String encodedColumns = options.get(COLUMNS_OPTION);
    if (encodedColumns.length() == 0)
      return false;
    
    for (String columns : encodedColumns.split(",")) {
      if (!ColumnSet.isValidEncoding(columns))
        return false;
    }
    
    return true;
  }
  
  /**
   * A convenience method to set which columns a combiner should be applied to.
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
   * A convenience method to set the "all columns" option on a Combiner. If true, the columns option will be ignored and the Combiner will be applied to all
   * columns.
   * 
   * @param is
   *          iterator settings object to configure
   * @param enableAllColumns
   *          if true the combiner will be applied to all columns
   */
  public static void setCombineAllColumns(IteratorSetting is, boolean combineAllColumns) {
    is.addOption(ALL_OPTION, Boolean.toString(combineAllColumns));
  }
}
