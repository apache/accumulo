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
package org.apache.accumulo.examples.wikisearch.iterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.examples.wikisearch.parser.EventFields;
import org.apache.accumulo.examples.wikisearch.parser.QueryEvaluator;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;


import com.esotericsoftware.kryo.Kryo;

/**
 * 
 * This iterator aggregates rows together using the specified key comparator. Subclasses will provide their own implementation of fillMap which will fill the
 * supplied EventFields object with field names (key) and field values (value). After all fields have been put into the aggregated object (by aggregating all
 * columns with the same key), the EventFields object will be compared against the supplied expression. If the expression returns true, then the return key and
 * return value can be retrieved via getTopKey() and getTopValue().
 * 
 * Optionally, the caller can set an expression (field operator value) that should not be evaluated against the event. For example, if the query is
 * "A == 'foo' and B == 'bar'", but for some reason B may not be in the data, then setting the UNEVALUATED_EXPRESSIONS option to "B == 'bar'" will allow the
 * events to be evaluated against the remainder of the expression and still return as true.
 * 
 * By default this iterator will return all Events in the shard. If the START_DATE and END_DATE are specified, then this iterator will evaluate the timestamp of
 * the key against the start and end dates. If the event date is not within the range of start to end, then it is skipped.
 * 
 * This iterator will return up the stack an EventFields object serialized using Kryo in the cell Value.
 * 
 */
public abstract class AbstractEvaluatingIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
  
  private static Logger log = Logger.getLogger(AbstractEvaluatingIterator.class);
  protected static final byte[] NULL_BYTE = new byte[0];
  public static final String QUERY_OPTION = "expr";
  public static final String UNEVALUTED_EXPRESSIONS = "unevaluated.expressions";
  
  private PartialKey comparator = null;
  protected SortedKeyValueIterator<Key,Value> iterator;
  private Key currentKey = new Key();
  private Key returnKey;
  private Value returnValue;
  private String expression;
  private QueryEvaluator evaluator;
  private EventFields event = null;
  private static Kryo kryo = new Kryo();
  private Range seekRange = null;
  private Set<String> skipExpressions = null;
  
  protected AbstractEvaluatingIterator(AbstractEvaluatingIterator other, IteratorEnvironment env) {
    iterator = other.iterator.deepCopy(env);
    event = other.event;
  }
  
  public AbstractEvaluatingIterator() {}
  
  /**
   * Implementations will return the PartialKey value to use for comparing keys for aggregating events
   * 
   * @return the type of comparator to use
   */
  public abstract PartialKey getKeyComparator();
  
  /**
   * When the query expression evaluates to true against the event, the event fields will be serialized into the Value and returned up the iterator stack.
   * Implemenations will need to provide a key to be used with the event.
   * 
   * @param k
   * @return the key that should be returned with the map of values.
   */
  public abstract Key getReturnKey(Key k) throws Exception;
  
  /**
   * Implementations will need to fill the map with field visibilities, names, and values. When all fields have been aggregated the event will be evaluated
   * against the query expression.
   * 
   * @param event
   *          Multimap of event names and fields.
   * @param key
   *          current Key
   * @param value
   *          current Value
   */
  public abstract void fillMap(EventFields event, Key key, Value value) throws Exception;
  
  /**
   * Provides the ability to skip this key and all of the following ones that match using the comparator.
   * 
   * @param key
   * @return true if the key should be acted upon, otherwise false.
   * @throws IOException
   */
  public abstract boolean isKeyAccepted(Key key) throws IOException;
  
  /**
   * Reset state.
   */
  public void reset() {
    event.clear();
  }
  
  private void aggregateRowColumn(EventFields event) throws IOException {
    
    currentKey.set(iterator.getTopKey());
    
    try {
      fillMap(event, iterator.getTopKey(), iterator.getTopValue());
      iterator.next();
      
      while (iterator.hasTop() && iterator.getTopKey().equals(currentKey, this.comparator)) {
        fillMap(event, iterator.getTopKey(), iterator.getTopValue());
        iterator.next();
      }
      
      // Get the return key
      returnKey = getReturnKey(currentKey);
    } catch (Exception e) {
      throw new IOException("Error aggregating event", e);
    }
    
  }
  
  private void findTop() throws IOException {
    do {
      reset();
      // check if aggregation is needed
      if (iterator.hasTop()) {
        // Check to see if the current key is accepted. For example in the wiki
        // table there are field index rows. We don't want to process those in
        // some cases so return right away. Consume all of the non-accepted keys
        while (iterator.hasTop() && !isKeyAccepted(iterator.getTopKey())) {
          iterator.next();
        }
        
        if (iterator.hasTop()) {
          aggregateRowColumn(event);
          
          // Evaluate the event against the expression
          if (event.size() > 0 && this.evaluator.evaluate(event)) {
            if (log.isDebugEnabled()) {
              log.debug("Event evaluated to true, key = " + returnKey);
            }
            // Create a byte array
            byte[] serializedMap = new byte[event.getByteSize() + (event.size() * 20)];
            // Wrap in ByteBuffer to work with Kryo
            ByteBuffer buf = ByteBuffer.wrap(serializedMap);
            // Serialize the EventFields object
            event.writeObjectData(kryo, buf);
            // Truncate array to the used size.
            returnValue = new Value(Arrays.copyOfRange(serializedMap, 0, buf.position()));
          } else {
            returnKey = null;
            returnValue = null;
          }
        } else {
          if (log.isDebugEnabled()) {
            log.debug("Iterator no longer has top.");
          }
        }
      } else {
        log.debug("Iterator.hasTop() == false");
      }
    } while (returnValue == null && iterator.hasTop());
    
    // Sanity check. Make sure both returnValue and returnKey are null or both are not null
    if (!((returnKey == null && returnValue == null) || (returnKey != null && returnValue != null))) {
      log.warn("Key: " + ((returnKey == null) ? "null" : returnKey.toString()));
      log.warn("Value: " + ((returnValue == null) ? "null" : returnValue.toString()));
      throw new IOException("Return values are inconsistent");
    }
  }
  
  public Key getTopKey() {
    if (returnKey != null) {
      return returnKey;
    }
    return iterator.getTopKey();
  }
  
  public Value getTopValue() {
    if (returnValue != null) {
      return returnValue;
    }
    return iterator.getTopValue();
  }
  
  public boolean hasTop() {
    return returnKey != null || iterator.hasTop();
  }
  
  public void next() throws IOException {
    if (returnKey != null) {
      returnKey = null;
      returnValue = null;
    } else if (iterator.hasTop()) {
      iterator.next();
    }
    
    findTop();
  }
  
  /**
   * Copy of IteratorUtil.maximizeStartKeyTimeStamp due to IllegalAccessError
   * 
   * @param range
   * @return
   */
  static Range maximizeStartKeyTimeStamp(Range range) {
    Range seekRange = range;
    
    if (range.getStartKey() != null && range.getStartKey().getTimestamp() != Long.MAX_VALUE) {
      Key seekKey = new Key(seekRange.getStartKey());
      seekKey.setTimestamp(Long.MAX_VALUE);
      seekRange = new Range(seekKey, true, range.getEndKey(), range.isEndKeyInclusive());
    }
    
    return seekRange;
  }
  
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    // do not want to seek to the middle of a value that should be
    // aggregated...
    
    seekRange = maximizeStartKeyTimeStamp(range);
    
    iterator.seek(seekRange, columnFamilies, inclusive);
    findTop();
    
    if (range.getStartKey() != null) {
      while (hasTop() && getTopKey().equals(range.getStartKey(), this.comparator) && getTopKey().getTimestamp() > range.getStartKey().getTimestamp()) {
        // the value has a more recent time stamp, so
        // pass it up
        // log.debug("skipping "+getTopKey());
        next();
      }
      
      while (hasTop() && range.beforeStartKey(getTopKey())) {
        next();
      }
    }
    
  }
  
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    validateOptions(options);
    event = new EventFields();
    this.comparator = getKeyComparator();
    this.iterator = source;
    try {
      // Replace any expressions that we should not evaluate.
      if (null != this.skipExpressions && this.skipExpressions.size() != 0) {
        for (String skip : this.skipExpressions) {
          // Expression should have form: field<sp>operator<sp>literal.
          // We are going to replace the expression with field == null.
          String field = skip.substring(0, skip.indexOf(" ") - 1);
          this.expression = this.expression.replaceAll(skip, field + " == null");
        }
      }
      this.evaluator = new QueryEvaluator(this.expression);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Failed to parse query", e);
    }
    EventFields.initializeKryo(kryo);
  }
  
  public IteratorOptions describeOptions() {
    Map<String,String> options = new HashMap<String,String>();
    options.put(QUERY_OPTION, "query expression");
    options.put(UNEVALUTED_EXPRESSIONS, "comma separated list of expressions to skip");
    return new IteratorOptions(getClass().getSimpleName(), "evaluates event objects against an expression", options, null);
  }
  
  public boolean validateOptions(Map<String,String> options) {
    if (!options.containsKey(QUERY_OPTION))
      return false;
    else
      this.expression = options.get(QUERY_OPTION);
    
    if (options.containsKey(UNEVALUTED_EXPRESSIONS)) {
      String expressionList = options.get(UNEVALUTED_EXPRESSIONS);
      if (expressionList != null && !expressionList.trim().equals("")) {
        this.skipExpressions = new HashSet<String>();
        for (String e : expressionList.split(","))
          this.skipExpressions.add(e);
      }
    }
    return true;
  }
  
  public String getQueryExpression() {
    return this.expression;
  }
}
