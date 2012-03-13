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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

/**
 * This iterator internally uses the BooleanLogicIterator to find event UIDs in the field index portion of the partition and uses the EvaluatingIterator to
 * evaluate the events against an expression. The key and value that are emitted from this iterator are the key and value that come from the EvaluatingIterator.
 */
public class OptimizedQueryIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
  
  private static Logger log = Logger.getLogger(OptimizedQueryIterator.class);
  private EvaluatingIterator event = null;
  private SortedKeyValueIterator<Key,Value> index = null;
  private Key key = null;
  private Value value = null;
  private boolean eventSpecificRange = false;
  
  public IteratorOptions describeOptions() {
    Map<String,String> options = new HashMap<String,String>();
    options.put(EvaluatingIterator.QUERY_OPTION, "full query expression");
    options.put(BooleanLogicIterator.FIELD_INDEX_QUERY, "modified query for the field index query portion");
    options.put(ReadAheadIterator.QUEUE_SIZE, "parallel queue size");
    options.put(ReadAheadIterator.TIMEOUT, "parallel iterator timeout");
    return new IteratorOptions(getClass().getSimpleName(), "evaluates event objects against an expression using the field index", options, null);
  }
  
  public boolean validateOptions(Map<String,String> options) {
    if (options.containsKey(EvaluatingIterator.QUERY_OPTION) && options.containsKey(BooleanLogicIterator.FIELD_INDEX_QUERY)) {
      return true;
    }
    return false;
  }
  
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    if (!validateOptions(options)) {
      throw new IllegalArgumentException("Invalid options");
    }
    
    // Setup the EvaluatingIterator
    event = new EvaluatingIterator();
    event.init(source.deepCopy(env), options, env);
    
    // if queue size and timeout are set, then use the read ahead iterator
    if (options.containsKey(ReadAheadIterator.QUEUE_SIZE) && options.containsKey(ReadAheadIterator.TIMEOUT)) {
      BooleanLogicIterator bli = new BooleanLogicIterator();
      bli.init(source, options, env);
      index = new ReadAheadIterator();
      index.init(bli, options, env);
    } else {
      index = new BooleanLogicIterator();
      // index.setDebug(Level.DEBUG);
      index.init(source, options, env);
    }
    
  }
  
  public OptimizedQueryIterator() {}
  
  public OptimizedQueryIterator(OptimizedQueryIterator other, IteratorEnvironment env) {
    this.event = other.event;
    this.index = other.index;
  }
  
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new OptimizedQueryIterator(this, env);
  }
  
  public Key getTopKey() {
    if (log.isDebugEnabled()) {
      log.debug("getTopKey: " + key);
    }
    return key;
  }
  
  public Value getTopValue() {
    if (log.isDebugEnabled()) {
      log.debug("getTopValue: " + value);
    }
    return value;
  }
  
  public boolean hasTop() {
    if (log.isDebugEnabled()) {
      log.debug("hasTop: returned: " + (key != null));
    }
    return (key != null);
  }
  
  public void next() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("next");
    }
    if (key != null) {
      key = null;
      value = null;
    }
    
    if (eventSpecificRange) {
      // Then this will probably return nothing
      event.next();
      if (event.hasTop()) {
        key = event.getTopKey();
        value = event.getTopValue();
      }
    } else {
      
      do {
        index.next();
        // If the index has a match, then seek the event to the key
        if (index.hasTop()) {
          Key eventKey = index.getTopKey();
          Key endKey = eventKey.followingKey(PartialKey.ROW_COLFAM);
          Key startKey = new Key(eventKey.getRow(), eventKey.getColumnFamily());
          Range eventRange = new Range(startKey, endKey);
          HashSet<ByteSequence> cf = new HashSet<ByteSequence>();
          cf.add(eventKey.getColumnFamilyData());
          event.seek(eventRange, cf, true);
          if (event.hasTop()) {
            key = event.getTopKey();
            value = event.getTopValue();
          }
        }
      } while (key == null && index.hasTop());
    }
    // Sanity check. Make sure both returnValue and returnKey are null or both are not null
    if (!((key == null && value == null) || (key != null && value != null))) {
      log.warn("Key: " + ((key == null) ? "null" : key.toString()));
      log.warn("Value: " + ((value == null) ? "null" : value.toString()));
      throw new IOException("Return values are inconsistent");
    }
    
  }
  
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("seek, range:" + range);
    }
    // Test the range to see if it is event specific.
    if (null != range.getEndKey() && range.getEndKey().getColumnFamily() != null && range.getEndKey().getColumnFamily().getLength() != 0) {
      if (log.isDebugEnabled()) {
        log.debug("Jumping straight to the event");
      }
      // Then this range is for a specific event. We don't need to use the index iterator to find it, we can just
      // seek to it with the event iterator and evaluate it.
      eventSpecificRange = true;
      event.seek(range, columnFamilies, inclusive);
      if (event.hasTop()) {
        key = event.getTopKey();
        value = event.getTopValue();
      }
    } else {
      if (log.isDebugEnabled()) {
        log.debug("Using BooleanLogicIteratorJexl");
      }
      // Seek the boolean logic iterator
      index.seek(range, columnFamilies, inclusive);
      
      // If the index has a match, then seek the event to the key
      if (index.hasTop()) {
        Key eventKey = index.getTopKey();
        // Range eventRange = new Range(eventKey, eventKey);
        Range eventRange = new Range(eventKey.getRow());
        HashSet<ByteSequence> cf = new HashSet<ByteSequence>();
        cf.add(eventKey.getColumnFamilyData());
        event.seek(eventRange, cf, true);
        if (event.hasTop()) {
          key = event.getTopKey();
          value = event.getTopValue();
        } else {
          next();
        }
      }
    }
  }
}
