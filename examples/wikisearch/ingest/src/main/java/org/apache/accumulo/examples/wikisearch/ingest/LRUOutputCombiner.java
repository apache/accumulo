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
package org.apache.accumulo.examples.wikisearch.ingest;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUOutputCombiner<Key,Value> extends LinkedHashMap<Key,Value> {
  
  private static final long serialVersionUID = 1L;
  
  public static abstract class Fold<Value> {
    public abstract Value fold(Value oldValue, Value newValue);
  }
  
  public static abstract class Output<Key,Value> {
    public abstract void output(Key key, Value value);
  }
  
  private final int capacity;
  private final Fold<Value> fold;
  private final Output<Key,Value> output;
  
  private long cacheHits = 0;
  private long cacheMisses = 0;
  
  public LRUOutputCombiner(int capacity, Fold<Value> fold, Output<Key,Value> output) {
    super(capacity + 1, 1.1f, true);
    this.capacity = capacity;
    this.fold = fold;
    this.output = output;
  }
  
  protected boolean removeEldestEntry(Map.Entry<Key,Value> eldest) {
    if (size() > capacity) {
      output.output(eldest.getKey(), eldest.getValue());
      return true;
    }
    return false;
  }
  
  @Override
  public Value put(Key key, Value value) {
    Value val = get(key);
    if (val != null) {
      value = fold.fold(val, value);
      cacheHits++;
    } else {
      cacheMisses++;
    }
    super.put(key, value);
    return null;
  }
  
  public void flush() {
    for (Map.Entry<Key,Value> e : entrySet()) {
      output.output(e.getKey(), e.getValue());
    }
    clear();
  }
}
