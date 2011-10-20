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
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * An iterator that support iterating over key and value pairs. Anything implementing this interface should return keys in sorted order.
 * 
 * 
 */

public interface SortedKeyValueIterator<K extends WritableComparable<?>,V extends Writable> {
  
  void init(SortedKeyValueIterator<K,V> source, Map<String,String> options, IteratorEnvironment env) throws IOException;
  
  // we should add method to get a continue key that appropriately translates
  
  boolean hasTop();
  
  void next() throws IOException;
  
  /**
   * An iterator must seek to the first key in the range taking inclusiveness into account. However, an iterator does not have to stop at the end of the range.
   * The whole range is provided so that iterators can make optimizations.
   */
  void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException;
  
  K getTopKey();
  
  V getTopValue();
  
  // create a deep copy of this iterator as though seek had not yet been called
  // init must not be called after clone, on either of the instances
  SortedKeyValueIterator<K,V> deepCopy(IteratorEnvironment env);
}
