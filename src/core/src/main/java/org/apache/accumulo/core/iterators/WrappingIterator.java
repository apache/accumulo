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
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

public abstract class WrappingIterator implements SortedKeyValueIterator<Key,Value> {
  
  private SortedKeyValueIterator<Key,Value> source;
  
  protected void setSource(SortedKeyValueIterator<Key,Value> source) {
    this.source = source;
  }
  
  protected SortedKeyValueIterator<Key,Value> getSource() {
    return source;
  }
  
  @Override
  public abstract SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env);
  
  @Override
  public Key getTopKey() {
    return getSource().getTopKey();
  }
  
  @Override
  public Value getTopValue() {
    return getSource().getTopValue();
  }
  
  @Override
  public boolean hasTop() {
    return getSource().hasTop();
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    this.setSource(source);
    
  }
  
  @Override
  public void next() throws IOException {
    getSource().next();
  }
  
  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    getSource().seek(range, columnFamilies, inclusive);
  }
  
}
