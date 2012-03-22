/**
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 
 */
public class WritableIterator<K extends WritableComparable<?>,V extends Writable> implements SortedKeyValueIterator<K,V>, Writable {
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.iterators.SortedKeyValueIterator#init(org.apache.accumulo.core.iterators.SortedKeyValueIterator, java.util.Map,
   * org.apache.accumulo.core.iterators.IteratorEnvironment)
   */
  @Override
  public void init(SortedKeyValueIterator<K,V> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    // TODO Auto-generated method stub
    
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.iterators.SortedKeyValueIterator#hasTop()
   */
  @Override
  public boolean hasTop() {
    // TODO Auto-generated method stub
    return false;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.iterators.SortedKeyValueIterator#next()
   */
  @Override
  public void next() throws IOException {
    // TODO Auto-generated method stub
    
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.iterators.SortedKeyValueIterator#seek(org.apache.accumulo.core.data.Range, java.util.Collection, boolean)
   */
  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    // TODO Auto-generated method stub
    
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.iterators.SortedKeyValueIterator#getTopKey()
   */
  @Override
  public K getTopKey() {
    // TODO Auto-generated method stub
    return null;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.iterators.SortedKeyValueIterator#getTopValue()
   */
  @Override
  public V getTopValue() {
    // TODO Auto-generated method stub
    return null;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.accumulo.core.iterators.SortedKeyValueIterator#deepCopy(org.apache.accumulo.core.iterators.IteratorEnvironment)
   */
  @Override
  public SortedKeyValueIterator<K,V> deepCopy(IteratorEnvironment env) {
    // TODO Auto-generated method stub
    return null;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    
  }
  
}
