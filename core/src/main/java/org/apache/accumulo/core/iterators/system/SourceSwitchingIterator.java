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
package org.apache.accumulo.core.iterators.system;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class SourceSwitchingIterator implements SortedKeyValueIterator<Key,Value>, InterruptibleIterator {
  
  public interface DataSource {
    boolean isCurrent();
    
    DataSource getNewDataSource();
    
    DataSource getDeepCopyDataSource(IteratorEnvironment env);
    
    SortedKeyValueIterator<Key,Value> iterator() throws IOException;
  }
  
  private DataSource source;
  private SortedKeyValueIterator<Key,Value> iter;
  
  private Key key;
  private Value val;
  
  private Range range;
  private boolean inclusive;
  private Collection<ByteSequence> columnFamilies;
  
  private boolean onlySwitchAfterRow;
  private AtomicBoolean iflag;
  
  private List<SourceSwitchingIterator> copies;
  
  private SourceSwitchingIterator(DataSource source, boolean onlySwitchAfterRow, List<SourceSwitchingIterator> copies, AtomicBoolean iflag) {
    this.source = source;
    this.onlySwitchAfterRow = onlySwitchAfterRow;
    this.copies = copies;
    this.iflag = iflag;
    copies.add(this);
  }
  
  public SourceSwitchingIterator(DataSource source, boolean onlySwitchAfterRow) {
    this(source, onlySwitchAfterRow, Collections.synchronizedList(new ArrayList<SourceSwitchingIterator>()), null);
  }
  
  public SourceSwitchingIterator(DataSource source) {
    this(source, false);
  }
  
  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new SourceSwitchingIterator(source.getDeepCopyDataSource(env), onlySwitchAfterRow, copies, iflag);
  }
  
  @Override
  public Key getTopKey() {
    return key;
  }
  
  @Override
  public Value getTopValue() {
    return val;
  }
  
  @Override
  public boolean hasTop() {
    return key != null;
  }
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void next() throws IOException {
    readNext(false);
  }
  
  private synchronized void readNext(boolean initialSeek) throws IOException {
    
    // check of initialSeek second is intentional so that it does not short
    // circuit the call to switchSource
    boolean seekNeeded = (!onlySwitchAfterRow && switchSource()) || initialSeek;
    
    if (seekNeeded)
      if (initialSeek)
        iter.seek(range, columnFamilies, inclusive);
      else
        iter.seek(new Range(key, false, range.getEndKey(), range.isEndKeyInclusive()), columnFamilies, inclusive);
    else {
      iter.next();
      if (onlySwitchAfterRow && iter.hasTop() && !source.isCurrent() && !key.getRowData().equals(iter.getTopKey().getRowData())) {
        switchSource();
        iter.seek(new Range(key.followingKey(PartialKey.ROW), true, range.getEndKey(), range.isEndKeyInclusive()), columnFamilies, inclusive);
      }
    }
    
    if (iter.hasTop()) {
      Key nextKey = iter.getTopKey();
      Value nextVal = iter.getTopValue();
      
      try {
        key = (Key) nextKey.clone();
      } catch (CloneNotSupportedException e) {
        throw new IOException(e);
      }
      val = nextVal;
    } else {
      key = null;
      val = null;
    }
  }
  
  private boolean switchSource() throws IOException {
    while (!source.isCurrent()) {
      source = source.getNewDataSource();
      iter = source.iterator();
      if (iflag != null)
        ((InterruptibleIterator) iter).setInterruptFlag(iflag);
      
      return true;
    }
    
    return false;
  }
  
  @Override
  public synchronized void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    this.range = range;
    this.inclusive = inclusive;
    this.columnFamilies = columnFamilies;
    
    if (iter == null) {
      iter = source.iterator();
      if (iflag != null)
        ((InterruptibleIterator) iter).setInterruptFlag(iflag);
    }
    
    readNext(true);
  }
  
  private synchronized void _switchNow() throws IOException {
    if (onlySwitchAfterRow)
      throw new IllegalStateException("Can only switch on row boundries");
    
    if (switchSource()) {
      if (key != null) {
        iter.seek(new Range(key, true, range.getEndKey(), range.isEndKeyInclusive()), columnFamilies, inclusive);
      }
    }
  }
  
  public void switchNow() throws IOException {
    synchronized (copies) {
      for (SourceSwitchingIterator ssi : copies)
        ssi._switchNow();
    }
  }
  
  @Override
  public synchronized void setInterruptFlag(AtomicBoolean flag) {
    if (copies.size() != 1)
      throw new IllegalStateException("setInterruptFlag() called after deep copies made " + copies.size());
    
    this.iflag = flag;
    if (iter != null)
      ((InterruptibleIterator) iter).setInterruptFlag(flag);
    
  }
  
}
