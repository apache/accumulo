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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.examples.wikisearch.util.FieldIndexKeyParser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class UniqFieldNameValueIterator extends WrappingIterator {
  
  protected static final Logger log = Logger.getLogger(UniqFieldNameValueIterator.class);
  // Wrapping iterator only accesses its private source in setSource and getSource
  // Since this class overrides these methods, it's safest to keep the source declaration here
  private SortedKeyValueIterator<Key,Value> source;
  private FieldIndexKeyParser keyParser;
  private Key topKey = null;
  private Value topValue = null;
  private Range overallRange = null;
  private Range currentSubRange;
  private Text fieldName = null;
  private Text fieldValueLowerBound = null;
  private Text fieldValueUpperBound = null;
  private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
  private static final String ONE_BYTE = "\1";
  private boolean multiRow = false;
  private boolean seekInclusive = false;
  
  // -------------------------------------------------------------------------
  // ------------- Static Methods
  public static void setLogLevel(Level l) {
    log.setLevel(l);
  }
  
  // -------------------------------------------------------------------------
  // ------------- Constructors
  public UniqFieldNameValueIterator(Text fName, Text fValLower, Text fValUpper) {
    this.fieldName = fName;
    this.fieldValueLowerBound = fValLower;
    this.fieldValueUpperBound = fValUpper;
    keyParser = createDefaultKeyParser();
    
  }
  
  public UniqFieldNameValueIterator(UniqFieldNameValueIterator other, IteratorEnvironment env) {
    source = other.getSource().deepCopy(env);
    // Set a default KeyParser
    keyParser = createDefaultKeyParser();
  }
  
  // -------------------------------------------------------------------------
  // ------------- Overrides
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    source = super.getSource();
  }
  
  @Override
  protected void setSource(SortedKeyValueIterator<Key,Value> source) {
    this.source = source;
  }
  
  @Override
  protected SortedKeyValueIterator<Key,Value> getSource() {
    return source;
  }
  
  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new UniqFieldNameValueIterator(this, env);
  }
  
  @Override
  public Key getTopKey() {
    return this.topKey;
  }
  
  @Override
  public Value getTopValue() {
    return this.topValue;
  }
  
  @Override
  public boolean hasTop() {
    return (topKey != null);
  }
  
  @Override
  public void next() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("next()");
    }
    if (!source.hasTop()) {
      topKey = null;
      topValue = null;
      return;
    }
    
    Key currentKey = topKey;
    keyParser.parse(topKey);
    String fValue = keyParser.getFieldValue();
    
    Text currentRow = currentKey.getRow();
    Text currentFam = currentKey.getColumnFamily();
    
    if (overallRange.getEndKey() != null && overallRange.getEndKey().getRow().compareTo(currentRow) < 0) {
      if (log.isDebugEnabled()) {
        log.debug("next, overall endRow: " + overallRange.getEndKey().getRow() + "  currentRow: " + currentRow);
      }
      topKey = null;
      topValue = null;
      return;
    }
    
    if (fValue.compareTo(this.fieldValueUpperBound.toString()) > 0) {
      topKey = null;
      topValue = null;
      return;
    }
    Key followingKey = new Key(currentKey.getRow(), this.fieldName, new Text(fValue + ONE_BYTE));
    if (log.isDebugEnabled()) {
      log.debug("next, followingKey to seek on: " + followingKey);
    }
    Range r = new Range(followingKey, followingKey);
    source.seek(r, EMPTY_COL_FAMS, false);
    while (true) {
      if (!source.hasTop()) {
        topKey = null;
        topValue = null;
        return;
      }
      
      Key k = source.getTopKey();
      if (!overallRange.contains(k)) {
        topKey = null;
        topValue = null;
        return;
      }
      if (log.isDebugEnabled()) {
        log.debug("next(), key: " + k + " subrange: " + this.currentSubRange);
      }
      // if (this.currentSubRange.contains(k)) {
      keyParser.parse(k);
      Text currentVal = new Text(keyParser.getFieldValue());
      if (k.getRow().equals(currentRow) && k.getColumnFamily().equals(currentFam) && currentVal.compareTo(fieldValueUpperBound) <= 0) {
        topKey = k;
        topValue = source.getTopValue();
        return;
        
      } else { // need to move to next row.
        if (this.overallRange.contains(k) && this.multiRow) {
          // need to find the next sub range
          // STEPS
          // 1. check if you moved past your current row on last call to next
          // 2. figure out next row
          // 3. build new start key with lowerbound fvalue
          // 4. seek the source
          // 5. test the subrange.
          if (k.getRow().equals(currentRow)) {
            // get next row
            currentRow = getNextRow();
            if (currentRow == null) {
              topKey = null;
              topValue = null;
              return;
            }
          } else {
            // i'm already in the next row
            currentRow = source.getTopKey().getRow();
          }
          
          // build new startKey
          Key sKey = new Key(currentRow, fieldName, fieldValueLowerBound);
          Key eKey = new Key(currentRow, fieldName, fieldValueUpperBound);
          currentSubRange = new Range(sKey, eKey);
          source.seek(currentSubRange, EMPTY_COL_FAMS, seekInclusive);
          
        } else { // not multi-row or outside overall range, we're done
          topKey = null;
          topValue = null;
          return;
        }
      }
      
    }
    
  }
  
  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("seek, range: " + range);
    }
    this.overallRange = range;
    this.seekInclusive = inclusive;
    source.seek(range, EMPTY_COL_FAMS, inclusive);
    topKey = null;
    topValue = null;
    Key sKey;
    Key eKey;
    
    if (range.isInfiniteStartKey()) {
      sKey = source.getTopKey();
      if (sKey == null) {
        return;
      }
    } else {
      sKey = range.getStartKey();
    }
    
    if (range.isInfiniteStopKey()) {
      eKey = null;
      this.multiRow = true; // assume we will go to the end of the tablet.
    } else {
      eKey = range.getEndKey();
      if (sKey.getRow().equals(eKey.getRow())) {
        this.multiRow = false;
      } else {
        this.multiRow = true;
      }
    }
    
    if (log.isDebugEnabled()) {
      log.debug("seek, multiRow:" + multiRow + " range:" + range);
    }
    
    /*
     * NOTE: If the seek range spans multiple rows, we are only interested in the fieldName:fieldValue subranges in each row. Keys will exist in the
     * overallRange that we will want to skip over so we need to create subranges per row so we don't have to examine every key in between.
     */
    
    Text sRow = sKey.getRow();
    Key ssKey = new Key(sRow, this.fieldName, this.fieldValueLowerBound);
    Key eeKey = new Key(sRow, this.fieldName, this.fieldValueUpperBound);
    this.currentSubRange = new Range(ssKey, eeKey);
    
    if (log.isDebugEnabled()) {
      log.debug("seek, currentSubRange: " + currentSubRange);
    }
    source.seek(this.currentSubRange, columnFamilies, inclusive);
    // cycle until we find a valid topKey, or we get ejected b/c we hit the
    // end of the tablet or exceeded the overallRange.
    while (topKey == null) {
      if (source.hasTop()) {
        Key k = source.getTopKey();
        if (log.isDebugEnabled()) {
          log.debug("seek, source.topKey: " + k);
        }
        if (currentSubRange.contains(k)) {
          topKey = k;
          topValue = source.getTopValue();
          
          if (log.isDebugEnabled()) {
            log.debug("seek, source has top in valid range");
          }
          
        } else { // outside of subRange.
          // if multiRow mode, get the next row and seek to it
          if (multiRow && overallRange.contains(k)) {
            
            Key fKey = sKey.followingKey(PartialKey.ROW);
            Range fRange = new Range(fKey, eKey);
            source.seek(fRange, columnFamilies, inclusive);
            
            if (source.hasTop()) {
              Text row = source.getTopKey().getRow();
              Key nKey = new Key(row, this.fieldName, this.fieldValueLowerBound);
              this.currentSubRange = new Range(nKey, eKey);
              sKey = this.currentSubRange.getStartKey();
              Range nextRange = new Range(sKey, eKey);
              source.seek(nextRange, columnFamilies, inclusive);
            } else {
              topKey = null;
              topValue = null;
              return;
            }
            
          } else { // not multi row & outside range, we're done.
            topKey = null;
            topValue = null;
            return;
          }
        }
      } else { // source does not have top, we're done
        topKey = null;
        topValue = null;
        return;
      }
    }
    
  }
  
  // -------------------------------------------------------------------------
  // ------------- Internal Methods
  private FieldIndexKeyParser createDefaultKeyParser() {
    FieldIndexKeyParser parser = new FieldIndexKeyParser();
    return parser;
  }
  
  private Text getNextRow() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("getNextRow()");
    }
    Key fakeKey = new Key(source.getTopKey().followingKey(PartialKey.ROW));
    Range fakeRange = new Range(fakeKey, fakeKey);
    source.seek(fakeRange, EMPTY_COL_FAMS, false);
    if (source.hasTop()) {
      return source.getTopKey().getRow();
    } else {
      return null;
    }
  }
}
