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
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * An iterator that handles "OR" query constructs on the server side. This code has been adapted/merged from Heap and Multi Iterators.
 */
public class OrIterator implements SortedKeyValueIterator<Key,Value> {
  
  private TermSource currentTerm;
  private ArrayList<TermSource> sources;
  private PriorityQueue<TermSource> sorted = new PriorityQueue<TermSource>(5);
  private static final Text nullText = new Text();
  private Key topKey = null;
  private Range overallRange;
  private Collection<ByteSequence> columnFamilies;
  private boolean inclusive;
  protected static final Logger log = Logger.getLogger(OrIterator.class);
  private Text parentEndRow;
  
  protected static class TermSource implements Comparable<TermSource> {
    
    public SortedKeyValueIterator<Key,Value> iter;
    public Text dataLocation;
    public Text term;
    public Text docid;
    public Text fieldTerm;
    public Key topKey;
    public boolean atEnd;
    
    public TermSource(TermSource other) {
      this.iter = other.iter;
      this.term = other.term;
      this.dataLocation = other.dataLocation;
      this.atEnd = other.atEnd;
    }
    
    public TermSource(SortedKeyValueIterator<Key,Value> iter, Text term) {
      this.iter = iter;
      this.term = term;
      this.atEnd = false;
    }
    
    public TermSource(SortedKeyValueIterator<Key,Value> iter, Text dataLocation, Text term) {
      this.iter = iter;
      this.dataLocation = dataLocation;
      this.term = term;
      this.atEnd = false;
    }
    
    public void setNew() {
      if (!this.atEnd && this.iter.hasTop()) {
        this.topKey = this.iter.getTopKey();
        
        if (log.isDebugEnabled()) {
          log.debug("OI.TermSource.setNew TS.iter.topKey >>" + topKey + "<<");
        }
        
        if (this.term == null) {
          this.docid = this.topKey.getColumnQualifier();
        } else {
          String cqString = this.topKey.getColumnQualifier().toString();
          
          int idx = cqString.indexOf("\0");
          this.fieldTerm = new Text(cqString.substring(0, idx));
          this.docid = new Text(cqString.substring(idx + 1));
        }
      } else {
        if (log.isDebugEnabled()) {
          log.debug("OI.TermSource.setNew Setting to null...");
        }
        
        // this.term = null;
        // this.dataLocation = null;
        this.topKey = null;
        this.fieldTerm = null;
        this.docid = null;
      }
    }
    
    public int compareTo(TermSource o) {
      // NOTE: If your implementation can have more than one row in a tablet,
      // you must compare row key here first, then column qualifier.
      // NOTE2: A null check is not needed because things are only added to the
      // sorted after they have been determined to be valid.
      // return this.docid.compareTo(o.docid);
      // return this.topKey.compareTo(o.topKey);
      
      // NOTE! We need to compare UID's, not Keys!
      Key k1 = topKey;
      Key k2 = o.topKey;
      // return t1.compareTo(t2);
      String uid1 = getUID(k1);
      String uid2 = getUID(k2);
      
      if (uid1 != null && uid2 != null) {
        return uid1.compareTo(uid2);
      } else if (uid1 == null && uid2 == null) {
        return 0;
      } else if (uid1 == null) {
        return 1;
      } else {
        return -1;
      }
      
    }
    
    @Override
    public String toString() {
      return "TermSource: " + this.dataLocation + " " + this.term;
    }
    
    public boolean hasTop() {
      return this.topKey != null;
    }
  }
  
  /**
   * Returns the given key's row
   * 
   * @param key
   * @return The given key's row
   */
  protected Text getPartition(Key key) {
    return key.getRow();
  }
  
  /**
   * Returns the given key's dataLocation
   * 
   * @param key
   * @return The given key's dataLocation
   */
  protected Text getDataLocation(Key key) {
    return key.getColumnFamily();
  }
  
  /**
   * Returns the given key's term
   * 
   * @param key
   * @return The given key's term
   */
  protected Text getTerm(Key key) {
    int idx = 0;
    String sKey = key.getColumnQualifier().toString();
    
    idx = sKey.indexOf("\0");
    return new Text(sKey.substring(0, idx));
  }
  
  /**
   * Returns the given key's DocID
   * 
   * @param key
   * @return The given key's DocID
   */
  protected Text getDocID(Key key) {
    int idx = 0;
    String sKey = key.getColumnQualifier().toString();
    
    idx = sKey.indexOf("\0");
    return new Text(sKey.substring(idx + 1));
  }
  
  /**
   * Returns the given key's UID
   * 
   * @param key
   * @return The given key's UID
   */
  static protected String getUID(Key key) {
    try {
      int idx = 0;
      String sKey = key.getColumnQualifier().toString();
      
      idx = sKey.indexOf("\0");
      return sKey.substring(idx + 1);
    } catch (Exception e) {
      return null;
    }
  }
  
  public OrIterator() {
    this.sources = new ArrayList<TermSource>();
  }
  
  private OrIterator(OrIterator other, IteratorEnvironment env) {
    this.sources = new ArrayList<TermSource>();
    
    for (TermSource TS : other.sources) {
      this.sources.add(new TermSource(TS.iter.deepCopy(env), TS.dataLocation, TS.term));
    }
  }
  
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new OrIterator(this, env);
  }
  
  public void addTerm(SortedKeyValueIterator<Key,Value> source, Text term, IteratorEnvironment env) {
    if (log.isDebugEnabled()) {
      log.debug("OI.addTerm Added source w/o family");
      log.debug("OI.addTerm term >>" + term + "<<");
    }
    
    // Don't deepcopy an iterator
    if (term == null) {
      this.sources.add(new TermSource(source, term));
    } else {
      this.sources.add(new TermSource(source.deepCopy(env), term));
    }
  }
  
  public void addTerm(SortedKeyValueIterator<Key,Value> source, Text dataLocation, Text term, IteratorEnvironment env) {
    if (log.isDebugEnabled()) {
      log.debug("OI.addTerm Added source ");
      log.debug("OI.addTerm family >>" + dataLocation + "<<      term >>" + term + "<<");
    }
    
    // Don't deepcopy an iterator
    if (term == null) {
      this.sources.add(new TermSource(source, dataLocation, term));
    } else {
      this.sources.add(new TermSource(source.deepCopy(env), dataLocation, term));
    }
  }
  
  /**
   * Construct the topKey given the current <code>TermSource</code>
   * 
   * @param TS
   * @return The top Key for a given TermSource
   */
  protected Key buildTopKey(TermSource TS) {
    if ((TS == null) || (TS.topKey == null)) {
      return null;
    }
    
    if (log.isDebugEnabled()) {
      log.debug("OI.buildTopKey New topKey >>" + new Key(TS.topKey.getRow(), TS.dataLocation, TS.docid) + "<<");
    }
    
    return new Key(TS.topKey.getRow(), TS.topKey.getColumnFamily(), TS.topKey.getColumnQualifier());
  }
  
  final public void next() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("OI.next Enter: sorted.size = " + sorted.size() + " currentTerm = " + ((currentTerm == null) ? "null" : "not null"));
    }
    
    if (currentTerm == null) {
      if (log.isDebugEnabled()) {
        log.debug("OI.next currentTerm is NULL... returning");
      }
      
      topKey = null;
      return;
    }
    
    // Advance currentTerm
    currentTerm.iter.next();
    
    advanceToMatch(currentTerm);
    
    currentTerm.setNew();
    
    // See if currentTerm is still valid, remove if not
    if (log.isDebugEnabled()) {
      log.debug("OI.next Checks (correct = 0,0,0): " + ((currentTerm.topKey != null) ? "0," : "1,") + ((currentTerm.dataLocation != null) ? "0," : "1,")
          + ((currentTerm.term != null && currentTerm.fieldTerm != null) ? (currentTerm.term.compareTo(currentTerm.fieldTerm)) : "0"));
    }
    
    if (currentTerm.topKey == null || ((currentTerm.dataLocation != null) && (currentTerm.term.compareTo(currentTerm.fieldTerm) != 0))) {
      if (log.isDebugEnabled()) {
        log.debug("OI.next removing entry:" + currentTerm.term);
      }
      
      currentTerm = null;
    }
    
    // optimization.
    // if size == 0, currentTerm is the only item left,
    // OR there are no items left.
    // In either case, we don't need to use the PriorityQueue
    if (sorted.size() > 0) {
      // sort the term back in
      if (currentTerm != null) {
        sorted.add(currentTerm);
      }
      // and get the current top item out.
      currentTerm = sorted.poll();
    }
    
    if (log.isDebugEnabled()) {
      log.debug("OI.next CurrentTerm is " + ((currentTerm == null) ? "null" : currentTerm));
    }
    
    topKey = buildTopKey(currentTerm);
    
    if (hasTop()) {
      if (overallRange != null && !overallRange.contains(topKey)) {
        topKey = null;
      }
    }
  }
  
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    
    overallRange = new Range(range);
    if (log.isDebugEnabled()) {
      log.debug("seek, overallRange: " + overallRange);
    }
    
    if (range.getEndKey() != null && range.getEndKey().getRow() != null) {
      this.parentEndRow = range.getEndKey().getRow();
    }
    
    if (log.isDebugEnabled()) {
      log.debug("OI.seek Entry - sources.size = " + sources.size());
      log.debug("OI.seek Entry - currentTerm = " + ((currentTerm == null) ? "false" : currentTerm.iter.getTopKey()));
      log.debug("OI.seek Entry - Key from Range = " + ((range == null) ? "false" : range.getStartKey()));
    }
    
    // If sources.size is 0, there is nothing to process, so just return.
    if (sources.isEmpty()) {
      currentTerm = null;
      topKey = null;
      return;
    }
    
    this.columnFamilies = columnFamilies;
    this.inclusive = inclusive;
    
    Range newRange = range;
    Key sourceKey = null;
    Key startKey = null;
    
    if (range != null) {
      startKey = range.getStartKey();
    }
    
    // Clear the PriorityQueue so that we can re-populate it.
    sorted.clear();
    
    TermSource TS = null;
    Iterator<TermSource> iter = sources.iterator();
    // For each term, seek forward.
    // if a hit is not found, delete it from future searches.
    int counter = 1;
    while (iter.hasNext()) {
      TS = iter.next();
      
      TS.atEnd = false;
      
      if (sources.size() == 1) {
        currentTerm = TS;
      }
      
      if (log.isDebugEnabled()) {
        log.debug("OI.seek on TS >>" + TS + "<<");
        log.debug("OI.seek seeking source >>" + counter + "<< ");
      }
      
      counter++;
      
      newRange = range;
      sourceKey = null;
      
      if (startKey != null) {
        // Construct the new key for the range
        if (log.isDebugEnabled()) {
          log.debug("OI.seek startKey >>" + startKey + "<<");
        }
        
        if (startKey.getColumnQualifier() != null) {
          sourceKey = new Key(startKey.getRow(), (TS.dataLocation == null) ? nullText : TS.dataLocation, new Text(((TS.term == null) ? "" : TS.term + "\0")
              + range.getStartKey().getColumnQualifier()));
        } else {
          sourceKey = new Key(startKey.getRow(), (TS.dataLocation == null) ? nullText : TS.dataLocation, (TS.term == null) ? nullText : TS.term);
        }
        
        if (log.isDebugEnabled()) {
          log.debug("OI.seek Seeking to the key => " + sourceKey);
        }
        
        newRange = new Range(sourceKey, true, sourceKey.followingKey(PartialKey.ROW), false);
      } else {
        if (log.isDebugEnabled()) {
          log.debug("OI.seek Using the range Seek() argument to seek => " + newRange);
        }
      }
      
      TS.iter.seek(newRange, columnFamilies, inclusive);
      
      TS.setNew();
      
      // Make sure we're on a key with the correct dataLocation and term
      advanceToMatch(TS);
      
      TS.setNew();
      
      if (log.isDebugEnabled()) {
        log.debug("OI.seek sourceKey >>" + sourceKey + "<< ");
        log.debug("OI.seek topKey >>" + ((TS.topKey == null) ? "false" : TS.topKey) + "<< ");
        log.debug("OI.seek TS.fieldTerm == " + TS.fieldTerm);
        
        log.debug("OI.seek Checks (correct = 0,0,0 / 0,1,1): " + ((TS.topKey != null) ? "0," : "1,") + ((TS.dataLocation != null) ? "0," : "1,")
            + (((TS.term != null && TS.fieldTerm != null) && (TS.term.compareTo(TS.fieldTerm) != 0)) ? "0" : "1"));
      }
      
      if ((TS.topKey == null) || ((TS.dataLocation != null) && (TS.term.compareTo(TS.fieldTerm) != 0))) {
        // log.debug("OI.seek Removing " + TS.term);
        // iter.remove();
      } // Optimization if we only have one element
      else if (sources.size() > 0 || iter.hasNext()) {
        // We have more than one source to search for, use the priority queue
        sorted.add(TS);
      } else {
        // Don't need to continue, only had one item to search
        if (log.isDebugEnabled()) {
          log.debug("OI.seek new topKey >>" + ((topKey == null) ? "false" : topKey) + "<< ");
        }
        
        // make sure it is in the range if we have one.
        if (hasTop()) {
          if (overallRange != null && !overallRange.contains(topKey)) {
            if (log.isDebugEnabled()) {
              log.debug("seek, topKey: " + topKey + " is not in the overallRange: " + overallRange);
            }
            topKey = null;
          }
        }
        return;
      }
    }
    
    // And set currentTerm = the next valid key/term.
    currentTerm = sorted.poll();
    
    if (log.isDebugEnabled()) {
      log.debug("OI.seek currentTerm = " + currentTerm);
    }
    
    topKey = buildTopKey(currentTerm);
    if (topKey == null) {
      if (log.isDebugEnabled()) {
        log.debug("OI.seek() topKey is null");
      }
    }
    
    if (log.isDebugEnabled()) {
      log.debug("OI.seek new topKey >>" + ((topKey == null) ? "false" : topKey) + "<< ");
    }
    
    if (hasTop()) {
      if (overallRange != null && !overallRange.contains(topKey)) {
        if (log.isDebugEnabled()) {
          log.debug("seek, topKey: " + topKey + " is not in the overallRange: " + overallRange);
        }
        topKey = null;
      }
    }
    
  }
  
  final public Key getTopKey() {
    if (log.isDebugEnabled()) {
      log.debug("OI.getTopKey key >>" + topKey);
    }
    
    return topKey;
  }
  
  final public Value getTopValue() {
    if (log.isDebugEnabled()) {
      log.debug("OI.getTopValue key >>" + currentTerm.iter.getTopValue());
    }
    
    return currentTerm.iter.getTopValue();
  }
  
  final public boolean hasTop() {
    if (log.isDebugEnabled()) {
      log.debug("OI.hasTop  =  " + ((topKey == null) ? "false" : "true"));
    }
    
    return topKey != null;
  }
  
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  /**
   * Ensures that the current <code>TermSource</code> is pointing to a key with the correct <code>dataLocation</code> and <code>term</code> or sets
   * <code>topKey</code> to null if there is no such key remaining.
   * 
   * @param TS
   *          The <code>TermSource</code> to advance
   * @throws IOException
   */
  private void advanceToMatch(TermSource TS) throws IOException {
    boolean matched = false;
    while (!matched) {
      if (!TS.iter.hasTop()) {
        TS.topKey = null;
        return;
      }
      
      Key iterTopKey = TS.iter.getTopKey();
      
      if (log.isDebugEnabled()) {
        log.debug("OI.advanceToMatch current topKey = " + iterTopKey);
      }
      
      // we should compare the row to the end of the range
      if (overallRange.getEndKey() != null) {
        
        if (overallRange != null && !overallRange.contains(TS.iter.getTopKey())) {
          if (log.isDebugEnabled()) {
            log.debug("overallRange: " + overallRange + " does not contain TS.iter.topKey: " + TS.iter.getTopKey());
            log.debug("OI.advanceToMatch at the end, returning");
          }
          
          TS.atEnd = true;
          TS.topKey = null;
          
          return;
        } else {
          if (log.isDebugEnabled()) {
            log.debug("OI.advanceToMatch not at the end");
          }
        }
      } else {
        if (log.isDebugEnabled()) {
          log.debug("OI.advanceToMatch overallRange.getEndKey() == null");
        }
      }
      
      // Advance to the correct dataLocation
      if (log.isDebugEnabled()) {
        log.debug("Comparing dataLocations.");
        log.debug("OI.advanceToMatch dataLocationCompare: " + getDataLocation(iterTopKey) + " == " + TS.dataLocation);
      }
      
      int dataLocationCompare = getDataLocation(iterTopKey).compareTo(TS.dataLocation);
      
      if (log.isDebugEnabled()) {
        log.debug("OI.advanceToMatch dataLocationCompare = " + dataLocationCompare);
      }
      
      // Make sure we're at a row for this dataLocation
      if (dataLocationCompare < 0) {
        if (log.isDebugEnabled()) {
          log.debug("OI.advanceToMatch seek to desired dataLocation");
        }
        
        Key seekKey = new Key(iterTopKey.getRow(), TS.dataLocation, nullText);
        
        if (log.isDebugEnabled()) {
          log.debug("OI.advanceToMatch seeking to => " + seekKey);
        }
        
        TS.iter.seek(new Range(seekKey, true, null, false), columnFamilies, inclusive);
        
        continue;
      } else if (dataLocationCompare > 0) {
        if (log.isDebugEnabled()) {
          log.debug("OI.advanceToMatch advanced beyond desired dataLocation, seek to next row");
        }
        
        // Gone past the current dataLocation, seek to the next row
        Key seekKey = iterTopKey.followingKey(PartialKey.ROW);
        
        if (log.isDebugEnabled()) {
          log.debug("OI.advanceToMatch seeking to => " + seekKey);
        }
        
        TS.iter.seek(new Range(seekKey, true, null, false), columnFamilies, inclusive);
        
        continue;
      }
      
      // Advance to the correct term
      if (log.isDebugEnabled()) {
        log.debug("OI.advanceToMatch termCompare: " + getTerm(iterTopKey) + " == " + TS.term);
      }
      
      int termCompare = getTerm(iterTopKey).compareTo(TS.term);
      
      if (log.isDebugEnabled()) {
        log.debug("OI.advanceToMatch termCompare = " + termCompare);
      }
      
      // Make sure we're at a row for this term
      if (termCompare < 0) {
        if (log.isDebugEnabled()) {
          log.debug("OI.advanceToMatch seek to desired term");
        }
        
        Key seekKey = new Key(iterTopKey.getRow(), iterTopKey.getColumnFamily(), TS.term);
        
        if (log.isDebugEnabled()) {
          log.debug("OI.advanceToMatch seeking to => " + seekKey);
        }
        
        TS.iter.seek(new Range(seekKey, true, null, false), columnFamilies, inclusive);
        
        continue;
      } else if (termCompare > 0) {
        if (log.isDebugEnabled()) {
          log.debug("OI.advanceToMatch advanced beyond desired term, seek to next row");
        }
        
        // Gone past the current term, seek to the next row
        Key seekKey = iterTopKey.followingKey(PartialKey.ROW);
        
        if (log.isDebugEnabled()) {
          log.debug("OI.advanceToMatch seeking to => " + seekKey);
        }
        
        TS.iter.seek(new Range(seekKey, true, null, false), columnFamilies, inclusive);
        continue;
      }
      
      // If we made it here, we found a match
      matched = true;
    }
  }
  
  public boolean jump(Key jumpKey) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("OR jump: " + jumpKey);
      printTopKeysForTermSources();
    }
    
    // is the jumpKey outside my overall range?
    if (parentEndRow != null && parentEndRow.compareTo(jumpKey.getRow()) < 0) {
      // can't go there.
      if (log.isDebugEnabled()) {
        log.debug("jumpRow: " + jumpKey.getRow() + " is greater than my parentEndRow: " + parentEndRow);
      }
      return false;
    }
    
    // Clear the PriorityQueue so that we can re-populate it.
    sorted.clear();
    
    // check each term source and jump it if necessary.
    for (TermSource ts : sources) {
      int comp;
      if (!ts.hasTop()) {
        if (log.isDebugEnabled()) {
          log.debug("jump called, but ts.topKey is null, this one needs to move to next row.");
        }
        Key startKey = new Key(jumpKey.getRow(), ts.dataLocation, new Text(ts.term + "\0" + jumpKey.getColumnFamily()));
        Key endKey = null;
        if (parentEndRow != null) {
          endKey = new Key(parentEndRow);
        }
        Range newRange = new Range(startKey, true, endKey, false);
        ts.iter.seek(newRange, columnFamilies, inclusive);
        ts.setNew();
        advanceToMatch(ts);
        ts.setNew();
        
      } else {
        // check row, then uid
        comp = this.topKey.getRow().compareTo(jumpKey.getRow());
        if (comp > 0) {
          if (log.isDebugEnabled()) {
            log.debug("jump, our row is ahead of jumpKey.");
            log.debug("jumpRow: " + jumpKey.getRow() + " myRow: " + topKey.getRow() + " parentEndRow" + parentEndRow);
          }
          if (ts.hasTop()) {
            sorted.add(ts);
          }
          // do nothing, we're ahead of jumpKey row and have topkey
        } else if (comp < 0) { // a row behind jump key, need to move forward
          if (log.isDebugEnabled()) {
            log.debug("OR jump, row jump");
          }
          Key endKey = null;
          if (parentEndRow != null) {
            endKey = new Key(parentEndRow);
          }
          Key sKey = new Key(jumpKey.getRow());
          Range fake = new Range(sKey, true, endKey, false);
          ts.iter.seek(fake, columnFamilies, inclusive);
          ts.setNew();
          advanceToMatch(ts);
          ts.setNew();
        } else {
          // need to check uid
          String myUid = getUID(ts.topKey);
          String jumpUid = getUID(jumpKey);
          
          if (log.isDebugEnabled()) {
            if (myUid == null) {
              log.debug("myUid is null");
            } else {
              log.debug("myUid: " + myUid);
            }
            
            if (jumpUid == null) {
              log.debug("jumpUid is null");
            } else {
              log.debug("jumpUid: " + jumpUid);
            }
          }
          
          int ucomp = myUid.compareTo(jumpUid);
          if (ucomp < 0) {
            // need to move forward
            // create range and seek it.
            Text row = ts.topKey.getRow();
            Text cf = ts.topKey.getColumnFamily();
            String cq = ts.topKey.getColumnQualifier().toString().replaceAll(myUid, jumpUid);
            Text cq_text = new Text(cq);
            Key sKey = new Key(row, cf, cq_text);
            Key eKey = null;
            if (parentEndRow != null) {
              eKey = new Key(parentEndRow);
            }
            Range fake = new Range(sKey, true, eKey, false);
            if (log.isDebugEnabled()) {
              log.debug("uid jump, new ts.iter.seek range: " + fake);
            }
            ts.iter.seek(fake, columnFamilies, inclusive);
            ts.setNew();
            advanceToMatch(ts);
            ts.setNew();
            
            if (log.isDebugEnabled()) {
              if (ts.iter.hasTop()) {
                log.debug("ts.iter.topkey: " + ts.iter.getTopKey());
              } else {
                log.debug("ts.iter.topKey is null");
              }
            }
          }// else do nothing, we're ahead of jump key
        }
      }
      
      // ts should have moved, validate this particular ts.
      if (ts.hasTop()) {
        if (overallRange != null) {
          if (overallRange.contains(topKey)) {
            // if (topKey.getRow().compareTo(parentEndRow) < 0) {
            sorted.add(ts);
          }
        } else {
          sorted.add(ts);
        }
      }
    }
    // now get the top key from all TermSources.
    currentTerm = sorted.poll();
    if (log.isDebugEnabled()) {
      log.debug("OI.jump currentTerm = " + currentTerm);
    }
    
    topKey = buildTopKey(currentTerm);
    if (log.isDebugEnabled()) {
      log.debug("OI.jump new topKey >>" + ((topKey == null) ? "false" : topKey) + "<< ");
    }
    return hasTop();
  }
  
  private void printTopKeysForTermSources() {
    if (log.isDebugEnabled()) {
      for (TermSource ts : sources) {
        if (ts != null) {
          if (ts.topKey == null) {
            log.debug(ts.toString() + " topKey is null");
          } else {
            log.debug(ts.toString() + " topKey: " + ts.topKey);
          }
        } else {
          log.debug("ts is null");
        }
      }
      
      if (topKey != null) {
        log.debug("OrIterator current topKey: " + topKey);
      } else {
        log.debug("OrIterator current topKey is null");
      }
    }
  }
}
