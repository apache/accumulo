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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

// change the authority iterator to use ImmutableBytesWritable for the value and create a wrapper from iterators that use DeletableImmutableBytesWritable
public class ScanCache implements SortedKeyValueIterator<Key,Value> {
  
  public ScanCache deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException("cloning ScanCache not yet supported");
  }
  
  /*
   * The ValueWrapper object lets us keep track of where contiguous sections of cache start and end entryPoint is true only if there is the possibility of an
   * entry just before the associated key that is not in the cache exitPoint is true only if there is the possibility of an entry just after the associated key
   * that is not in the cache
   */
  private static class ValueWrapper {
    ValueWrapper(Value value, long lastRead) {
      this.value = value;
      entryPoint = false;
      exitPoint = false;
      this.lastRead = lastRead;
    }
    
    public String toString() {
      return "Entry Point? " + entryPoint + " exitPoint? " + exitPoint + " value is " + value + " last read at " + lastRead;
    }
    
    boolean entryPoint;
    boolean exitPoint;
    long lastRead;
    Value value;
  }
  
  // this is the main cache object
  private SortedMap<Key,ValueWrapper> cacheEntries = new TreeMap<Key,ValueWrapper>();
  // this is an iterator over the main cache object that keeps track of the location during a scan
  private Iterator<Entry<Key,ValueWrapper>> cacheIterator = null;
  // this is an iterator over some authority for key value pairs that keeps track of the authority location during a scan
  private SortedKeyValueIterator<Key,Value> authorityIter = null;
  // firstCacheEntry holds the top element from the cache map
  private Entry<Key,ValueWrapper> topCacheEntry = null;
  // topKey and topValue hold the current top key value pair that the ScanCache iterator is on
  private Key topKey = null;
  private Value topValue = null;
  // topValid is used to indicate that we have reached the end of the scan
  private boolean topValid = false;
  // topIsFromCache lets us know that we can search the cache first for the next entry if firstCacheEntry.getValue().exitPoint is false
  // it also lets us know if we need to do a seek next time we use the authorityIter
  private boolean topIsFromCache = false;
  
  // keep track of when entries were added for quick removal
  // ASSUMPTION: no two queries will come in at the same millisecond
  // obviate this assumption by using an entry point object that sorts by both the time and the Key
  private SortedMap<Long,Key> timesOfEntryPoints = new TreeMap<Long,Key>();
  private Key latestEntryPoint = null;
  private long currentScanTime = 0;
  
  // keep track of new entries found during the scan, as well as entries that are no longer entry/exit points
  private ArrayList<Key> noLongerEntryPoints = new ArrayList<Key>();
  private ArrayList<Key> noLongerExitPoints = new ArrayList<Key>();
  private ArrayList<Long> noLongerEntryPointTimes = new ArrayList<Long>();
  private SortedMap<Key,ValueWrapper> newEntries = new TreeMap<Key,ValueWrapper>();
  // currentScanSize keeps track of the memory usage of the objects in newEntries
  private int currentScanSize = 0;
  // currentScanOversize keeps track of whether the size of the data read from disk in the current scan is more than the scan cache size
  // if currentScanOversize is true, then newEntries should be empty and no new data should be added to newEntries
  // also, finishScan should not remove or add exit and entry points
  private boolean currentScanOversize = false;
  
  // these fields are used to make sure the cache does not grow too big
  private long currentSize = 0;
  private long maxSize;
  
  // keep track of the range of the scan, so that we do not go past the end of the current tablet
  // this could be useful when the range of the authority scanner does not match the maximum range of the scan we wish to perform
  private KeyExtent extent;
  
  // the logger
  private static final Logger log = Logger.getLogger(ScanCache.class);
  
  // create a scan cache by specifying the range it covers and the maximum amount of memory it can take
  // must call setAuthorityIterator() and seek() before reading key value pairs
  public ScanCache(long maxSize, KeyExtent extent) {
    this.maxSize = maxSize;
    this.extent = extent;
  }
  
  // this is a method of the SortedKeyValueIterator interface
  // behavior is to return the current top key only if hasTop is true
  // behavior is unspecified otherwise
  // this method does not change the scan cache
  public Key getTopKey() {
    // log.debug("returning topKey " + topKey + " from scanCache if topisFromCache? " + topIsFromCache);
    return topKey;
  }
  
  // this is a method of the SortedKeyValueIterator interface
  // behavior is to return the current top value only if hasTop is true
  // behavior is unspecified otherwise
  // this method does not change the scan cache
  public Value getTopValue() {
    return topValue;
  }
  
  // this is a method of the SortedKeyValueIterator interface
  // returns true only when topKey and topValue are primed and are in the range of the given key extent
  // returns false after the iterator runs out of key value pairs within the range of extent
  public boolean hasTop() {
    return topValid && extent.contains(topKey.getRow());
  }
  
  // this is a method of the SortedKeyValueIterator interface
  // set up for the getTopKey() and getTopValue() functions
  // set hasTop() to false if there are no more key value pairs to get
  public void next() throws IOException {
    topValid = false;
    // check to see if the last thing we found was in the cache
    if (topIsFromCache) {
      // we're in the cache
      // check if we're moving past the end of a contiguous section
      // assert(outOfEntries == topCacheEntry.getValue().exitPoint);
      if (topCacheEntry.getValue().exitPoint == true) {
        // log.debug("Adding " + topKey + " to noLongerExitPoints");
        if (currentScanOversize == false)
          noLongerExitPoints.add(topKey);
        // get the next cache entry
        if (cacheIterator.hasNext() == false) {
          topCacheEntry = null;
        } else {
          topCacheEntry = cacheIterator.next();
        }
        checkForTopFromAuthority(topKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME));
      } else {
        // the next entry is in the cache if it exists
        
        // we're in a contiguous section, so not having a next entry in the cache means that there's none in the authoritative section
        if (cacheIterator.hasNext() == false) {
          // there isn't a next entry
          topKey = null;
          topValue = null;
          return;
        }
        // there is a next entry
        topCacheEntry = cacheIterator.next();
        // just read the next key from the cache
        topValid = true;
        // outOfEntries = topCacheEntry.getValue().exitPoint;
        topKey = topCacheEntry.getKey();
        topValue = topCacheEntry.getValue().value;
        topCacheEntry.getValue().lastRead = currentScanTime;
      }
    } else {
      // continue to read the next key from the authority iterator
      checkForTopFromAuthority(null);
    }
  }
  
  // set the iterator that is the authority for this data
  // this should be called once before calling the first seek
  // use the given iterator as an authority for all keys
  public void setAuthorityIterator(SortedKeyValueIterator<Key,Value> authorityIter) {
    this.authorityIter = authorityIter;
  }
  
  // private boolean locked = false;
  
  // this is a method of the SortedKeyValueIterator interface
  // reset the iterator and jump to the key k, or just after if k is not in the map
  // k should not be null
  @Override
  public/* synchronized */void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) {
    // this code broken when switching from seek(Key) to seek(Range)... so this compiles, but it's nothing that works
    // this code further broken when switching from seek(Range) to seek(Range, columnFam)
    Key k = range.getStartKey();
    
    currentScanTime = System.currentTimeMillis();
    topValid = false;
    currentScanSize = 0;
    currentScanOversize = false;
    newEntries.clear();
    noLongerEntryPoints.clear();
    noLongerEntryPointTimes.clear();
    noLongerExitPoints.clear();
    
    // grab the first entry from the cache that is greater than or equal to the given key k
    cacheIterator = cacheEntries.tailMap(k).entrySet().iterator();
    if (cacheIterator.hasNext())
      topCacheEntry = cacheIterator.next();
    else
      topCacheEntry = null;
    // if we're skipping the given key, we should be past it at this point
    
    // if there is something in the cache then we should try to use it
    if (topCacheEntry != null) {
      // check to see if the cache is valid for the start of the range
      if (topCacheEntry.getKey().compareTo(k, PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME) == 0) {
        // we're not skipping the given key, and the given key is in the cache
        // we have a cache hit
        topIsFromCache = true;
        topValid = true;
        topKey = topCacheEntry.getKey();
        topValue = topCacheEntry.getValue().value;
        // outOfEntries = topCacheEntry.getValue().exitPoint;
        topCacheEntry.getValue().lastRead = currentScanTime;
      } else {
        // we're not skipping the given key, and the given key is not in the cache
        // we have a cache hit if the first cache entry is not an entry point
        
        // the given key is not in the cache, and the cache contains something after it
        assert (k.compareTo(topCacheEntry.getKey()) < 0);
        if (topCacheEntry.getValue().entryPoint) {
          // check the authority iterator for the first key
          checkForTopFromAuthority(k);
        } else {
          topIsFromCache = true;
          topValid = true;
          topKey = topCacheEntry.getKey();
          topValue = topCacheEntry.getValue().value;
          // outOfEntries = topCacheEntry.getValue().exitPoint;
          topCacheEntry.getValue().lastRead = currentScanTime;
        }
      }
      // we're skipping the key, so we always need to make sure that the first entry is not an entry point if it is to be valid
    } else {
      // there is nothing in the cache for this scan, so just go to the authority
      checkForTopFromAuthority(k);
    }
    
    // mark the entry point for LRU purposes
    if (topValid) {
      latestEntryPoint = topKey;
    }
  }
  
  boolean seekingBeforeExtent;
  
  // look to see if the next key can be found outside the cache
  private void checkForTopFromAuthority(Key seekAfter) {
    // we should always either seek in the authority or call next if we're in this function
    if (seekAfter != null) {
      // log.debug("seekKey in checkForTopFromAuthority is " + seekAfter);
      // ASSUMPTION: the table does not contain the row key ""
      seekingBeforeExtent = ((extent.getPrevEndRow() == null && seekAfter.getRow().equals(new Text(""))) || (extent.getPrevEndRow() != null && extent
          .getPrevEndRow().compareTo(seekAfter.getRow()) >= 0));
      // jump to the correct point in the authority iterator
      try {
        // this code broken when switching from seek(Key) to seek(Range)
        // authorityIter.seek(seekAfter);
        authorityIter.seek(null, null, false);
      } catch (IOException e) {
        log.error("Exception when seeking in authorityIter", e);
        topValid = false;
        return;
      }
    } else {
      seekingBeforeExtent = false;
      
      // we should have picked the last k-v pair from the authority, and it should not have been invalid
      assert (topIsFromCache == false && authorityIter.hasTop());
      
      // get the next record from the authority iterator
      try {
        // increment the iterator
        authorityIter.next();
      } catch (IOException e) {
        log.error("Exception when calling next in authorityIter", e);
        topValid = false;
        return;
      }
    }
    
    if (topCacheEntry == null) {
      // we have nothing more in the cache, so we must go to the authority
      if (authorityIter.hasTop()) {
        grabTopFromAuthority();
      } else {
        topValid = false;
      }
      
    } else {
      // sanity check
      if (!authorityIter.hasTop()) {
        // this is a strange state, since there is nothing left in the authority, but the cache has something
        // we should never see this state
        log.debug("Assertion, DANGER" + false);
        assert (false);
      }
      // compare the top authoritative key with the one in the cache
      if (topCacheEntry.getKey().compareTo(authorityIter.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS) == 0) {
        // we're back in the cache
        // log.debug("Back in the cache at "+topCacheEntry.getKey());
        
        // we should have only gotten here if there was a possibility of a non-cached k-v pair before the one we found
        // log.debug("Assertion true? " + topCacheEntry.getValue().entryPoint);
        // log.debug("Scan cache dump " + cacheEntries.toString());
        assert (topCacheEntry.getValue().entryPoint == true);
        
        // mark the fact that this is no longer an entry point if we have a new cache entry before this entry or we started scanning from the beginning
        // of the
        // tablet
        if (latestEntryPoint != null && topCacheEntry.getValue().entryPoint && currentScanOversize == false) {
          noLongerEntryPoints.add(topCacheEntry.getKey());
          noLongerEntryPointTimes.add(topCacheEntry.getValue().lastRead);
        }
        
        // set the top to be the current cache entry
        topValid = true;
        topIsFromCache = true;
        topKey = topCacheEntry.getKey();
        topValue = topCacheEntry.getValue().value;
        topCacheEntry.getValue().lastRead = currentScanTime;
        // outOfEntries = topCacheEntry.getValue().exitPoint;
      } else {
        // use the one from the authority
        grabTopFromAuthority();
      }
    }
  }
  
  private void grabTopFromAuthority() {
    // log.debug("Grabbing top from authority");
    topValid = true;
    boolean firstKey = false;
    if (topKey == null) {
      firstKey = true;
    }
    topIsFromCache = false;
    if (currentScanOversize == true) {
      topKey = authorityIter.getTopKey();
      topValue = authorityIter.getTopValue();
    } else {
      topKey = new Key(authorityIter.getTopKey());
      topValue = new Value(authorityIter.getTopValue());
      ValueWrapper vw = new ValueWrapper(topValue, currentScanTime);
      // if this is the first key in the scan and we're going to the authority, then it will be an entry point in the cache unless we started scanning
      // before
      // the extent of the tablet
      if (firstKey == true && !seekingBeforeExtent) {
        vw.entryPoint = true;
        timesOfEntryPoints.put(currentScanTime, topKey);
      }
      addToTempSize(topKey, vw);
      // add this pair to the cache
      newEntries.put(topKey, vw);
    }
  }
  
  // write all the entries scanned from the other iterator to this cache
  // redo the contiguous region markers
  public void finishScan() {
    if (currentScanOversize == false) {
      // add all of the new entries to the cache object
      cacheEntries.putAll(newEntries);
      currentSize += currentScanSize;
      currentScanSize = 0;
      // log.debug("cache dump after putAll " + cacheEntries.toString());
      
      // we should be removing each entry point from the list of times, so the size of the no longer entry point set
      // should be the same as the size of the no longer entry point times set
      assert (noLongerEntryPoints.size() == noLongerEntryPointTimes.size());
      
      // go through the list of entries that are no longer entry/exit points and make it so
      for (Key k : noLongerEntryPoints) {
        cacheEntries.get(k).entryPoint = false;
      }
      // go through the list of new entries and add them to the cache
      for (Key k : noLongerExitPoints) {
        cacheEntries.get(k).exitPoint = false;
      }
      // remove old entry point times
      for (Long l : noLongerEntryPointTimes) {
        timesOfEntryPoints.remove(l);
      }
      // if we got the last record from the authority and we didn't run out of records, then it will be an exit point in the cache
      if (topIsFromCache == false && topValid == true) {
        newEntries.get(topKey).exitPoint = true;
      }
      
      // update the time of the entry point
      if (latestEntryPoint != null) {
        // only add the true entry points to the timesOfEntryPoints map
        if (cacheEntries.get(latestEntryPoint).entryPoint)
          timesOfEntryPoints.put(currentScanTime, latestEntryPoint);
      }
      
      // free some space if necessary
      if (currentSize > maxSize) {
        makeRoom();
      }
    }
    
    newEntries.clear();
    noLongerEntryPoints.clear();
    noLongerEntryPointTimes.clear();
    noLongerExitPoints.clear();
    currentScanSize = 0;
    currentScanOversize = false;
    topKey = null;
    topValue = null;
    authorityIter = null;
    topValid = false;
    latestEntryPoint = null;
  }
  
  Entry<Long,Key> makeRoomSecondTimeEntryPoint = null;
  Iterator<Entry<Key,ValueWrapper>> makeRoomCacheIter = null;
  
  void grabNextEntryPoint() {
    Iterator<Entry<Long,Key>> entryTimeIter = timesOfEntryPoints.entrySet().iterator();
    assert (entryTimeIter.hasNext() || cacheEntries.get(cacheEntries.firstKey()).entryPoint == false);
    if (!entryTimeIter.hasNext()) {
      makeRoomSecondTimeEntryPoint = null;
      makeRoomCacheIter = cacheEntries.entrySet().iterator();
    } else {
      Entry<Long,Key> firstTimeEntryPoint = entryTimeIter.next();
      entryTimeIter.remove();
      makeRoomSecondTimeEntryPoint = null;
      if (entryTimeIter.hasNext())
        makeRoomSecondTimeEntryPoint = entryTimeIter.next();
      
      makeRoomCacheIter = cacheEntries.tailMap(firstTimeEntryPoint.getValue()).entrySet().iterator();
    }
  }
  
  // delete the least recently used contiguous sections
  // prefer to remove very small segments
  // prefer to remove portions from one end of a large segment
  private void makeRoom() {
    if (currentSize <= maxSize)
      return;
    
    // only remove from the beginning of a contiguous section to preserve contiguous sections
    // support removing the tail of a contiguous section as well to prevent bad cases with backwards sequential reads
    // get the earliest entry point
    grabNextEntryPoint();
    // delete stuff until we are under the max size
    while (currentSize > maxSize) {
      if (!makeRoomCacheIter.hasNext()) {
        // reset from the next entry point
        grabNextEntryPoint();
        continue;
      }
      
      Entry<Key,ValueWrapper> currentCacheEntry = makeRoomCacheIter.next();
      
      // stop deleting when we get to a record that was read more recently than the next oldest entry point
      if (makeRoomSecondTimeEntryPoint != null && currentCacheEntry.getValue().lastRead >= makeRoomSecondTimeEntryPoint.getKey()) {
        // we're finished with the contiguous cache section, so set up for removing from the next section
        // set the current cache entry to be an entry point
        if (currentCacheEntry.getValue().entryPoint == false) {
          currentCacheEntry.getValue().entryPoint = true;
          timesOfEntryPoints.put(currentCacheEntry.getValue().lastRead, currentCacheEntry.getKey());
        }
        
        grabNextEntryPoint();
      } else {
        // remove this entry
        reduceSize(currentCacheEntry.getKey(), currentCacheEntry.getValue());
        makeRoomCacheIter.remove();
      }
    }
    
    // mark the new entry point
    if (makeRoomCacheIter.hasNext()) {
      Entry<Key,ValueWrapper> currentCacheEntry = makeRoomCacheIter.next();
      if (currentCacheEntry.getValue().entryPoint == false) {
        currentCacheEntry.getValue().entryPoint = true;
        timesOfEntryPoints.put(currentCacheEntry.getValue().lastRead, currentCacheEntry.getKey());
      }
    }
    
    makeRoomCacheIter = null;
    makeRoomSecondTimeEntryPoint = null;
  }
  
  private void addToTempSize(Key k, ValueWrapper v) {
    currentScanSize += k.getSize();
    currentScanSize += v.value.getSize();
    if (currentScanSize > maxSize) {
      currentScanOversize = true;
      currentScanSize = 0;
      newEntries.clear();
      noLongerEntryPoints.clear();
      noLongerEntryPointTimes.clear();
      noLongerExitPoints.clear();
    }
  }
  
  // calculate the space required for the given key value pair and remove it from the current size
  private void reduceSize(Key k, ValueWrapper v) {
    currentSize -= k.getSize();
    currentSize -= v.value.getSize();
  }
  
  // remove all keys that match the given key
  // mark the keys on either side of this key as being end points
  public void invalidate(Key key) {
    // Setting timestamp to max length so key sorts BEFORE all others with same colname
    Key timeKey = new Key(key);
    timeKey.setTimestamp(Long.MAX_VALUE);
    // mark the key before key as being an exit point
    SortedMap<Key,ValueWrapper> headMap = cacheEntries.headMap(timeKey);
    if (!headMap.isEmpty()) {
      headMap.get(headMap.lastKey()).exitPoint = true;
    }
    // remove all versions of key and mark the next key as being an entry point
    SortedMap<Key,ValueWrapper> tailMap = cacheEntries.tailMap(timeKey);
    Iterator<Entry<Key,ValueWrapper>> tailIter = tailMap.entrySet().iterator();
    while (tailIter.hasNext()) {
      Entry<Key,ValueWrapper> currentEntry = tailIter.next();
      // compare the key without looking at the timestamp
      if (currentEntry.getKey().compareTo(timeKey, PartialKey.ROW_COLFAM_COLQUAL_COLVIS) == 0) {
        // this entry has a key equal to the given key, so it is no longer valid
        // remove it from the cache
        reduceSize(currentEntry.getKey(), currentEntry.getValue());
        tailIter.remove();
        // try marking the next entry as an entry point;
        continue;
      }
      // this is the first entry after the given key
      // mark this entry as an entry point
      if (currentEntry.getValue().entryPoint == false) {
        currentEntry.getValue().entryPoint = true;
        timesOfEntryPoints.put(currentEntry.getValue().lastRead, currentEntry.getKey());
      }
      break;
    }
  }
  
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
    
  }
}
