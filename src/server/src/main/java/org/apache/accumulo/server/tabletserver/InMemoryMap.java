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
package org.apache.accumulo.server.tabletserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.map.MapFileOperations;
import org.apache.accumulo.core.file.map.MyMapFile;
import org.apache.accumulo.core.file.map.MySequenceFile;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.system.InterruptibleIterator;
import org.apache.accumulo.core.iterators.system.SourceSwitchingIterator;
import org.apache.accumulo.core.iterators.system.SourceSwitchingIterator.DataSource;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.trace.TraceFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

@SuppressWarnings("deprecation")
class MemKeyComparator implements Comparator<Key> {
  
  @Override
  public int compare(Key k1, Key k2) {
    int cmp = k1.compareTo(k2);
    
    if (cmp == 0) {
      if (k1 instanceof MemKey)
        if (k2 instanceof MemKey)
          cmp = ((MemKey) k2).mutationCount - ((MemKey) k1).mutationCount;
        else
          cmp = 1;
      else if (k2 instanceof MemKey)
        cmp = -1;
    }
    
    return cmp;
  }
}

class PartialMutationSkippingIterator extends SkippingIterator implements InterruptibleIterator {
  
  int maxMutationCount;
  
  public PartialMutationSkippingIterator(SortedKeyValueIterator<Key,Value> source, int maxMutationCount) {
    setSource(source);
    this.maxMutationCount = maxMutationCount;
  }
  
  @Override
  protected void consume() throws IOException {
    while (getSource().hasTop() && ((MemKey) getSource().getTopKey()).mutationCount > maxMutationCount)
      getSource().next();
  }
  
  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new PartialMutationSkippingIterator(getSource().deepCopy(env), maxMutationCount);
  }
  
  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    ((InterruptibleIterator) getSource()).setInterruptFlag(flag);
  }
  
}

public class InMemoryMap {
  MutationLog mutationLog;
  
  private SimpleMap map = null;
  
  private static final Logger log = Logger.getLogger(InMemoryMap.class);
  
  private volatile String memDumpFile = null;
  private final String memDumpDir;
  
  public InMemoryMap(boolean useNativeMap, String memDumpDir) {
    this.memDumpDir = memDumpDir;
    if (useNativeMap && NativeMap.loadedNativeLibraries()) {
      try {
        map = new NativeMapWrapper();
      } catch (Throwable t) {
        log.error("Failed to create native map", t);
      }
    }
    
    if (map == null) {
      map = new DefaultMap();
    }
  }
  
  public InMemoryMap() {
    this(ServerConfiguration.getSystemConfiguration().getBoolean(Property.TSERV_NATIVEMAP_ENABLED), ServerConfiguration.getSystemConfiguration().get(
        Property.TSERV_MEMDUMP_DIR));
  }
  
  private interface SimpleMap {
    public Value get(Key key);
    
    public Iterator<Entry<Key,Value>> iterator(Key startKey);
    
    public int size();
    
    public InterruptibleIterator skvIterator();
    
    public void delete();
    
    public long getMemoryUsed();
    
    public void mutate(List<Mutation> mutations, int mutationCount);
  }
  
  private static class DefaultMap implements SimpleMap {
    private ConcurrentSkipListMap<Key,Value> map = new ConcurrentSkipListMap<Key,Value>(new MemKeyComparator());
    private AtomicLong bytesInMemory = new AtomicLong();
    private AtomicInteger size = new AtomicInteger();
    
    public void put(Key key, Value value) {
      bytesInMemory.addAndGet(key.getLength());
      bytesInMemory.addAndGet(value.getSize());
      if (map.put(key, value) == null)
        size.incrementAndGet();
    }
    
    public Value get(Key key) {
      return map.get(key);
    }
    
    public Iterator<Entry<Key,Value>> iterator(Key startKey) {
      Key lk = new Key(startKey);
      SortedMap<Key,Value> tm = map.tailMap(lk);
      return tm.entrySet().iterator();
    }
    
    public int size() {
      return size.get();
    }
    
    public synchronized InterruptibleIterator skvIterator() {
      if (map == null)
        throw new IllegalStateException();
      
      return new SortedMapIterator(map);
    }
    
    public synchronized void delete() {
      map = null;
    }
    
    public long getOverheadPerEntry() {
      // all of the java objects that are used to hold the
      // data and make it searchable have overhead... this
      // overhead is estimated using test.EstimateInMemMapOverhead
      // and is in bytes.. the estimates were obtained by running
      // java 6_16 in 64 bit server mode
      
      return 270;
    }
    
    @Override
    public void mutate(List<Mutation> mutations, int mutationCount) {
      for (Mutation m : mutations) {
        for (ColumnUpdate cvp : m.getUpdates()) {
          Key newKey = new MemKey(m.getRow(), cvp.getColumnFamily(), cvp.getColumnQualifier(), cvp.getColumnVisibility(), cvp.getTimestamp(), cvp.isDeleted(),
              false, mutationCount);
          Value value = new Value(cvp.getValue());
          put(newKey, value);
        }
        mutationCount++;
      }
    }
    
    @Override
    public long getMemoryUsed() {
      return bytesInMemory.get() + (size() * getOverheadPerEntry());
    }
  }
  
  private static class NativeMapWrapper implements SimpleMap {
    private NativeMap nativeMap;
    
    NativeMapWrapper() {
      nativeMap = new NativeMap();
    }
    
    public Value get(Key key) {
      return nativeMap.get(key);
    }
    
    public Iterator<Entry<Key,Value>> iterator(Key startKey) {
      return nativeMap.iterator(startKey);
    }
    
    public int size() {
      return nativeMap.size();
    }
    
    public InterruptibleIterator skvIterator() {
      return (InterruptibleIterator) nativeMap.skvIterator();
    }
    
    public void delete() {
      nativeMap.delete();
    }
    
    public long getMemoryUsed() {
      return nativeMap.getMemoryUsed();
    }
    
    @Override
    public void mutate(List<Mutation> mutations, int mutationCount) {
      nativeMap.mutate(mutations, mutationCount);
    }
  }
  
  private AtomicInteger nextMutationCount = new AtomicInteger(1);
  private AtomicInteger mutationCount = new AtomicInteger(0);
  
  /**
   * Applies changes to a row in the InMemoryMap
   * 
   */
  public void mutate(List<Mutation> mutations) {
    int mc = nextMutationCount.getAndAdd(mutations.size());
    try {
      map.mutate(mutations, mc);
    } finally {
      synchronized (this) {
        // Can not update mutationCount while writes that started before
        // are in progress, this would cause partial mutations to be seen.
        // Also, can not continue until mutation count is updated, because
        // a read may not see a successful write. Therefore writes must
        // wait for writes that started before to finish.
        
        while (mutationCount.get() != mc - 1) {
          try {
            wait();
          } catch (InterruptedException ex) {
            // ignored
          }
        }
        mutationCount.set(mc + mutations.size() - 1);
        notifyAll();
      }
    }
  }
  
  /**
   * Returns a long representing the size of the InMemoryMap
   * 
   * @return bytesInMemory
   */
  public synchronized long estimatedSizeInBytes() {
    if (map == null)
      return 0;
    
    return map.getMemoryUsed();
  }
  
  Iterator<Map.Entry<Key,Value>> iterator(Key startKey) {
    return map.iterator(startKey);
  }
  
  public long getNumEntries() {
    return map.size();
  }
  
  private Set<MemoryIterator> activeIters = Collections.synchronizedSet(new HashSet<MemoryIterator>());
  
  class MemoryDataSource implements DataSource {
    
    boolean switched = false;
    private InterruptibleIterator iter;
    private List<FileSKVIterator> readers;
    
    MemoryDataSource() {
      this(new ArrayList<FileSKVIterator>());
    }
    
    public MemoryDataSource(List<FileSKVIterator> readers) {
      this.readers = readers;
    }
    
    @Override
    public boolean isCurrent() {
      if (switched)
        return true;
      else
        return memDumpFile == null;
    }
    
    @Override
    public DataSource getNewDataSource() {
      if (switched)
        throw new IllegalStateException();
      
      if (!isCurrent()) {
        switched = true;
        iter = null;
      }
      
      return this;
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> iterator() throws IOException {
      if (iter == null)
        if (!switched)
          iter = map.skvIterator();
        else {
          
          Configuration conf = CachedConfiguration.getInstance();
          FileSystem fs = TraceFileSystem.wrap(FileSystem.getLocal(conf));
          
          @SuppressWarnings("deprecation")
          FileSKVIterator reader = new MapFileOperations.RangeIterator(new MyMapFile.Reader(fs, memDumpFile, conf));
          
          readers.add(reader);
          
          iter = reader;
        }
      
      return iter;
    }
    
    @Override
    public DataSource getDeepCopyDataSource(IteratorEnvironment env) {
      return new MemoryDataSource(readers);
    }
    
  }
  
  class MemoryIterator extends WrappingIterator implements InterruptibleIterator {
    
    private AtomicBoolean closed;
    private SourceSwitchingIterator ssi;
    private MemoryDataSource mds;
    
    protected SortedKeyValueIterator<Key,Value> getSource() {
      if (closed.get())
        throw new IllegalStateException("Memory iterator is closed");
      return super.getSource();
    }
    
    private MemoryIterator(InterruptibleIterator source) {
      this(source, new AtomicBoolean(false));
    }
    
    private MemoryIterator(SortedKeyValueIterator<Key,Value> source, AtomicBoolean closed) {
      setSource(source);
      this.closed = closed;
    }
    
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      return new MemoryIterator(getSource().deepCopy(env), closed);
    }
    
    public void close() {
      
      synchronized (this) {
        if (closed.compareAndSet(false, true)) {
          
          for (FileSKVIterator reader : mds.readers)
            try {
              reader.close();
            } catch (IOException e) {
              log.warn(e, e);
            }
        }
      }
      
      // remove outside of sync to avoid deadlock
      activeIters.remove(this);
    }
    
    private synchronized boolean switchNow() throws IOException {
      if (closed.get())
        return false;
      
      ssi.switchNow();
      return true;
    }
    
    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      ((InterruptibleIterator) getSource()).setInterruptFlag(flag);
    }
    
    private void setSSI(SourceSwitchingIterator ssi) {
      this.ssi = ssi;
    }
    
    public void setMDS(MemoryDataSource mds) {
      this.mds = mds;
    }
    
  }
  
  public synchronized MemoryIterator skvIterator() {
    if (map == null)
      throw new NullPointerException();
    
    if (deleted)
      throw new IllegalStateException("Can not obtain iterator after map deleted");
    
    int mc = mutationCount.get();
    MemoryDataSource mds = new MemoryDataSource();
    SourceSwitchingIterator ssi = new SourceSwitchingIterator(new MemoryDataSource());
    MemoryIterator mi = new MemoryIterator(new ColumnFamilySkippingIterator(new PartialMutationSkippingIterator(ssi, mc)));
    mi.setSSI(ssi);
    mi.setMDS(mds);
    activeIters.add(mi);
    return mi;
  }
  
  public SortedKeyValueIterator<Key,Value> compactionIterator() {
    
    if (nextMutationCount.get() - 1 != mutationCount.get())
      throw new IllegalStateException("Memory map in unexpected state : nextMutationCount = " + nextMutationCount.get() + " mutationCount = "
          + mutationCount.get());
    
    return new ColumnFamilySkippingIterator(map.skvIterator());
  }
  
  private boolean deleted = false;
  
  @SuppressWarnings("deprecation")
  public void delete(long waitTime) {
    
    synchronized (this) {
      if (deleted)
        throw new IllegalStateException("Double delete");
      
      deleted = true;
    }
    
    long t1 = System.currentTimeMillis();
    
    while (activeIters.size() > 0 && System.currentTimeMillis() - t1 < waitTime) {
      UtilWaitThread.sleep(50);
    }
    
    if (activeIters.size() > 0) {
      // dump memmap exactly as is to a tmp file on disk, and switch scans to that temp file
      try {
        Configuration conf = CachedConfiguration.getInstance();
        FileSystem fs = TraceFileSystem.wrap(FileSystem.getLocal(conf));
        
        String tmpFile = memDumpDir + "/memDump" + UUID.randomUUID() + ".map";
        
        Configuration newConf = new Configuration(conf);
        newConf.setInt("io.seqfile.compress.blocksize", 100000);
        
        MyMapFile.Writer out = new MyMapFile.Writer(newConf, fs, tmpFile, MemKey.class, Value.class, MySequenceFile.CompressionType.BLOCK);
        InterruptibleIterator iter = map.skvIterator();
        iter.seek(new Range(), LocalityGroupUtil.EMPTY_CF_SET, false);
        
        while (iter.hasTop() && activeIters.size() > 0) {
          out.append(iter.getTopKey(), iter.getTopValue());
          iter.next();
        }
        
        out.close();
        
        log.debug("Created mem dump file " + tmpFile);
        
        memDumpFile = tmpFile;
        
        synchronized (activeIters) {
          for (MemoryIterator mi : activeIters) {
            mi.switchNow();
          }
        }
        
        // rely on unix behavior that file will be deleted when last
        // reader closes it
        fs.delete(new Path(memDumpFile), true);
        
      } catch (IOException ioe) {
        log.error("Failed to create mem dump file ", ioe);
        
        while (activeIters.size() > 0) {
          UtilWaitThread.sleep(100);
        }
      }
      
    }
    
    SimpleMap tmpMap = map;
    
    synchronized (this) {
      map = null;
    }
    
    tmpMap.delete();
  }
}
