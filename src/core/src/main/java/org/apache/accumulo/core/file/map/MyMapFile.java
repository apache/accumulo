/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.core.file.map;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.NoSuchMetaStoreException;
import org.apache.accumulo.core.file.map.MySequenceFile.CompressionType;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.InterruptibleIterator;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Stat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableName;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

/**
 * A file-based map from keys to values.
 * 
 * <p>
 * A map is a directory containing two files, the <code>data</code> file, containing all keys and values in the map, and a smaller <code>index</code> file,
 * containing a fraction of the keys. The fraction is determined by {@link Writer#getIndexInterval()}.
 * 
 * <p>
 * The index file is read entirely into memory. Thus key implementations should try to keep themselves small.
 * 
 * <p>
 * Map files are created by adding entries in-order. To maintain a large database, perform updates by copying the previous version of a database and merging in
 * a sorted change list, to create a new version of the database in a new file. Sorting large change lists can be done with {@link MySequenceFile.Sorter}.
 * 
 * 
 * @deprecated since 1.4
 * @use org.apache.accumulo.core.file.rfile.RFile
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class MyMapFile {
  
  /**
   * @deprecated since 1.4
   * @use org.apache.accumulo.core.Constants.MAPFILE_EXTENSION
   */
  public static final String EXTENSION = "map";
  
  private static final Logger log = Logger.getLogger(MyMapFile.class);
  
  /** The name of the index file. */
  public static final String INDEX_FILE_NAME = "index";
  
  /** The name of the data file. */
  public static final String DATA_FILE_NAME = "data";
  
  public static final Stat mapFileSeekTimeStat = new Stat();
  
  public static final Stat mapFileSeekScans = new Stat();
  
  public static final Stat mapFileSeekScanTime = new Stat();
  
  public static final Stat mapFileSeekScanCompareTime = new Stat();
  
  protected MyMapFile() {} // no public ctor
  
  /** Writes a new map. */
  public static class Writer {
    private MySequenceFile.Writer data;
    private MySequenceFile.Writer index;
    
    final private static String INDEX_INTERVAL = "io.map.index.interval";
    private int indexInterval = 128;
    
    private long size;
    private LongWritable position = new LongWritable();
    
    // the following fields are used only for checking key order
    private WritableComparator comparator;
    private DataInputBuffer inBuf = new DataInputBuffer();
    private DataOutputBuffer outBuf = new DataOutputBuffer();
    private WritableComparable lastKey;
    
    /** Create the named map for keys of the named class. */
    public Writer(Configuration conf, FileSystem fs, String dirName, Class keyClass, Class valClass) throws IOException {
      this(conf, fs, dirName, WritableComparator.get(keyClass), valClass, MySequenceFile.getCompressionType(conf));
    }
    
    /** Create the named map for keys of the named class. */
    public Writer(Configuration conf, FileSystem fs, String dirName, Class keyClass, Class valClass, CompressionType compress, Progressable progress)
        throws IOException {
      this(conf, fs, dirName, WritableComparator.get(keyClass), valClass, compress, progress);
    }
    
    /** Create the named map for keys of the named class. */
    public Writer(Configuration conf, FileSystem fs, String dirName, Class keyClass, Class valClass, CompressionType compress, CompressionCodec codec,
        Progressable progress) throws IOException {
      this(conf, fs, dirName, WritableComparator.get(keyClass), valClass, compress, codec, progress);
    }
    
    /** Create the named map for keys of the named class. */
    public Writer(Configuration conf, FileSystem fs, String dirName, Class keyClass, Class valClass, CompressionType compressionType) throws IOException {
      this(conf, fs, dirName, WritableComparator.get(keyClass), valClass, compressionType);
    }
    
    /** Create the named map using the named key comparator. */
    public Writer(Configuration conf, FileSystem fs, String dirName, WritableComparator comparator, Class valClass) throws IOException {
      this(conf, fs, dirName, comparator, valClass, MySequenceFile.getCompressionType(conf));
    }
    
    /** Create the named map using the named key comparator. */
    public Writer(Configuration conf, FileSystem fs, String dirName, WritableComparator comparator, Class valClass, MySequenceFile.CompressionType compress)
        throws IOException {
      this(conf, fs, dirName, comparator, valClass, compress, null);
    }
    
    /** Create the named map using the named key comparator. */
    public Writer(Configuration conf, FileSystem fs, String dirName, WritableComparator comparator, Class valClass, MySequenceFile.CompressionType compress,
        Progressable progress) throws IOException {
      this(conf, fs, dirName, comparator, valClass, compress, new DefaultCodec(), progress);
    }
    
    /** Create the named map using the named key comparator. */
    public Writer(Configuration conf, FileSystem fs, String dirName, WritableComparator comparator, Class valClass, MySequenceFile.CompressionType compress,
        CompressionCodec codec, Progressable progress) throws IOException {
      
      // LOG.debug("Opening map file "+dirName+" for write");
      
      this.indexInterval = conf.getInt(INDEX_INTERVAL, this.indexInterval);
      
      this.comparator = comparator;
      this.lastKey = comparator.newKey();
      
      Path dir = new Path(dirName);
      if (!fs.mkdirs(dir)) {
        throw new IOException("Mkdirs failed to create directory " + dir.toString());
      }
      Path dataFile = new Path(dir, DATA_FILE_NAME);
      Path indexFile = new Path(dir, INDEX_FILE_NAME);
      
      Class keyClass = comparator.getKeyClass();
      this.data = MySequenceFile.createWriter(fs, conf, dataFile, keyClass, valClass, compress, codec, progress);
      this.index = MySequenceFile.createWriter(fs, conf, indexFile, keyClass, LongWritable.class, CompressionType.BLOCK, progress);
    }
    
    /** The number of entries that are added before an index entry is added. */
    public int getIndexInterval() {
      return indexInterval;
    }
    
    /**
     * Sets the index interval.
     * 
     * @see #getIndexInterval()
     */
    public void setIndexInterval(int interval) {
      indexInterval = interval;
    }
    
    /**
     * Sets the index interval and stores it in conf
     * 
     * @see #getIndexInterval()
     */
    public static void setIndexInterval(Configuration conf, int interval) {
      conf.setInt(INDEX_INTERVAL, interval);
    }
    
    /** Close the map. */
    public synchronized void close() throws IOException {
      
      // LOG.debug("Closing map file "+myDir+" for write");
      
      data.close();
      index.close();
    }
    
    /**
     * Append a key/value pair to the map. The key must be greater or equal to the previous key added to the map.
     */
    public synchronized void append(WritableComparable key, Writable val) throws IOException {
      
      checkKey(key);
      
      /*******************************************************************
       * Instead of storing index values for every 128th key that all point back to the same compressed block, we can store one key for each compressed block.
       */
      if (data.isBlockCompressed()) {
        // add an index entry when the data size changes, indicating a
        // new compressed block is added
        // also add an index entry for the first value
        if (size == 0 || position.get() != data.getLength()) {
          position.set(data.getLength());
          index.append(key, position);
        }
      } else {
        if (size % indexInterval == 0) { // add an index entry
          position.set(data.getLength()); // point to current eof
          index.append(key, position);
        }
      }
      
      data.append(key, val); // append key/value to data
      size++;
    }
    
    private void checkKey(WritableComparable key) throws IOException {
      // check that keys are well-ordered
      if (size != 0 && comparator.compare(lastKey, key) > 0)
        throw new IOException("key out of order: " + key + " after " + lastKey);
      
      // update lastKey with a copy of key by writing and reading
      outBuf.reset();
      key.write(outBuf); // write new key
      
      inBuf.reset(outBuf.getData(), outBuf.getLength());
      lastKey.readFields(inBuf); // read into lastKey
    }
    
  }
  
  /** Provide access to an existing map. */
  public static class Reader implements FileSKVIterator {
    
    static {
      WritableName.setName(Key.class, Constants.OLD_PACKAGE_NAME + ".core.data.Key");
      WritableName.setName(Value.class, Constants.OLD_PACKAGE_NAME + ".core.data.Value");
    }
    
    /**
     * Number of index entries to skip between each entry. Zero by default. Setting this to values larger than zero can facilitate opening large map files using
     * less memory.
     */
    private int INDEX_SKIP = 0;
    
    private WritableComparator comparator;
    
    private WritableComparable nextKey;
    private long seekPosition = -1;
    private int seekIndex = -1;
    private long firstPosition;
    
    // the data, on disk
    private MySequenceFile.Reader data;
    private MySequenceFile.Reader index;
    
    // whether the index Reader was closed
    private boolean indexClosed = false;
    
    // the index, in memory
    private MapFileIndex indexInfo;
    
    /** Returns the class of keys in this file. */
    public Class getKeyClass() {
      return data.getKeyClass();
    }
    
    /** Returns the class of values in this file. */
    public Class getValueClass() {
      return data.getValueClass();
    }
    
    /** Construct a map reader for the named map. */
    public Reader(FileSystem fs, String dirName, Configuration conf) throws IOException {
      this(fs, dirName, null, conf);
      INDEX_SKIP = conf.getInt("io.map.index.skip", 0);
    }
    
    /** Construct a map reader for the named map using the named comparator. */
    public Reader(FileSystem fs, String dirName, WritableComparator comparator, Configuration conf) throws IOException {
      this(fs, dirName, comparator, conf, true);
    }
    
    // keep track of the dir and the conf so that we can clone
    String dirName;
    FileSystem fs;
    Configuration conf;
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      try {
        SortedKeyValueIterator<Key,Value> other = env.reserveMapFileReader(dirName);
        ((InterruptibleIterator) other).setInterruptFlag(interruptFlag);
        log.debug("deep copying MyMapFile: " + this + " -> " + other);
        return other;
      } catch (IOException e) {
        log.error("failed to clone map file reader", e);
        throw new RuntimeException(e);
      }
    }
    
    @Override
    public void closeDeepCopies() throws IOException {
      // nothing to do, deep copies are externally managed/closed
    }
    
    /**
     * Hook to allow subclasses to defer opening streams until further initialization is complete.
     * 
     * @see #createDataFileReader(FileSystem, Path, Configuration)
     */
    protected Reader(FileSystem fs, String dirName, WritableComparator comparator, Configuration conf, boolean open) throws IOException {
      
      this.dirName = dirName;
      this.fs = fs;
      this.conf = conf;
      
      if (open) {
        open(fs, dirName, comparator, conf);
      }
    }
    
    protected synchronized void open(FileSystem fs, String dirName, WritableComparator comparator, Configuration conf) throws IOException {
      
      // LOG.debug("Opening map file "+dirName+" for read");
      
      Path dir = new Path(dirName);
      Path dataFile = new Path(dir, DATA_FILE_NAME);
      Path indexFile = new Path(dir, INDEX_FILE_NAME);
      
      // open the data
      this.data = createDataFileReader(fs, dataFile, conf);
      this.firstPosition = data.getPosition();
      
      if (comparator == null)
        this.comparator = WritableComparator.get(data.getKeyClass());
      else
        this.comparator = comparator;
      
      this.myIndexKey = new MapFileIndexKey(dirName + "/" + INDEX_FILE_NAME, fs);
      
      // open the index
      indexInfo = indexCache.get(myIndexKey);
      if (indexInfo == null)
        this.index = new MySequenceFile.Reader(fs, indexFile, conf);
      else
        this.index = null;
    }
    
    /**
     * Override this method to specialize the type of {@link MySequenceFile.Reader} returned.
     */
    protected MySequenceFile.Reader createDataFileReader(FileSystem fs, Path dataFile, Configuration conf) throws IOException {
      return new MySequenceFile.Reader(fs, dataFile, conf);
    }
    
    private static class MapFileIndexKey {
      private String filename;
      private Long hashInfo;
      
      public MapFileIndexKey(String filename, FileSystem fs) throws IOException {
        this.filename = filename;
        Path p = new Path(filename);
        FileStatus fstatus = fs.getFileStatus(p);
        hashInfo = fstatus.getModificationTime() ^ (fstatus.getLen() << 32);
      }
      
      @Override
      public int hashCode() {
        return filename.hashCode() + hashInfo.hashCode();
      }
      
      @Override
      public boolean equals(Object other) {
        return other instanceof MapFileIndexKey && ((MapFileIndexKey) other).filename.equals(filename) && ((MapFileIndexKey) other).hashInfo.equals(hashInfo);
      }
    }
    
    private static class MapFileIndex {
      int count;
      WritableComparable[] keys;
      long[] positions;
      Key firstKey;
      WeakReference<MapFileIndexKey> myKey;
    }
    
    private MapFileIndexKey myIndexKey;
    private static WeakHashMap<MapFileIndexKey,MapFileIndex> indexCache = new WeakHashMap<MapFileIndexKey,MapFileIndex>();
    
    private void readIndex() throws IOException {
      if (indexInfo != null) {
        return;
      }
      // try to find a cached index
      indexInfo = indexCache.get(myIndexKey);
      if (indexInfo != null) {
        MapFileIndexKey temp = indexInfo.myKey.get();
        if (temp != null)
          myIndexKey = temp;
        return;
      }
      
      indexInfo = new MapFileIndex();
      indexInfo.myKey = new WeakReference(myIndexKey);
      
      // read the index entirely into memory
      indexInfo.count = 0;
      indexInfo.keys = new WritableComparable[1024];
      indexInfo.positions = new long[1024];
      try {
        int skip = INDEX_SKIP;
        LongWritable position = new LongWritable();
        WritableComparable lastKey = null;
        while (true) {
          WritableComparable k = comparator.newKey();
          
          if (!index.next(k, position))
            break;
          
          // check order to make sure comparator is compatible
          if (lastKey != null && comparator.compare(lastKey, k) > 0)
            throw new IOException("key out of order: " + k + " after " + lastKey);
          lastKey = k;
          
          if (skip > 0) {
            skip--;
            continue; // skip this entry
          }
          skip = INDEX_SKIP; // reset skip
          
          if (indexInfo.count == indexInfo.keys.length) { // time to grow arrays
            int newLength = (indexInfo.keys.length * 3) / 2;
            WritableComparable[] newKeys = new WritableComparable[newLength];
            long[] newPositions = new long[newLength];
            System.arraycopy(indexInfo.keys, 0, newKeys, 0, indexInfo.count);
            System.arraycopy(indexInfo.positions, 0, newPositions, 0, indexInfo.count);
            indexInfo.keys = newKeys;
            indexInfo.positions = newPositions;
          }
          
          indexInfo.keys[indexInfo.count] = k;
          if (indexInfo.count == 0) {
            try {
              indexInfo.firstKey = (Key) k;
            } catch (ClassCastException e) {
              // we don't need to respond -- firstKey is only a performance enhancement
            }
          }
          indexInfo.positions[indexInfo.count] = position.get();
          indexInfo.count++;
        }
        indexCache.put(myIndexKey, indexInfo);
      } catch (EOFException e) {
        log.warn("Unexpected EOF reading " + index + " at entry #" + indexInfo.count + ".  Ignoring.");
      } finally {
        indexClosed = true;
        index.close();
      }
    }
    
    /** Re-positions the reader before its first key. */
    public synchronized void reset() throws IOException {
      data.seek(firstPosition);
    }
    
    /**
     * Get the key at approximately the middle of the file.
     * 
     */
    public synchronized WritableComparable midKey() throws IOException {
      
      readIndex();
      int pos = ((indexInfo.count - 1) / 2); // middle of the index
      if (pos < 0) {
        throw new IOException("MapFile empty");
      }
      
      return indexInfo.keys[pos];
    }
    
    /**
     * Reads the final key from the file.
     * 
     * @param key
     *          key to read into
     */
    public synchronized void finalKey(WritableComparable key) throws IOException {
      
      long originalPosition = data.getPosition(); // save position
      try {
        readIndex(); // make sure index is valid
        if (indexInfo.count > 0) {
          data.seek(indexInfo.positions[indexInfo.count - 1]); // skip to last indexed
          // entry
        } else {
          reset(); // start at the beginning
        }
        while (data.next(key)) {} // scan to eof
        
      } finally {
        data.seek(originalPosition); // restore position
      }
    }
    
    /**
     * Positions the reader at the named key, or if none such exists, at the first entry after the named key. Returns true iff the named key exists in this map.
     */
    public synchronized boolean seek(WritableComparable key) throws IOException {
      return seekInternal(key, null) == 0;
    }
    
    public synchronized boolean seek(WritableComparable key, WritableComparable lastKey) throws IOException {
      return seekInternal(key, lastKey) == 0;
    }
    
    /**
     * Positions the reader at the named key, or if none such exists, at the first entry after the named key.
     * 
     * @return 0 - exact match found < 0 - positioned at next record 1 - no more records in file
     */
    private synchronized int seekInternal(WritableComparable key, WritableComparable lastKey) throws IOException {
      return seekInternal(key, false, lastKey);
    }
    
    /**
     * Positions the reader at the named key, or if none such exists, at the key that falls just before or just after dependent on how the <code>before</code>
     * parameter is set.
     * 
     * @param before
     *          - IF true, and <code>key</code> does not exist, position file at entry that falls just before <code>key</code>. Otherwise, position file at
     *          record that sorts just after.
     * @return 0 - exact match found < 0 - positioned at next record 1 - no more records in file
     */
    private synchronized int seekInternal(WritableComparable key, final boolean before, WritableComparable lastKey) throws IOException {
      if (before) {
        throw new IllegalArgumentException("seeking before not supported!");
      }
      readIndex(); // make sure index is read
      
      int seekLocation = 0;
      
      if (seekIndex != -1 // seeked before
          && ((seekIndex + 1 < indexInfo.count && comparator.compare(key, indexInfo.keys[seekIndex + 1]) < 0) || seekIndex + 1 == indexInfo.count) // before
          // next
          // indexed
          && comparator.compare(key, nextKey) >= 0) { // but after
        // last seeked
        if (lastKey != null) {
          int comp = comparator.compare(lastKey, key);
          if (comp >= 0) {
            // need to rewind
            // log.debug("Seeking backwards!!!!!!");
            seekLocation = 1;
            data.seek(seekPosition);
          } else {
            // log.debug("Avoiding seek!!!!");
          }
        } else {
          seekLocation = 2;
          data.seek(seekPosition);
        }
        // do nothing
      } else {
        seekIndex = binarySearch(key);
        if (seekIndex < 0) // decode insertion point
          seekIndex = -seekIndex - 2;
        
        if (seekIndex == -1) // belongs before first entry
          seekPosition = firstPosition; // use beginning of file
        else
          seekPosition = indexInfo.positions[seekIndex]; // else use index
        seekLocation = 3;
        data.seek(seekPosition);
      }
      
      if (nextKey == null)
        nextKey = comparator.newKey();
      
      long startTime = System.currentTimeMillis();
      
      int scans = 0;
      long mark = startTime;
      while (data.next(nextKey)) {
        {
          long temp = System.currentTimeMillis();
          MyMapFile.mapFileSeekScanTime.addStat(temp - mark);
          mark = temp;
        }
        scans++;
        int c = comparator.compare(key, nextKey);
        {
          long temp = System.currentTimeMillis();
          MyMapFile.mapFileSeekScanCompareTime.addStat(temp - mark);
          mark = temp;
        }
        if (c <= 0) { // at or beyond desired
          MyMapFile.mapFileSeekScans.addStat(scans);
          long timeSpent = System.currentTimeMillis() - startTime;
          if (timeSpent > 200)
            log.debug("long seek time: " + timeSpent + " seek location: " + seekLocation + " seek scans: " + scans + " seeking key " + key + " from " + lastKey
                + " previous " + previousKey);
          MyMapFile.mapFileSeekTimeStat.addStat(timeSpent);
          return c;
        }
      }
      long timeSpent = System.currentTimeMillis() - startTime;
      if (timeSpent > 100)
        log.debug("long seek time (2): " + timeSpent + " seek location: " + seekLocation + " seek scans: " + scans + " seeking key " + key + " from " + lastKey
            + " previous " + previousKey);
      MyMapFile.mapFileSeekTimeStat.addStat(timeSpent);
      MyMapFile.mapFileSeekScans.addStat(scans);
      return 1;
    }
    
    private int binarySearch(WritableComparable key) {
      int low = 0;
      int high = indexInfo.count - 1;
      
      while (low <= high) {
        int mid = (low + high) >>> 1;
        WritableComparable midVal = indexInfo.keys[mid];
        int cmp = comparator.compare(midVal, key);
        
        if (cmp < 0)
          low = mid + 1;
        else if (cmp > 0)
          high = mid - 1;
        else
          return mid; // key found
      }
      return -(low + 1); // key not found.
    }
    
    public synchronized int getIndexPosition(WritableComparable key) throws IOException {
      readIndex();
      return binarySearch(key);
    }
    
    public synchronized void printIndex() throws IOException {
      readIndex();
      for (int i = 0; i < indexInfo.count; i++) {
        System.out.println(i + " " + indexInfo.keys[i]);
      }
    }
    
    /**
     * Read the next key/value pair in the map into <code>key</code> and <code>val</code>. Returns true if such a pair exists and false when at the end of the
     * map
     */
    public synchronized boolean next(WritableComparable key, Writable val) throws IOException {
      return data.next(key, val);
    }
    
    /** Return the value for the named key, or null if none exists. */
    public synchronized Writable get(WritableComparable key, Writable val) throws IOException {
      if (seek(key)) {
        data.getCurrentValue(val);
        return val;
      }
      return null;
    }
    
    /**
     * Finds the record that is the closest match to the specified key. Returns <code>key</code> or if it does not exist, at the first entry after the named
     * key. - *
     * 
     * @param key
     *          - key that we're trying to find - *
     * @param val
     *          - data value if key is found - *
     * @return - the key that was the closest match or null if eof.
     */
    public synchronized WritableComparable getClosest(WritableComparable key, Writable val) throws IOException {
      return getClosest(key, val, false);
    }
    
    /**
     * Finds the record that is the closest match to the specified key.
     * 
     * @param key
     *          - key that we're trying to find
     * @param val
     *          - data value if key is found
     * @param before
     *          - IF true, and <code>key</code> does not exist, return the first entry that falls just before the <code>key</code>. Otherwise, return the record
     *          that sorts just after.
     * @return - the key that was the closest match or null if eof.
     */
    public synchronized WritableComparable getClosest(WritableComparable key, Writable val, final boolean before) throws IOException {
      int c = seekInternal(key, before, null);
      
      // If we didn't get an exact match, and we ended up in the wrong
      // direction relative to the query key, return null since we
      // must be at the beginning or end of the file.
      if ((!before && c > 0) || (before && c < 0)) {
        return null;
      }
      
      data.getCurrentValue(val);
      return nextKey;
    }
    
    public synchronized WritableComparable getClosest(WritableComparable key, Writable val, final boolean before, WritableComparable lastKey)
        throws IOException {
      
      int c = seekInternal(key, before, lastKey);
      
      // If we didn't get an exact match, and we ended up in the wrong
      // direction relative to the query key, return null since we
      // must be at the beginning or end of the file.
      if ((!before && c > 0) || (before && c < 0)) {
        return null;
      }
      
      data.getCurrentValue(val);
      return nextKey;
    }
    
    /** Close the map. */
    public synchronized void close() throws IOException {
      // LOG.debug("Closing map file "+myDir+" for read");
      
      if (!indexClosed && index != null) {
        index.close();
      }
      data.close();
    }
    
    private Key previousKey = null;
    private Key topKey = null;
    private Value topValue = null;
    // private Key firstKey = null;
    private Key lastKey = null;
    
    private AtomicBoolean interruptFlag;
    private int interruptCheckCount = 0;
    
    @Override
    public Key getTopKey() {
      return topKey;
    }
    
    @Override
    public Value getTopValue() {
      if (topKey != null)
        return topValue;
      return null;
    }
    
    @Override
    public boolean hasTop() {
      return topKey != null;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void next() throws IOException {
      if (interruptFlag != null && interruptCheckCount++ % 100 == 0 && interruptFlag.get())
        throw new IterationInterruptedException();
      
      Key temp = previousKey;
      previousKey = topKey;
      topKey = temp;
      if (topKey == null)
        topKey = (Key) comparator.newKey();
      if (topValue == null)
        topValue = new Value();
      if (!next(topKey, topValue)) {
        topKey = null;
        // try to learn the last key
        if (previousKey != null && lastKey == null) {
          lastKey = new Key(previousKey);
        } else if (previousKey != null && lastKey != null && comparator.compare(previousKey, lastKey) < 0) {
          lastKey = new Key(previousKey);
        }
      }
    }
    
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
      
      if (columnFamilies.size() != 0 || inclusive) {
        throw new IllegalArgumentException("I do not know how to filter column families");
      }
      
      if (range == null)
        throw new IllegalArgumentException("Cannot seek to null range");
      
      if (interruptFlag != null && interruptFlag.get())
        throw new IterationInterruptedException();
      
      Key key = range.getStartKey();
      if (key == null) {
        key = new Key();
      }
      
      // check boundaries
      if (topKey != null && indexInfo.firstKey != null && previousKey == null && comparator.compare(key, indexInfo.firstKey) <= 0
          && comparator.compare(topKey, indexInfo.firstKey) == 0) {
        // seeking to the beginning of the file and we're already there
        // so do nothing
      } else if (topKey != null && previousKey != null && comparator.compare(previousKey, key) < 0 && comparator.compare(key, topKey) <= 0) {
        // seeking to the current location
        // so do nothing
      } else if (topKey == null && lastKey != null && comparator.compare(key, lastKey) > 0) {
        // seeking past the end of the file and we're already there
        // so do nothing
      } else {
        // seek in the map file
        previousKey = null;
        if (topValue == null)
          topValue = new Value();
        topKey = (Key) getClosest(key, topValue, false, topKey);
        if (topKey == null) {
          // we seeked past the end of the map file
          if (lastKey == null) {
            // key is an upper bound on last key (we can get more precise later)
            lastKey = new Key(key);
          } else if (comparator.compare(lastKey, key) > 0) {
            // key is a better upper bound for last key
            lastKey = new Key(key);
          }
        }
      }
      
      while (hasTop() && range.beforeStartKey(getTopKey())) {
        next();
      }
    }
    
    @Override
    public synchronized Key getFirstKey() throws IOException {
      readIndex();
      if (indexInfo.keys.length == 0)
        return null;
      return (Key) indexInfo.keys[0];
    }
    
    @Override
    public Key getLastKey() throws IOException {
      Key k = new Key();
      finalKey(k);
      return k;
    }
    
    @Override
    public DataInputStream getMetaStore(String name) throws IOException {
      Path path = new Path(this.dirName, name);
      if (!fs.exists(path))
        throw new NoSuchMetaStoreException("name = " + name);
      return fs.open(path);
    }
    
    public void dropIndex() {
      indexInfo = null;
    }
    
    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      this.interruptFlag = flag;
    }
  }
  
  /** Renames an existing map directory. */
  public static void rename(FileSystem fs, String oldName, String newName) throws IOException {
    Path oldDir = new Path(oldName);
    Path newDir = new Path(newName);
    if (!fs.rename(oldDir, newDir)) {
      throw new IOException("Could not rename " + oldDir + " to " + newDir);
    }
  }
  
  /** Deletes the named map file. */
  public static void delete(FileSystem fs, String name) throws IOException {
    Path dir = new Path(name);
    Path data = new Path(dir, DATA_FILE_NAME);
    Path index = new Path(dir, INDEX_FILE_NAME);
    
    fs.delete(data, true);
    fs.delete(index, true);
    fs.delete(dir, true);
  }
  
  /** Checks a new configuration for the index interval to use, and caches the result */
  private static Integer indexInterval = null;
  
  public static int getIndexInterval() {
    if (indexInterval == null) {
      Configuration conf = CachedConfiguration.getInstance();
      indexInterval = conf.getInt("io.map.index.interval", 128);
    }
    return indexInterval;
  }
  
  /**
   * This method attempts to fix a corrupt MapFile by re-creating its index.
   * 
   * Code copied from hadoop (0.18.0) because it was broken there. This is a fixed version. Do not want to loose changes if MyMapFile is updated.
   * 
   * @param fs
   *          filesystem
   * @param dir
   *          directory containing the MapFile data and index
   * @param keyClass
   *          key class (has to be a subclass of Writable)
   * @param valueClass
   *          value class (has to be a subclass of Writable)
   * @param dryrun
   *          do not perform any changes, just report what needs to be done
   * @return number of valid entries in this MapFile, or -1 if no fixing was needed
   */
  public static long fix(FileSystem fs, Path dir, Class<? extends WritableComparable> keyClass, Class<? extends Writable> valueClass, boolean dryrun,
      Configuration conf) throws Exception {
    String dr = (dryrun ? "[DRY RUN ] " : "");
    Path data = new Path(dir, DATA_FILE_NAME);
    Path index = new Path(dir, INDEX_FILE_NAME);
    int indexInterval = getIndexInterval();
    if (!fs.exists(data)) {
      // there's nothing we can do to fix this!
      throw new Exception(dr + "Missing data file in " + dir + ", impossible to fix this.");
    }
    if (fs.exists(index)) {
      // no fixing needed
      return -1;
    }
    MySequenceFile.Reader dataReader = null;
    MySequenceFile.Writer indexWriter = null;
    long cnt = 0L;
    try {
      dataReader = new MySequenceFile.Reader(fs, data, conf);
      if (!dataReader.getKeyClass().equals(keyClass)) {
        throw new Exception(dr + "Wrong key class in " + dir + ", expected" + keyClass.getName() + ", got " + dataReader.getKeyClass().getName());
      }
      if (!dataReader.getValueClass().equals(valueClass)) {
        throw new Exception(dr + "Wrong value class in " + dir + ", expected" + valueClass.getName() + ", got " + dataReader.getValueClass().getName());
      }
      Writable key = ReflectionUtils.newInstance(keyClass, conf);
      Writable value = ReflectionUtils.newInstance(valueClass, conf);
      if (!dryrun)
        indexWriter = MySequenceFile.createWriter(fs, conf, index, keyClass, LongWritable.class);
      long currentPos = 0L;
      long lastPos = 0L;
      
      LongWritable position = new LongWritable();
      lastPos = dataReader.getPosition();
      
      boolean blockCompressed = dataReader.isBlockCompressed();
      
      if (!blockCompressed) {
        currentPos = lastPos;
      }
      
      while (dataReader.next(key, value)) {
        if (blockCompressed) {
          if (cnt == 0) {
            currentPos = dataReader.getPosition();
          } else {
            long pos = dataReader.getPosition();
            if (pos != currentPos) {
              lastPos = currentPos;
              currentPos = pos;
            }
          }
          // write an index entry at position 0 and whenever the position changes
          if (cnt == 0 || position.get() != lastPos) {
            position.set(lastPos);
            if (!dryrun)
              indexWriter.append(key, position);
          }
        } else {
          if (cnt % indexInterval == 0) {
            position.set(currentPos);
            if (!dryrun)
              indexWriter.append(key, position);
          }
          long pos = dataReader.getPosition();
          if (pos != currentPos) {
            lastPos = currentPos;
            currentPos = pos;
          }
        }
        cnt++;
        
      }
    } catch (Throwable t) {
      // truncated data file. swallow it.
      log.error("Exception when trying to fix map file " + dir, t);
    } finally {
      if (dataReader != null)
        dataReader.close();
      if (indexWriter != null)
        indexWriter.close();
    }
    return cnt;
  }
  
  public static void main(String[] args) throws Exception {
    String usage = "Usage: MapFile inFile outFile";
    
    if (args.length != 2) {
      System.err.println(usage);
      System.exit(1);
    }
    
    String in = args[0];
    String out = args[1];
    
    Configuration conf = CachedConfiguration.getInstance();
    FileSystem fs = FileSystem.getLocal(conf);
    MyMapFile.Reader reader = new MyMapFile.Reader(fs, in, conf);
    MyMapFile.Writer writer = new MyMapFile.Writer(conf, fs, out, reader.getKeyClass(), reader.getValueClass());
    
    WritableComparable key = (WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
    Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
    
    while (reader.next(key, value))
      // copy all entries
      writer.append(key, value);
    
    writer.close();
  }
  
}
