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
package org.apache.accumulo.core.file.rfile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.RandomAccess;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileCFSkippingIterator;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.NoSuchMetaStoreException;
import org.apache.accumulo.core.file.blockfile.ABlockReader;
import org.apache.accumulo.core.file.blockfile.ABlockWriter;
import org.apache.accumulo.core.file.blockfile.BlockFileReader;
import org.apache.accumulo.core.file.blockfile.BlockFileWriter;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.RelativeKey.MByteSequence;
import org.apache.accumulo.core.file.rfile.bcfile.MetaBlockDoesNotExist;
import org.apache.accumulo.core.iterators.HeapIterator;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

public class RFile {
  
  public static final String EXTENSION = "rf";
  
  private static final Logger log = Logger.getLogger(RFile.class);
  
  private RFile() {}
  
  private static final int RINDEX_MAGIC = 0x20637474;
  private static final int RINDEX_VER = 4;
  
  public static class IndexEntry implements WritableComparable<IndexEntry> {
    private Key key;
    private int entries;
    
    IndexEntry(Key k, int e) {
      this.key = k;
      this.entries = e;
    }
    
    public IndexEntry() {}
    
    @Override
    public void readFields(DataInput in) throws IOException {
      key = new Key();
      key.readFields(in);
      entries = in.readInt();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      key.write(out);
      out.writeInt(entries);
    }
    
    public Key getKey() {
      return key;
    }
    
    public int getNumEntries() {
      return entries;
    }
    
    @Override
    public int compareTo(IndexEntry o) {
      return key.compareTo(o.key);
    }
  }
  
  private static class Count {
    public Count(int i) {
      this.count = i;
    }
    
    public Count(long count) {
      this.count = count;
    }
    
    long count;
  }
  
  // a list that deserializes index entries on demand
  private static class SerializedIndex extends AbstractList<IndexEntry> implements List<IndexEntry>, RandomAccess {
    
    private int[] offsets;
    private byte[] data;
    
    SerializedIndex(int[] offsets, byte[] data) {
      this.offsets = offsets;
      this.data = data;
    }
    
    @Override
    public IndexEntry get(int index) {
      int len;
      if (index == offsets.length - 1)
        len = data.length - offsets[index];
      else
        len = offsets[index + 1] - offsets[index];
      
      ByteArrayInputStream bais = new ByteArrayInputStream(data, offsets[index], len);
      DataInputStream dis = new DataInputStream(bais);
      
      IndexEntry ie = new IndexEntry();
      try {
        ie.readFields(dis);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      
      return ie;
    }
    
    @Override
    public int size() {
      return offsets.length;
    }
    
    public long sizeInBytes() {
      return data.length + 4 * offsets.length;
    }
    
  }
  
  private static class LocalityGroupMetadata implements Writable {
    
    private int startBlock;
    private Key firstKey;
    private Map<ByteSequence,Count> columnFamilies;
    private List<IndexEntry> index;
    
    private ByteArrayOutputStream indexBytes;
    private DataOutputStream indexOut;
    private ArrayList<Integer> offsets;
    
    private boolean isDefaultLG = false;
    private String name;
    private Set<ByteSequence> previousColumnFamilies;
    private int version;
    
    public LocalityGroupMetadata(int version) {
      columnFamilies = new HashMap<ByteSequence,Count>();
      this.version = version;
    }
    
    public LocalityGroupMetadata(int nextBlock, Set<ByteSequence> pcf) {
      this.startBlock = nextBlock;
      isDefaultLG = true;
      columnFamilies = new HashMap<ByteSequence,Count>();
      previousColumnFamilies = pcf;
      
      indexBytes = new ByteArrayOutputStream();
      indexOut = new DataOutputStream(indexBytes);
      offsets = new ArrayList<Integer>();
    }
    
    public LocalityGroupMetadata(String name, Set<ByteSequence> cfset, int nextBlock) {
      this.startBlock = nextBlock;
      this.name = name;
      isDefaultLG = false;
      columnFamilies = new HashMap<ByteSequence,Count>();
      for (ByteSequence cf : cfset) {
        columnFamilies.put(cf, new Count(0));
      }
      
      indexBytes = new ByteArrayOutputStream();
      indexOut = new DataOutputStream(indexBytes);
      offsets = new ArrayList<Integer>();
    }
    
    private Key getFirstKey() {
      return firstKey;
    }
    
    private void setFirstKey(Key key) {
      if (firstKey != null)
        throw new IllegalStateException();
      this.firstKey = new Key(key);
    }
    
    public void updateColumnCount(Key key) {
      
      if (isDefaultLG && columnFamilies == null) {
        if (previousColumnFamilies.size() > 0) {
          // only do this check when there are previous column families
          ByteSequence cf = key.getColumnFamilyData();
          if (previousColumnFamilies.contains(cf)) {
            throw new IllegalArgumentException("Added column family \"" + cf + "\" to default locality group that was in previous locality group");
          }
        }
        
        // no longer keeping track of column families, so return
        return;
      }
      
      ByteSequence cf = key.getColumnFamilyData();
      Count count = columnFamilies.get(cf);
      
      if (count == null) {
        if (!isDefaultLG) {
          throw new IllegalArgumentException("invalid column family : " + cf);
        }
        
        if (previousColumnFamilies.contains(cf)) {
          throw new IllegalArgumentException("Added column family \"" + cf + "\" to default locality group that was in previous locality group");
        }
        
        if (columnFamilies.size() > Writer.MAX_CF_IN_DLG) {
          // stop keeping track, there are too many
          columnFamilies = null;
          return;
        }
        count = new Count(0);
        columnFamilies.put(new ArrayByteSequence(cf.getBackingArray(), cf.offset(), cf.length()), count);
        
      }
      
      count.count++;
      
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      
      isDefaultLG = in.readBoolean();
      if (!isDefaultLG) {
        name = in.readUTF();
      }
      
      startBlock = in.readInt();
      
      int size = in.readInt();
      
      if (size == -1) {
        if (!isDefaultLG)
          throw new IllegalStateException("Non default LG " + name + " does not have column families");
        
        columnFamilies = null;
      } else {
        if (columnFamilies == null)
          columnFamilies = new HashMap<ByteSequence,Count>();
        else
          columnFamilies.clear();
        
        for (int i = 0; i < size; i++) {
          int len = in.readInt();
          byte cf[] = new byte[len];
          in.readFully(cf);
          long count = in.readLong();
          
          columnFamilies.put(new ArrayByteSequence(cf), new Count(count));
        }
      }
      
      if (in.readBoolean()) {
        firstKey = new Key();
        firstKey.readFields(in);
      } else {
        firstKey = null;
      }
      
      if (version == 3) {
        index = new ArrayList<IndexEntry>();
        size = in.readInt();
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ArrayList<Integer> oal = new ArrayList<Integer>();
        
        for (int i = 0; i < size; i++) {
          IndexEntry ie = new IndexEntry();
          oal.add(dos.size());
          ie.readFields(in);
          ie.write(dos);
        }
        
        dos.close();
        
        int[] oia = new int[oal.size()];
        for (int i = 0; i < oal.size(); i++) {
          oia[i] = oal.get(i);
        }
        
        index = new SerializedIndex(oia, baos.toByteArray());
        
      } else if (version == RINDEX_VER) {
        int numIndexEntries = in.readInt();
        int offsets[] = new int[numIndexEntries];
        for (int i = 0; i < numIndexEntries; i++) {
          offsets[i] = in.readInt();
        }
        
        size = in.readInt();
        byte[] indexData = new byte[size];
        in.readFully(indexData);
        
        index = new SerializedIndex(offsets, indexData);
      } else {
        throw new RuntimeException("Unexpected version " + version);
      }
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      
      out.writeBoolean(isDefaultLG);
      if (!isDefaultLG) {
        out.writeUTF(name);
      }
      
      out.writeInt(startBlock);
      
      if (isDefaultLG && columnFamilies == null) {
        // only expect null when default LG, otherwise let a NPE occur
        out.writeInt(-1);
      } else {
        out.writeInt(columnFamilies.size());
        
        for (Entry<ByteSequence,Count> entry : columnFamilies.entrySet()) {
          out.writeInt(entry.getKey().length());
          out.write(entry.getKey().getBackingArray(), entry.getKey().offset(), entry.getKey().length());
          out.writeLong(entry.getValue().count);
        }
      }
      
      out.writeBoolean(firstKey != null);
      if (firstKey != null)
        firstKey.write(out);
      
      out.writeInt(offsets.size());
      for (Integer offset : offsets) {
        out.writeInt(offset);
      }
      
      indexOut.close();
      byte[] indexData = indexBytes.toByteArray();
      
      out.writeInt(indexData.length);
      out.write(indexData);
      
    }
    
    public void printInfo() {
      PrintStream out = System.out;
      out.println("Locality group : " + (isDefaultLG ? "<DEFAULT>" : name));
      out.println("\tStart block     : " + startBlock);
      out.println("\tNum   blocks    : " + index.size());
      out.println("\tIndex size      : " + String.format("%,d", ((SerializedIndex) index).sizeInBytes()) + " bytes");
      out.println("\tFirst key       : " + firstKey);
      
      Key lastKey = null;
      if (index != null && index.size() > 0) {
        lastKey = index.get(index.size() - 1).key;
      }
      
      out.println("\tLast key        : " + lastKey);
      
      long numKeys = 0;
      for (IndexEntry ie : index) {
        numKeys += ie.entries;
      }
      
      out.println("\tNum entries     : " + numKeys);
      out.println("\tColumn families : " + (isDefaultLG && columnFamilies == null ? "<UNKNOWN>" : columnFamilies.keySet()));
    }
    
  }
  
  public static class Writer implements FileSKVWriter {
    
    public static final int MAX_CF_IN_DLG = 1000;
    
    private BlockFileWriter fileWriter;
    private ABlockWriter blockWriter;
    
    // private BlockAppender blockAppender;
    private long blockSize = 100000;
    private int entries = 0;
    
    private ArrayList<LocalityGroupMetadata> localityGroups = new ArrayList<LocalityGroupMetadata>();
    private LocalityGroupMetadata currentLocalityGroup = null;
    private int nextBlock = 0;
    
    private Key lastKeyInBlock = null;
    
    private boolean dataClosed = false;
    private boolean closed = false;
    private Key prevKey = new Key();
    private boolean startedDefaultLocalityGroup = false;
    
    private HashSet<ByteSequence> previousColumnFamilies;
    
    public Writer(BlockFileWriter bfw, int blockSize) throws IOException {
      this.blockSize = blockSize;
      this.fileWriter = bfw;
      this.blockWriter = null;
      previousColumnFamilies = new HashSet<ByteSequence>();
    }
    
    public synchronized void close() throws IOException {
      
      if (closed) {
        return;
      }
      
      closeData();
      
      ABlockWriter mba = fileWriter.prepareMetaBlock("RFile.index");
      
      mba.writeInt(RINDEX_MAGIC);
      mba.writeInt(RINDEX_VER);
      
      if (currentLocalityGroup != null)
        localityGroups.add(currentLocalityGroup);
      
      mba.writeInt(localityGroups.size());
      
      for (LocalityGroupMetadata lc : localityGroups) {
        lc.write(mba);
      }
      
      mba.close();
      
      fileWriter.close();
      
      closed = true;
    }
    
    private void closeData() throws IOException {
      
      if (dataClosed) {
        return;
      }
      
      dataClosed = true;
      
      if (blockWriter != null) {
        closeBlock(lastKeyInBlock);
      }
    }
    
    public void append(Key key, Value value) throws IOException {
      
      if (dataClosed) {
        throw new IllegalStateException("Cannont append, data closed");
      }
      
      if (key.compareTo(prevKey) < 0) {
        throw new IOException(key + " < " + prevKey);
      }
      
      currentLocalityGroup.updateColumnCount(key);
      
      if (currentLocalityGroup.getFirstKey() == null) {
        currentLocalityGroup.setFirstKey(key);
      }
      
      if (blockWriter == null) {
        blockWriter = fileWriter.prepareDataBlock();
      }
      
      RelativeKey rk = new RelativeKey(lastKeyInBlock, key);
      
      rk.write(blockWriter);
      value.write(blockWriter);
      entries++;
      
      prevKey = new Key(key);
      lastKeyInBlock = prevKey;
      
      if (blockWriter.getRawSize() > blockSize) {
        closeBlock(prevKey);
      }
      
    }
    
    private void closeBlock(Key key) throws IOException {
      blockWriter.close();
      blockWriter = null;
      lastKeyInBlock = null;
      currentLocalityGroup.offsets.add(currentLocalityGroup.indexOut.size());
      new IndexEntry(key, entries).write(currentLocalityGroup.indexOut);
      entries = 0;
      nextBlock++;
    }
    
    @Override
    public DataOutputStream createMetaStore(String name) throws IOException {
      closeData();
      
      return (DataOutputStream) fileWriter.prepareMetaBlock(name);
    }
    
    private void _startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies) throws IOException {
      if (dataClosed) {
        throw new IllegalStateException("data closed");
      }
      
      if (startedDefaultLocalityGroup) {
        throw new IllegalStateException("Can not start anymore new locality groups after default locality group started");
      }
      
      if (blockWriter != null) {
        closeBlock(lastKeyInBlock);
      }
      
      if (currentLocalityGroup != null) {
        localityGroups.add(currentLocalityGroup);
      }
      
      if (columnFamilies == null) {
        startedDefaultLocalityGroup = true;
        currentLocalityGroup = new LocalityGroupMetadata(nextBlock, previousColumnFamilies);
      } else {
        if (!Collections.disjoint(columnFamilies, previousColumnFamilies)) {
          HashSet<ByteSequence> overlap = new HashSet<ByteSequence>(columnFamilies);
          overlap.retainAll(previousColumnFamilies);
          throw new IllegalArgumentException("Column families over lap with previous locality group : " + overlap);
        }
        currentLocalityGroup = new LocalityGroupMetadata(name, columnFamilies, nextBlock);
        previousColumnFamilies.addAll(columnFamilies);
      }
      
      prevKey = new Key();
    }
    
    @Override
    public void startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies) throws IOException {
      if (columnFamilies == null)
        throw new NullPointerException();
      
      _startNewLocalityGroup(name, columnFamilies);
    }
    
    @Override
    public void startDefaultLocalityGroup() throws IOException {
      _startNewLocalityGroup(null, null);
    }
    
    @Override
    public boolean supportsLocalityGroups() {
      return true;
    }
  }
  
  private static class LocalityGroupReader implements FileSKVIterator {
    
    private BlockFileReader reader;
    private List<IndexEntry> index;
    private int blockCount;
    private Key firstKey;
    private int startBlock;
    private Map<ByteSequence,Count> columnFamilies;
    private boolean isDefaultLocalityGroup;
    private boolean closed = false;
    
    private LocalityGroupReader(BlockFileReader reader, LocalityGroupMetadata lgm) throws IOException {
      this.firstKey = lgm.firstKey;
      this.index = lgm.index;
      this.startBlock = lgm.startBlock;
      blockCount = lgm.index.size();
      this.columnFamilies = lgm.columnFamilies;
      this.isDefaultLocalityGroup = lgm.isDefaultLG;
      
      this.reader = reader;
      
      if (index.size() > 0) {
        // check a random subset of the index to ensure it is in sorted order
        Random rand = new Random();
        Key prev = index.get(0).getKey();
        for (int i = 1; i < index.size(); i += Math.max(1, rand.nextInt(index.size() - i))) {
          Key key = index.get(i).getKey();
          
          if (prev.compareTo(key) > 0) {
            throw new IOException("Index out of order " + prev + " > " + key);
          }
          
          prev = key;
        }
        
        // ensure the first key is <= all index entries
        if (firstKey.compareTo(index.get(0).getKey()) > 0) {
          throw new IOException("First key out of order " + firstKey + " > " + index.get(0).getKey());
        }
      }
      
    }
    
    public LocalityGroupReader(LocalityGroupReader lgr) {
      this.firstKey = lgr.firstKey;
      this.index = lgr.index;
      this.startBlock = lgr.startBlock;
      this.blockCount = lgr.blockCount;
      this.columnFamilies = lgr.columnFamilies;
      this.isDefaultLocalityGroup = lgr.isDefaultLocalityGroup;
      this.reader = lgr.reader;
    }
    
    List<IndexEntry> getIndex() {
      return Collections.unmodifiableList(index);
    }
    
    @Override
    public void close() throws IOException {
      closed = true;
      hasTop = false;
      if (currBlock != null)
        currBlock.close();
      
    }
    
    private int block;
    private int entriesLeft;
    private ABlockReader currBlock;
    private RelativeKey rk;
    private Value val;
    private Key prevKey = null;
    private Range range = null;
    private boolean hasTop = false;
    private AtomicBoolean interruptFlag;
    
    @Override
    public Key getTopKey() {
      return rk.getKey();
    }
    
    @Override
    public Value getTopValue() {
      return val;
    }
    
    @Override
    public boolean hasTop() {
      return hasTop;
    }
    
    @Override
    public void next() throws IOException {
      try {
        _next();
      } catch (IOException ioe) {
        reset();
        throw ioe;
      }
    }
    
    private void _next() throws IOException {
      
      if (!hasTop)
        throw new IllegalStateException();
      
      if (entriesLeft == 0) {
        
        block++;
        currBlock.close();
        
        if (block < blockCount) {
          currBlock = getDataBlock();
          entriesLeft = index.get(block).entries;
        } else {
          rk = null;
          val = null;
          hasTop = false;
          return;
        }
      }
      
      prevKey = rk.getKey();
      rk.readFields(currBlock);
      val.readFields(currBlock);
      entriesLeft--;
      hasTop = !range.afterEndKey(rk.getKey());
    }
    
    private ABlockReader getDataBlock() throws IOException {
      if (interruptFlag != null && interruptFlag.get())
        throw new IterationInterruptedException();
      
      return reader.getDataBlock(startBlock + block);
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
      
      if (closed)
        throw new IllegalStateException("Locality group reader closed");
      
      if (columnFamilies.size() != 0 || inclusive)
        throw new IllegalArgumentException("I do not know how to filter column families");
      
      if (interruptFlag != null && interruptFlag.get())
        throw new IterationInterruptedException();
      
      try {
        _seek(range);
      } catch (IOException ioe) {
        reset();
        throw ioe;
      }
    }
    
    private void reset() {
      rk = null;
      hasTop = false;
      if (currBlock != null) {
        try {
          try {
            currBlock.close();
          } catch (IOException e) {
            log.warn("Failed to close block reader", e);
          }
        } finally {
          currBlock = null;
        }
      }
    }
    
    private void _seek(Range range) throws IOException {
      
      this.range = range;
      
      if (blockCount == 0) {
        // its an empty file
        rk = null;
        return;
      }
      
      Key startKey = range.getStartKey();
      if (startKey == null)
        startKey = new Key();
      
      boolean reseek = true;
      
      if (range.afterEndKey(firstKey)) {
        // range is before first key in rfile, so there is nothing to do
        reset();
        reseek = false;
      }
      
      if (rk != null) {
        if (range.beforeStartKey(prevKey) && range.afterEndKey(getTopKey())) {
          // range is between the two keys in the file where the last range seeked to stopped, so there is
          // nothing to do
          reseek = false;
        }
        
        if (startKey.compareTo(getTopKey()) <= 0 && startKey.compareTo(prevKey) > 0) {
          // current location in file can satisfy this request, no need to seek
          reseek = false;
        }
        
        if (startKey.compareTo(index.get(block).getKey()) <= 0 && startKey.compareTo(getTopKey()) >= 0) {
          // start key is within the unconsumed portion of the current block
          
          MByteSequence valbs = new MByteSequence(new byte[64], 0, 0);
          RelativeKey tmpRk = new RelativeKey();
          Key pKey = new Key(getTopKey());
          int fastSkipped = tmpRk.fastSkip(currBlock, startKey, valbs, pKey, getTopKey());
          if (fastSkipped > 0) {
            entriesLeft -= fastSkipped;
            val = new Value(valbs.toArray());
            prevKey = pKey;
            rk = tmpRk;
          }
          
          reseek = false;
        }
        
        if (block == 0 && getTopKey().equals(firstKey) && startKey.compareTo(firstKey) <= 0) {
          // seeking before the beginning of the file, and already positioned at the first key in the file
          // so there is nothing to do
          reseek = false;
        }
      }
      
      int fastSkipped = -1;
      
      if (reseek) {
        int pos = Collections.binarySearch(index, new IndexEntry(startKey, 0), new Comparator<IndexEntry>() {
          @Override
          public int compare(IndexEntry o1, IndexEntry o2) {
            return o1.getKey().compareTo(o2.getKey());
          }
        });
        
        if (pos >= 0) {
          block = pos;
        } else {
          block = (pos * -1) - 1;
        }
        
        reset();
        
        if (block == index.size()) {
          // past the last key
        } else {
          
          // if the index contains the same key multiple times, then go to the
          // earliest index entry containing the key
          while (block > 0 && index.get(block - 1).getKey().equals(index.get(block).getKey())) {
            block--;
          }
          
          currBlock = getDataBlock();
          entriesLeft = index.get(block).entries;
          
          val = new Value();
          
          if (block > 0)
            prevKey = new Key(index.get(block - 1).getKey()); // initially prevKey is the last key of the prev block
          else
            prevKey = new Key(); // first block in the file, so set prev key to minimal key
            
          MByteSequence valbs = new MByteSequence(new byte[64], 0, 0);
          RelativeKey tmpRk = new RelativeKey();
          fastSkipped = tmpRk.fastSkip(currBlock, startKey, valbs, prevKey, null);
          entriesLeft -= fastSkipped;
          val = new Value(valbs.toArray());
          // set rk when everything above is successful, if exception
          // occurs rk will not be set
          rk = tmpRk;
        }
      }
      
      hasTop = rk != null && !range.afterEndKey(rk.getKey());
      
      while (hasTop() && range.beforeStartKey(getTopKey())) {
        next();
      }
    }
    
    @Override
    public Key getFirstKey() throws IOException {
      return firstKey;
    }
    
    @Override
    public Key getLastKey() throws IOException {
      if (index.size() == 0)
        return null;
      return index.get(index.size() - 1).getKey();
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void closeDeepCopies() throws IOException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public DataInputStream getMetaStore(String name) throws IOException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      this.interruptFlag = flag;
    }
  }
  
  public static class Reader extends HeapIterator implements FileSKVIterator {
    
    private static final Collection<ByteSequence> EMPTY_CF_SET = Collections.emptySet();
    
    private BlockFileReader reader;
    
    private ArrayList<LocalityGroupMetadata> localityGroups = new ArrayList<LocalityGroupMetadata>();
    
    private LocalityGroupReader lgReaders[];
    private HashSet<ByteSequence> nonDefaultColumnFamilies;
    
    private List<Reader> deepCopies;
    private boolean deepCopy = false;
    
    private AtomicBoolean interruptFlag;
    
    Reader(BlockFileReader rdr) throws IOException {
      this.reader = rdr;
      
      ABlockReader mb = reader.getMetaBlock("RFile.index");
      
      int magic = mb.readInt();
      int ver = mb.readInt();
      
      if (magic != RINDEX_MAGIC)
        throw new IOException("Did not see expected magic number, saw " + magic);
      if (ver != RINDEX_VER && ver != 3)
        throw new IOException("Did not see expected version, saw " + ver);
      
      int size = mb.readInt();
      lgReaders = new LocalityGroupReader[size];
      
      deepCopies = new LinkedList<Reader>();
      
      for (int i = 0; i < size; i++) {
        LocalityGroupMetadata lgm = new LocalityGroupMetadata(ver);
        lgm.readFields(mb);
        localityGroups.add(lgm);
        
        lgReaders[i] = new LocalityGroupReader(reader, lgm);
      }
      
      mb.close();
      
      nonDefaultColumnFamilies = new HashSet<ByteSequence>();
      for (LocalityGroupMetadata lgm : localityGroups) {
        if (!lgm.isDefaultLG)
          nonDefaultColumnFamilies.addAll(lgm.columnFamilies.keySet());
      }
      
      createHeap(lgReaders.length);
    }
    
    private Reader(Reader r) {
      super(r.lgReaders.length);
      this.reader = r.reader;
      this.nonDefaultColumnFamilies = r.nonDefaultColumnFamilies;
      this.lgReaders = new LocalityGroupReader[r.lgReaders.length];
      this.deepCopies = r.deepCopies;
      this.deepCopy = true;
      for (int i = 0; i < lgReaders.length; i++) {
        this.lgReaders[i] = new LocalityGroupReader(r.lgReaders[i]);
        this.lgReaders[i].setInterruptFlag(r.interruptFlag);
      }
    }
    
    private void closeLocalityGroupReaders() {
      for (LocalityGroupReader lgr : lgReaders) {
        try {
          lgr.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    
    @Override
    public void closeDeepCopies() {
      if (deepCopy)
        throw new RuntimeException("Calling closeDeepCopies on a deep copy is not supported");
      
      for (Reader deepCopy : deepCopies)
        deepCopy.closeLocalityGroupReaders();
      
      deepCopies.clear();
    }
    
    @Override
    public void close() throws IOException {
      if (deepCopy)
        throw new RuntimeException("Calling close on a deep copy is not supported");
      
      closeDeepCopies();
      closeLocalityGroupReaders();
      
      try {
        reader.close();
      } finally {
        /**
         * input Stream is passed to CachableBlockFile and closed there
         */
      }
    }
    
    @Override
    public Key getFirstKey() throws IOException {
      if (lgReaders.length == 0) {
        return null;
      }
      
      Key minKey = null;
      
      for (int i = 0; i < lgReaders.length; i++) {
        if (minKey == null) {
          minKey = lgReaders[i].getFirstKey();
        } else {
          Key firstKey = lgReaders[i].getFirstKey();
          if (firstKey != null && firstKey.compareTo(minKey) < 0)
            minKey = firstKey;
        }
      }
      
      return minKey;
    }
    
    @Override
    public Key getLastKey() throws IOException {
      if (lgReaders.length == 0) {
        return null;
      }
      
      Key maxKey = null;
      
      for (int i = 0; i < lgReaders.length; i++) {
        if (maxKey == null) {
          maxKey = lgReaders[i].getLastKey();
        } else {
          Key lastKey = lgReaders[i].getLastKey();
          if (lastKey != null && lastKey.compareTo(maxKey) > 0)
            maxKey = lastKey;
        }
      }
      
      return maxKey;
    }
    
    @Override
    public DataInputStream getMetaStore(String name) throws IOException, NoSuchMetaStoreException {
      try {
        return this.reader.getMetaBlock(name).getStream();
      } catch (MetaBlockDoesNotExist e) {
        throw new NoSuchMetaStoreException("name = " + name, e);
      }
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      Reader copy = new Reader(this);
      copy.setInterruptFlagInternal(interruptFlag);
      deepCopies.add(copy);
      return copy;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
      throw new UnsupportedOperationException();
      
    }
    
    private int numLGSeeked = 0;
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
      
      clear();
      
      numLGSeeked = 0;
      
      Set<ByteSequence> cfSet;
      if (columnFamilies.size() > 0)
        if (columnFamilies instanceof Set<?>) {
          cfSet = (Set<ByteSequence>) columnFamilies;
        } else {
          cfSet = new HashSet<ByteSequence>();
          cfSet.addAll(columnFamilies);
        }
      else
        cfSet = Collections.emptySet();
      
      for (LocalityGroupReader lgr : lgReaders) {
        
        // when exclude is set to true it means this locality group contains
        // unwanted column families
        boolean exclude = false;
        
        // when include is set to true it means this locality groups contains
        // wanted column families
        boolean include = false;
        
        if (cfSet.size() == 0) {
          include = !inclusive;
          exclude = false;
        } else if (lgr.isDefaultLocalityGroup && lgr.columnFamilies == null) {
          // do not know what column families are in the default locality group,
          // only know what column families are not in it
          
          if (inclusive) {
            if (!nonDefaultColumnFamilies.containsAll(cfSet)) {
              // default LG may contain wanted and unwanted column families
              exclude = true;
              include = true;
            }// else - everything wanted is in other locality groups, so nothing to do
          } else {
            // must include, if all excluded column families are in other locality groups
            // then there are not unwanted column families in default LG
            include = true;
            exclude = !nonDefaultColumnFamilies.containsAll(cfSet);
          }
        } else {
          /*
           * Need to consider the following cases for inclusive and exclusive (lgcf:locality group column family set, cf:column family set) lgcf and cf are
           * disjoint lgcf and cf are the same cf contains lgcf lgcf contains cf lgccf and cf intersect but neither is a subset of the other
           */
          
          for (Entry<ByteSequence,Count> entry : lgr.columnFamilies.entrySet())
            if (entry.getValue().count > 0)
              if (cfSet.contains(entry.getKey()))
                if (inclusive)
                  include = true;
                else
                  exclude = true;
              else if (inclusive)
                exclude = true;
              else
                include = true;
        }
        
        if (include) {
          if (exclude) {
            // want a subset of what is in the locality group, therefore filtering is need
            FileCFSkippingIterator cfe = new FileCFSkippingIterator(lgr);
            cfe.seek(range, cfSet, inclusive);
            addSource(cfe);
          } else {
            // want everything in this locality group, therefore no filtering is needed
            lgr.seek(range, EMPTY_CF_SET, false);
            addSource(lgr);
          }
          numLGSeeked++;
        }// every column family is excluded, zero count, or not present
      }
    }
    
    int getNumLocalityGroupsSeeked() {
      return numLGSeeked;
    }
    
    public FileSKVIterator getIndex() {
      
      ArrayList<List<IndexEntry>> indexes = new ArrayList<List<IndexEntry>>();
      
      for (LocalityGroupReader lgr : lgReaders) {
        indexes.add(lgr.getIndex());
      }
      
      return new MultiIndexIterator(this, indexes);
    }
    
    public void printInfo() {
      for (LocalityGroupMetadata lgm : localityGroups) {
        lgm.printInfo();
      }
      
    }
    
    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      if (deepCopy)
        throw new RuntimeException("Calling setInterruptFlag on a deep copy is not supported");
      
      if (deepCopies.size() != 0)
        throw new RuntimeException("Setting interrupt flag after calling deep copy not supported");
      
      setInterruptFlagInternal(flag);
    }
    
    private void setInterruptFlagInternal(AtomicBoolean flag) {
      this.interruptFlag = flag;
      for (LocalityGroupReader lgr : lgReaders) {
        lgr.setInterruptFlag(interruptFlag);
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    
    int max_row = 10000;
    int max_cf = 10;
    int max_cq = 10;
    
    // FSDataOutputStream fsout = fs.create(new Path("/tmp/test.rf"));
    
    // RFile.Writer w = new RFile.Writer(fsout, 1000, "gz", conf);
    CachableBlockFile.Writer _cbw = new CachableBlockFile.Writer(fs, new Path("/tmp/test.rf"), "gz", conf);
    RFile.Writer w = new RFile.Writer(_cbw, 100000);
    
    w.startDefaultLocalityGroup();
    
    int c = 0;
    
    for (int i = 0; i < max_row; i++) {
      Text row = new Text(String.format("R%06d", i));
      for (int j = 0; j < max_cf; j++) {
        Text cf = new Text(String.format("CF%06d", j));
        for (int k = 0; k < max_cq; k++) {
          Text cq = new Text(String.format("CQ%06d", k));
          w.append(new Key(row, cf, cq), new Value((c++ + "").getBytes()));
        }
      }
    }
    
    w.close();
    // fsout.close();
    
    // Logger.getLogger("accumulo.core.file.rfile").setLevel(Level.DEBUG);
    long t1 = System.currentTimeMillis();
    FSDataInputStream fsin = fs.open(new Path("/tmp/test.rf"));
    long t2 = System.currentTimeMillis();
    CachableBlockFile.Reader _cbr = new CachableBlockFile.Reader(fs, new Path("/tmp/test.rf"), conf, null, null);
    RFile.Reader r = new RFile.Reader(_cbr);
    long t3 = System.currentTimeMillis();
    
    System.out.println("Open time " + (t2 - t1) + " " + (t3 - t2));
    
    SortedKeyValueIterator<Key,Value> rd = r.deepCopy(null);
    SortedKeyValueIterator<Key,Value> rd2 = r.deepCopy(null);
    
    Random rand = new Random(10);
    
    seekRandomly(100, max_row, max_cf, max_cq, r, rand);
    
    rand = new Random(10);
    seekRandomly(100, max_row, max_cf, max_cq, rd, rand);
    
    rand = new Random(10);
    seekRandomly(100, max_row, max_cf, max_cq, rd2, rand);
    
    r.closeDeepCopies();
    
    seekRandomly(100, max_row, max_cf, max_cq, r, rand);
    
    rd = r.deepCopy(null);
    
    seekRandomly(100, max_row, max_cf, max_cq, rd, rand);
    
    r.close();
    fsin.close();
    
    seekRandomly(100, max_row, max_cf, max_cq, r, rand);
  }
  
  private static void seekRandomly(int num, int max_row, int max_cf, int max_cq, SortedKeyValueIterator<Key,Value> rd, Random rand) throws Exception {
    long t1 = System.currentTimeMillis();
    
    for (int i = 0; i < num; i++) {
      Text row = new Text(String.format("R%06d", rand.nextInt(max_row)));
      Text cf = new Text(String.format("CF%06d", rand.nextInt(max_cf)));
      Text cq = new Text(String.format("CQ%06d", rand.nextInt(max_cq)));
      
      Key sk = new Key(row, cf, cq);
      rd.seek(new Range(sk, null), new ArrayList<ByteSequence>(), false);
      if (!rd.hasTop() || !rd.getTopKey().equals(sk)) {
        System.err.println(sk + " != " + rd.getTopKey());
      }
      
    }
    
    long t2 = System.currentTimeMillis();
    
    double delta = ((t2 - t1) / 1000.0);
    System.out.println("" + delta + " " + num / delta);
    
    System.gc();
    System.gc();
    System.gc();
    
    Thread.sleep(3000);
  }
}
