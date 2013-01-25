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
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Stack;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.file.blockfile.ABlockReader;
import org.apache.accumulo.core.file.blockfile.ABlockWriter;
import org.apache.accumulo.core.file.blockfile.BlockFileReader;
import org.apache.accumulo.core.file.blockfile.BlockFileWriter;
import org.apache.accumulo.core.file.rfile.bcfile.Utils;
import org.apache.accumulo.core.iterators.predicates.ColumnVisibilityPredicate;
import org.apache.accumulo.core.iterators.predicates.TimestampRangePredicate;
import org.apache.hadoop.io.WritableComparable;

public class MultiLevelIndex {
  
  public static class IndexEntry implements WritableComparable<IndexEntry> {
    private Key key;
    private BlockStats blockStats;
    private long offset;
    private long compressedSize;
    private long rawSize;
    private int format;
    
    IndexEntry(Key k, BlockStats blockStats, long offset, long compressedSize, long rawSize, int version) {
      this.key = k;
      this.blockStats = blockStats;
      this.offset = offset;
      this.compressedSize = compressedSize;
      this.rawSize = rawSize;
      format = version;
    }
    
    public IndexEntry(int format) {
      this.format = format;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      key = new Key();
      key.readFields(in);
      blockStats = new BlockStats(format);
      blockStats.readFields(in);
      if (format == RFile.RINDEX_VER_6 || format == RFile.RINDEX_VER_7) {
        offset = Utils.readVLong(in);
        compressedSize = Utils.readVLong(in);
        rawSize = Utils.readVLong(in);
      } else {
        offset = -1;
        compressedSize = -1;
        rawSize = -1;
      }
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      key.write(out);
      blockStats.write(out);
      if (format == RFile.RINDEX_VER_6 || format == RFile.RINDEX_VER_7) {
        Utils.writeVLong(out, offset);
        Utils.writeVLong(out, compressedSize);
        Utils.writeVLong(out, rawSize);
      }
    }
    
    public Key getKey() {
      return key;
    }
    
    public int getNumEntries() {
      return blockStats.entries;
    }
    
    public long getOffset() {
      return offset;
    }
    
    public long getCompressedSize() {
      return compressedSize;
    }
    
    public long getRawSize() {
      return rawSize;
    }
    
    @Override
    public int compareTo(IndexEntry o) {
      return key.compareTo(o.key);
    }
  }
  
  // a list that deserializes index entries on demand
  private static class SerializedIndex extends AbstractList<IndexEntry> implements List<IndexEntry>, RandomAccess {
    
    private int[] offsets;
    private byte[] data;
    private int format;
    
    SerializedIndex(int[] offsets, byte[] data, int format) {
      this.offsets = offsets;
      this.data = data;
      this.format = format;
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
      
      IndexEntry ie = new IndexEntry(format);
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
  
  private static class KeyIndex extends AbstractList<Key> implements List<Key>, RandomAccess {
    
    private int[] offsets;
    private byte[] data;
    
    KeyIndex(int[] offsets, byte[] data) {
      this.offsets = offsets;
      this.data = data;
    }
    
    @Override
    public Key get(int index) {
      int len;
      if (index == offsets.length - 1)
        len = data.length - offsets[index];
      else
        len = offsets[index + 1] - offsets[index];
      
      ByteArrayInputStream bais = new ByteArrayInputStream(data, offsets[index], len);
      DataInputStream dis = new DataInputStream(bais);
      
      Key key = new Key();
      try {
        key.readFields(dis);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      
      return key;
    }
    
    @Override
    public int size() {
      return offsets.length;
    }
  }
  
  static class IndexBlock {
    
    private ByteArrayOutputStream indexBytes;
    private DataOutputStream indexOut;
    
    private BlockStats blockStats = new BlockStats();
    
    private ArrayList<Integer> offsets;
    private int level;
    private int offset;
    
    SerializedIndex index;
    KeyIndex keyIndex;
    private boolean hasNext;
    
    public IndexBlock(int level, int totalAdded) {
      this.level = level;
      this.offset = totalAdded;
      
      indexBytes = new ByteArrayOutputStream();
      indexOut = new DataOutputStream(indexBytes);
      offsets = new ArrayList<Integer>();
    }
    
    public IndexBlock() {}
    
    public void add(Key key, BlockStats blockStats, long offset, long compressedSize, long rawSize, int version) throws IOException {
      offsets.add(indexOut.size());
      this.blockStats.updateBlockStats(blockStats);
      new IndexEntry(key, blockStats, offset, compressedSize, rawSize, version).write(indexOut);
    }
    
    int getSize() {
      return indexOut.size() + 4 * offsets.size();
    }
    
    public void write(DataOutput out) throws IOException {
      out.writeInt(level);
      out.writeInt(offset);
      out.writeBoolean(hasNext);
      
      out.writeInt(offsets.size());
      for (Integer offset : offsets) {
        out.writeInt(offset);
      }
      
      indexOut.close();
      byte[] indexData = indexBytes.toByteArray();
      
      out.writeInt(indexData.length);
      out.write(indexData);
    }
    
    public void readFields(DataInput in, int version) throws IOException {
      
      if (version == RFile.RINDEX_VER_6 || version == RFile.RINDEX_VER_7) {
        level = in.readInt();
        offset = in.readInt();
        hasNext = in.readBoolean();
        
        int numOffsets = in.readInt();
        int[] offsets = new int[numOffsets];
        
        for (int i = 0; i < numOffsets; i++)
          offsets[i] = in.readInt();
        
        int indexSize = in.readInt();
        byte[] serializedIndex = new byte[indexSize];
        in.readFully(serializedIndex);
        
        index = new SerializedIndex(offsets, serializedIndex, version);
        keyIndex = new KeyIndex(offsets, serializedIndex);
      } else if (version == RFile.RINDEX_VER_3) {
        level = 0;
        offset = 0;
        hasNext = false;
        
        int size = in.readInt();
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ArrayList<Integer> oal = new ArrayList<Integer>();
        
        for (int i = 0; i < size; i++) {
          IndexEntry ie = new IndexEntry(version);
          oal.add(dos.size());
          ie.readFields(in);
          ie.write(dos);
        }
        
        dos.close();
        
        int[] oia = new int[oal.size()];
        for (int i = 0; i < oal.size(); i++) {
          oia[i] = oal.get(i);
        }
        
        byte[] serializedIndex = baos.toByteArray();
        index = new SerializedIndex(oia, serializedIndex, version);
        keyIndex = new KeyIndex(oia, serializedIndex);
      } else if (version == RFile.RINDEX_VER_4) {
        level = 0;
        offset = 0;
        hasNext = false;
        
        int numIndexEntries = in.readInt();
        int offsets[] = new int[numIndexEntries];
        for (int i = 0; i < numIndexEntries; i++) {
          offsets[i] = in.readInt();
        }
        
        int size = in.readInt();
        byte[] indexData = new byte[size];
        in.readFully(indexData);
        
        index = new SerializedIndex(offsets, indexData, version);
        keyIndex = new KeyIndex(offsets, indexData);
      } else {
        throw new RuntimeException("Unexpected version " + version);
      }
      
    }
    
    List<IndexEntry> getIndex() {
      return index;
    }
    
    public List<Key> getKeyIndex() {
      return keyIndex;
    }
    
    int getLevel() {
      return level;
    }
    
    int getOffset() {
      return offset;
    }
    
    boolean hasNext() {
      return hasNext;
    }
    
    void setHasNext(boolean b) {
      this.hasNext = b;
    }
    
  }
  
  /**
   * this class buffers writes to the index so that chunks of index blocks are contiguous in the file instead of having index blocks sprinkled throughout the
   * file making scans of the entire index slow.
   */
  public static class BufferedWriter {
    
    private Writer writer;
    private DataOutputStream buffer;
    private int buffered;
    private ByteArrayOutputStream baos;
    private final int version;
    
    public BufferedWriter(Writer writer) {
      this.writer = writer;
      baos = new ByteArrayOutputStream(1 << 20);
      buffer = new DataOutputStream(baos);
      buffered = 0;
      version = RFile.RINDEX_VER_7;
    }
    
    private void flush() throws IOException {
      buffer.close();
      
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
      
      IndexEntry ie = new IndexEntry(version);
      for (int i = 0; i < buffered; i++) {
        ie.readFields(dis);
        writer.add(ie.getKey(), ie.blockStats, ie.getOffset(), ie.getCompressedSize(), ie.getRawSize(), ie.format);
      }
      
      buffered = 0;
      baos = new ByteArrayOutputStream(1 << 20);
      buffer = new DataOutputStream(baos);
      
    }
    
    public void add(Key key, BlockStats blockStats, long offset, long compressedSize, long rawSize, int version) throws IOException {
      if (buffer.size() > (10 * 1 << 20)) {
        flush();
      }
      
      new IndexEntry(key, blockStats, offset, compressedSize, rawSize, version).write(buffer);
      buffered++;
    }
    
    public void addLast(Key key, BlockStats blockStats, long offset, long compressedSize, long rawSize, int version) throws IOException {
      flush();
      writer.addLast(key, blockStats, offset, compressedSize, rawSize, version);
    }
    
    public void close(DataOutput out) throws IOException {
      writer.close(out);
    }
  }
  
  public static class Writer {
    private int threshold;
    
    private ArrayList<IndexBlock> levels;
    
    private int totalAdded;
    
    private boolean addedLast = false;
    
    private BlockFileWriter blockFileWriter;
    
    Writer(BlockFileWriter blockFileWriter, int maxBlockSize) {
      this.blockFileWriter = blockFileWriter;
      this.threshold = maxBlockSize;
      levels = new ArrayList<IndexBlock>();
    }
    
    private void add(int level, Key key, BlockStats blockStats, long offset, long compressedSize, long rawSize, boolean last, int version) throws IOException {
      if (level == levels.size()) {
        levels.add(new IndexBlock(level, 0));
      }
      
      IndexBlock iblock = levels.get(level);
      
      iblock.add(key, blockStats, offset, compressedSize, rawSize, version);
      
      if (last && level == levels.size() - 1)
        return;
      
      if ((iblock.getSize() > threshold && iblock.offsets.size() > 1) || last) {
        ABlockWriter out = blockFileWriter.prepareDataBlock();
        iblock.setHasNext(!last);
        iblock.write(out);
        out.close();
        
        add(level + 1, key, iblock.blockStats, out.getStartPos(), out.getCompressedSize(), out.getRawSize(), last, version);
        
        if (last)
          levels.set(level, null);
        else
          levels.set(level, new IndexBlock(level, totalAdded));
      }
    }
    
    public void add(Key key, BlockStats blockStats, long offset, long compressedSize, long rawSize, int version) throws IOException {
      totalAdded++;
      add(0, key, blockStats, offset, compressedSize, rawSize, false, version);
    }
    
    public void addLast(Key key, BlockStats blockStats, long offset, long compressedSize, long rawSize, int version) throws IOException {
      if (addedLast)
        throw new IllegalStateException("already added last");
      
      totalAdded++;
      add(0, key, blockStats, offset, compressedSize, rawSize, true, version);
      addedLast = true;
      
    }
    
    public void close(DataOutput out) throws IOException {
      if (totalAdded > 0 && !addedLast)
        throw new IllegalStateException("did not call addLast");
      
      out.writeInt(totalAdded);
      // save root node
      if (levels.size() > 0) {
        levels.get(levels.size() - 1).write(out);
      } else {
        new IndexBlock(0, 0).write(out);
      }
      
    }
  }
  
  public static class Reader {
    private IndexBlock rootBlock;
    private BlockFileReader blockStore;
    private int version;
    private int size;
    
    class StackEntry {
      public final IndexBlock block;
      public int offset;
      
      public StackEntry(IndexBlock block, int offset) {
        this.block = block;
        this.offset = offset;
      }
    }
    
    class IndexIterator implements Iterator<IndexEntry> {
      private Stack<StackEntry> position = new Stack<StackEntry>();
      private final TimestampRangePredicate timestampFilter;
      private final ColumnVisibilityPredicate columnVisibilityPredicate;
      
      private IndexIterator(TimestampRangePredicate timestampFilter, ColumnVisibilityPredicate columnVisibilityPredicate, Key lookupKey) {
        this.timestampFilter = timestampFilter;
        this.columnVisibilityPredicate = columnVisibilityPredicate;
        try {
          seek(lookupKey);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      
      private final boolean checkFilterIndexEntry(IndexEntry ie) {
        if(timestampFilter != null && (ie.blockStats.maxTimestamp < timestampFilter.getStartTimestamp() || ie.blockStats.minTimestamp > timestampFilter.getEndTimestamp()))
          return false;
        if(columnVisibilityPredicate != null && ie.blockStats.minimumVisibility != null && ie.blockStats.minimumVisibility.evaluate(columnVisibilityPredicate.getAuthorizations()) == false)
          return false;
        return true;
      }
      
      private void seek(Key lookupKey) throws IOException {
        StackEntry top = new StackEntry(rootBlock, -1);
        position.add(top);
        while (true) {
          top = position.peek();
          // go down the tree
          int pos = Collections.binarySearch(top.block.getKeyIndex(), lookupKey, new Comparator<Key>() {
            @Override
            public int compare(Key o1, Key o2) {
              return o1.compareTo(o2);
            }
          });
          
          if (pos < 0) {
            pos = (pos * -1) - 1;
          } else if (pos < top.block.getKeyIndex().size()) {
            // the exact key was found, so we want to go back to the first identical match
            while (pos > 0 && top.block.getKeyIndex().get(pos - 1).equals(lookupKey)) {
              pos--;
            }
          }
          
          IndexEntry ie = null;
          List<IndexEntry> index = top.block.getIndex();
          
          if (pos > 0) {
            // look backwards to find any initial previousEntry that might match the timestamp range such that no entry within the given timestamp range is
            // between the seeked key and the previousKey
            previousEntry = index.get(pos - 1);
            // TODO: find the offset for this block
            previousIndex = Integer.MIN_VALUE;
          }
          
          while (pos < index.size()) {
            ie = index.get(pos);
            // filter on timestampRange by skipping forward until a block passes the predicate
            if (checkFilterIndexEntry(ie))
              break;
            pos++;
          }
          
          if (pos == index.size()) {
            position.pop();
            goToNext();
            return;
          } else {
            if (top.block.level == 0) {
              // found a matching index entry -- set the pointer to be just before this location
              top.offset = pos - 1;
              return;
            } else {
              top.offset = pos;
              position.add(new StackEntry(getIndexBlock(ie), 0));
            }
          }
        }
      }
      
      private void goToNext() throws IOException {
        // traverse the index tree forwards
        while (position.isEmpty() == false) {
          StackEntry top = position.peek();
          top.offset++;
          List<IndexEntry> index = top.block.getIndex();
          while (top.offset < index.size()) {
            if (checkFilterIndexEntry(index.get(top.offset)))
              break;
            top.offset++;
          }
          if (top.offset == index.size()) {
            // go up
            position.pop();
          } else {
            if (top.block.level == 0) {
              // success!
              break;
            }
            // go down
            position.add(new StackEntry(getIndexBlock(index.get(top.offset)), -1));
          }
        }
        if (position.isEmpty())
          return;
        StackEntry e = position.peek();
        nextEntry = e.block.getIndex().get(e.offset);
        nextIndex = e.block.getOffset() + e.offset;
      }
      
      IndexEntry nextEntry = null;
      IndexEntry previousEntry = null;
      int nextIndex = -1;
      int previousIndex = -1;
      
      private void prepNext() {
        if (nextEntry == null) {
          try {
            goToNext();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
      
      public boolean hasNext() {
        if (nextEntry == null)
          prepNext();
        return nextEntry != null;
        
      }
      
      // initially, previous key is last key of the previous block
      public boolean hasPrevious() {
        return previousEntry != null;
      }
      
      public int nextIndex() {
        if (nextEntry == null)
          prepNext();
        return nextIndex;
      }
      
      public IndexEntry peek() {
        if (nextEntry == null)
          prepNext();
        return nextEntry;
      }
      
      public IndexEntry next() {
        prepNext();
        previousEntry = nextEntry;
        nextEntry = null;
        previousIndex = nextIndex;
        nextIndex = -1;
        return previousEntry;
      }
      
      public IndexEntry peekPrevious() {
        return previousEntry;
      }
      
      /*
       * (non-Javadoc)
       * 
       * @see java.util.Iterator#remove()
       */
      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
      public int previousIndex() {
        return previousIndex;
      }
      
    }
    
    public Reader(BlockFileReader blockStore, int version) {
      this.version = version;
      this.blockStore = blockStore;
    }
    
    private IndexBlock getIndexBlock(IndexEntry ie) throws IOException {
      IndexBlock iblock = new IndexBlock();
      ABlockReader in = blockStore.getMetaBlock(ie.getOffset(), ie.getCompressedSize(), ie.getRawSize());
      iblock.readFields(in, version);
      in.close();
      
      return iblock;
    }
    
    IndexIterator lookup(Key key) throws IOException {
      return new IndexIterator(timestampRange, columnVisibilityPredicate, key);
    }
    
    public void readFields(DataInput in) throws IOException {
      
      size = 0;
      
      if (version == RFile.RINDEX_VER_6 || version == RFile.RINDEX_VER_7) {
        size = in.readInt();
      }
      
      rootBlock = new IndexBlock();
      rootBlock.readFields(in, version);
      
      if (version == RFile.RINDEX_VER_3 || version == RFile.RINDEX_VER_4) {
        size = rootBlock.getIndex().size();
      }
    }
    
    public int size() {
      return size;
    }
    
    private void getIndexInfo(IndexBlock ib, Map<Integer,Long> sizesByLevel, Map<Integer,Long> countsByLevel) throws IOException {
      Long size = sizesByLevel.get(ib.getLevel());
      if (size == null)
        size = 0l;
      
      Long count = countsByLevel.get(ib.getLevel());
      if (count == null)
        count = 0l;
      
      size += ib.index.sizeInBytes();
      count++;
      
      sizesByLevel.put(ib.getLevel(), size);
      countsByLevel.put(ib.getLevel(), count);
      
      if (ib.getLevel() > 0) {
        for (IndexEntry ie : ib.index) {
          IndexBlock cib = getIndexBlock(ie);
          getIndexInfo(cib, sizesByLevel, countsByLevel);
        }
      }
    }
    
    public void getIndexInfo(Map<Integer,Long> sizes, Map<Integer,Long> counts) throws IOException {
      getIndexInfo(rootBlock, sizes, counts);
    }
    
    public Key getLastKey() {
      return rootBlock.getIndex().get(rootBlock.getIndex().size() - 1).getKey();
    }
    
    TimestampRangePredicate timestampRange = null;
    
    /**
     * @param r
     */
    public void setTimestampRange(TimestampRangePredicate r) {
      this.timestampRange = r;
    }
    
    ColumnVisibilityPredicate columnVisibilityPredicate = null;
    
    public void setColumnVisibilityPredicate(ColumnVisibilityPredicate columnVisibilityPredicate) {
      this.columnVisibilityPredicate = columnVisibilityPredicate;
    }
  }
  
}
