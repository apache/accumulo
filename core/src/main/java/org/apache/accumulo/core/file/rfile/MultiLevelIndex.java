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
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.RandomAccess;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.file.blockfile.ABlockReader;
import org.apache.accumulo.core.file.blockfile.ABlockWriter;
import org.apache.accumulo.core.file.blockfile.BlockFileReader;
import org.apache.accumulo.core.file.blockfile.BlockFileWriter;
import org.apache.accumulo.core.file.rfile.bcfile.Utils;
import org.apache.hadoop.io.WritableComparable;

public class MultiLevelIndex {

  public static class IndexEntry implements WritableComparable<IndexEntry> {
    private Key key;
    private int entries;
    private long offset;
    private long compressedSize;
    private long rawSize;
    private boolean newFormat;

    IndexEntry(Key k, int e, long offset, long compressedSize, long rawSize) {
      this.key = k;
      this.entries = e;
      this.offset = offset;
      this.compressedSize = compressedSize;
      this.rawSize = rawSize;
      newFormat = true;
    }

    public IndexEntry(boolean newFormat) {
      this.newFormat = newFormat;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      key = new Key();
      key.readFields(in);
      entries = in.readInt();
      if (newFormat) {
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
      out.writeInt(entries);
      if (newFormat) {
        Utils.writeVLong(out, offset);
        Utils.writeVLong(out, compressedSize);
        Utils.writeVLong(out, rawSize);
      }
    }

    public Key getKey() {
      return key;
    }

    public int getNumEntries() {
      return entries;
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

    @Override
    public boolean equals(Object o) {
      if (o instanceof IndexEntry)
        return compareTo((IndexEntry) o) == 0;
      return false;
    }

    @Override
    public int hashCode() {
      assert false : "hashCode not designed";
      return 42; // any arbitrary constant will do
    }
  }

  // a list that deserializes index entries on demand
  private static class SerializedIndex extends AbstractList<IndexEntry> implements List<IndexEntry>, RandomAccess {

    private int[] offsets;
    private byte[] data;
    private boolean newFormat;

    SerializedIndex(int[] offsets, byte[] data, boolean newFormat) {
      this.offsets = offsets;
      this.data = data;
      this.newFormat = newFormat;
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

      IndexEntry ie = new IndexEntry(newFormat);
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

    private ArrayList<Integer> offsets;
    private int level;
    private int offset;

    SerializedIndex index;
    KeyIndex keyIndex;
    private boolean hasNext;

    public IndexBlock(int level, int totalAdded) {
      // System.out.println("IndexBlock("+level+","+levelCount+","+totalAdded+")");

      this.level = level;
      this.offset = totalAdded;

      indexBytes = new ByteArrayOutputStream();
      indexOut = new DataOutputStream(indexBytes);
      offsets = new ArrayList<Integer>();
    }

    public IndexBlock() {}

    public void add(Key key, int value, long offset, long compressedSize, long rawSize) throws IOException {
      offsets.add(indexOut.size());
      new IndexEntry(key, value, offset, compressedSize, rawSize).write(indexOut);
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

        index = new SerializedIndex(offsets, serializedIndex, true);
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
          IndexEntry ie = new IndexEntry(false);
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
        index = new SerializedIndex(oia, serializedIndex, false);
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

        index = new SerializedIndex(offsets, indexData, false);
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

    public BufferedWriter(Writer writer) {
      this.writer = writer;
      baos = new ByteArrayOutputStream(1 << 20);
      buffer = new DataOutputStream(baos);
      buffered = 0;
    }

    private void flush() throws IOException {
      buffer.close();

      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));

      IndexEntry ie = new IndexEntry(true);
      for (int i = 0; i < buffered; i++) {
        ie.readFields(dis);
        writer.add(ie.getKey(), ie.getNumEntries(), ie.getOffset(), ie.getCompressedSize(), ie.getRawSize());
      }

      buffered = 0;
      baos = new ByteArrayOutputStream(1 << 20);
      buffer = new DataOutputStream(baos);

    }

    public void add(Key key, int data, long offset, long compressedSize, long rawSize) throws IOException {
      if (buffer.size() > (10 * 1 << 20)) {
        flush();
      }

      new IndexEntry(key, data, offset, compressedSize, rawSize).write(buffer);
      buffered++;
    }

    public void addLast(Key key, int data, long offset, long compressedSize, long rawSize) throws IOException {
      flush();
      writer.addLast(key, data, offset, compressedSize, rawSize);
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

    private void add(int level, Key key, int data, long offset, long compressedSize, long rawSize) throws IOException {
      if (level == levels.size()) {
        levels.add(new IndexBlock(level, 0));
      }

      IndexBlock iblock = levels.get(level);

      iblock.add(key, data, offset, compressedSize, rawSize);
    }

    private void flush(int level, Key lastKey, boolean last) throws IOException {

      if (last && level == levels.size() - 1)
        return;

      IndexBlock iblock = levels.get(level);
      if ((iblock.getSize() > threshold && iblock.offsets.size() > 1) || last) {
        ABlockWriter out = blockFileWriter.prepareDataBlock();
        iblock.setHasNext(!last);
        iblock.write(out);
        out.close();

        add(level + 1, lastKey, 0, out.getStartPos(), out.getCompressedSize(), out.getRawSize());
        flush(level + 1, lastKey, last);

        if (last)
          levels.set(level, null);
        else
          levels.set(level, new IndexBlock(level, totalAdded));
      }
    }

    public void add(Key key, int data, long offset, long compressedSize, long rawSize) throws IOException {
      totalAdded++;
      add(0, key, data, offset, compressedSize, rawSize);
      flush(0, key, false);
    }

    public void addLast(Key key, int data, long offset, long compressedSize, long rawSize) throws IOException {
      if (addedLast)
        throw new IllegalStateException("already added last");

      totalAdded++;
      add(0, key, data, offset, compressedSize, rawSize);
      flush(0, key, true);
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

    public class Node {

      private Node parent;
      private IndexBlock indexBlock;
      private int currentPos;

      Node(Node parent, IndexBlock iBlock) {
        this.parent = parent;
        this.indexBlock = iBlock;
      }

      Node(IndexBlock rootInfo) {
        this.parent = null;
        this.indexBlock = rootInfo;
      }

      private Node lookup(Key key) throws IOException {
        int pos = Collections.binarySearch(indexBlock.getKeyIndex(), key, new Comparator<Key>() {
          @Override
          public int compare(Key o1, Key o2) {
            return o1.compareTo(o2);
          }
        });

        if (pos < 0)
          pos = (pos * -1) - 1;

        if (pos == indexBlock.getIndex().size()) {
          if (parent != null)
            throw new IllegalStateException();
          this.currentPos = pos;
          return this;
        }

        this.currentPos = pos;

        if (indexBlock.getLevel() == 0) {
          return this;
        }

        IndexEntry ie = indexBlock.getIndex().get(pos);
        Node child = new Node(this, getIndexBlock(ie));
        return child.lookup(key);
      }

      private Node getLast() throws IOException {
        currentPos = indexBlock.getIndex().size() - 1;
        if (indexBlock.getLevel() == 0)
          return this;

        IndexEntry ie = indexBlock.getIndex().get(currentPos);
        Node child = new Node(this, getIndexBlock(ie));
        return child.getLast();
      }

      private Node getFirst() throws IOException {
        currentPos = 0;
        if (indexBlock.getLevel() == 0)
          return this;

        IndexEntry ie = indexBlock.getIndex().get(currentPos);
        Node child = new Node(this, getIndexBlock(ie));
        return child.getFirst();
      }

      private Node getPrevious() throws IOException {
        if (currentPos == 0)
          return parent.getPrevious();

        currentPos--;

        IndexEntry ie = indexBlock.getIndex().get(currentPos);
        Node child = new Node(this, getIndexBlock(ie));
        return child.getLast();

      }

      private Node getNext() throws IOException {
        if (currentPos == indexBlock.getIndex().size() - 1)
          return parent.getNext();

        currentPos++;

        IndexEntry ie = indexBlock.getIndex().get(currentPos);
        Node child = new Node(this, getIndexBlock(ie));
        return child.getFirst();

      }

      Node getNextNode() throws IOException {
        return parent.getNext();
      }

      Node getPreviousNode() throws IOException {
        return parent.getPrevious();
      }
    }

    static public class IndexIterator implements ListIterator<IndexEntry> {

      private Node node;
      private ListIterator<IndexEntry> liter;

      private Node getPrevNode() {
        try {
          return node.getPreviousNode();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      private Node getNextNode() {
        try {
          return node.getNextNode();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      public IndexIterator() {
        node = null;
      }

      public IndexIterator(Node node) {
        this.node = node;
        liter = node.indexBlock.getIndex().listIterator(node.currentPos);
      }

      @Override
      public boolean hasNext() {
        if (node == null)
          return false;

        if (!liter.hasNext()) {
          return node.indexBlock.hasNext();
        } else {
          return true;
        }

      }

      public IndexEntry peekPrevious() {
        IndexEntry ret = previous();
        next();
        return ret;
      }

      public IndexEntry peek() {
        IndexEntry ret = next();
        previous();
        return ret;
      }

      @Override
      public IndexEntry next() {
        if (!liter.hasNext()) {
          node = getNextNode();
          liter = node.indexBlock.getIndex().listIterator();
        }

        return liter.next();
      }

      @Override
      public boolean hasPrevious() {
        if (node == null)
          return false;

        if (!liter.hasPrevious()) {
          return node.indexBlock.getOffset() > 0;
        } else {
          return true;
        }
      }

      @Override
      public IndexEntry previous() {
        if (!liter.hasPrevious()) {
          node = getPrevNode();
          liter = node.indexBlock.getIndex().listIterator(node.indexBlock.getIndex().size());
        }

        return liter.previous();
      }

      @Override
      public int nextIndex() {
        return node.indexBlock.getOffset() + liter.nextIndex();
      }

      @Override
      public int previousIndex() {
        return node.indexBlock.getOffset() + liter.previousIndex();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void set(IndexEntry e) {
        throw new UnsupportedOperationException();

      }

      @Override
      public void add(IndexEntry e) {
        throw new UnsupportedOperationException();
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

    public IndexIterator lookup(Key key) throws IOException {
      Node node = new Node(rootBlock);
      return new IndexIterator(node.lookup(key));
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
  }

}
