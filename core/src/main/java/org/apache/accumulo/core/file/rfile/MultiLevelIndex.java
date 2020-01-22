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

import static java.util.Objects.requireNonNull;

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
import org.apache.accumulo.core.file.blockfile.impl.SeekableByteArrayInputStream;
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

  private static abstract class SerializedIndexBase<T> extends AbstractList<T>
      implements RandomAccess {
    protected int[] offsets;
    protected byte[] data;

    protected SeekableByteArrayInputStream sbais;
    protected DataInputStream dis;
    protected int offsetsOffset;
    protected int indexOffset;
    protected int numOffsets;
    protected int indexSize;

    SerializedIndexBase(int[] offsets, byte[] data) {
      requireNonNull(offsets, "offsets argument was null");
      requireNonNull(data, "data argument was null");
      this.offsets = offsets;
      this.data = data;
      sbais = new SeekableByteArrayInputStream(data);
      dis = new DataInputStream(sbais);
    }

    SerializedIndexBase(byte[] data, int offsetsOffset, int numOffsets, int indexOffset,
        int indexSize) {
      requireNonNull(data, "data argument was null");
      sbais = new SeekableByteArrayInputStream(data, indexOffset + indexSize);
      dis = new DataInputStream(sbais);
      this.offsetsOffset = offsetsOffset;
      this.indexOffset = indexOffset;
      this.numOffsets = numOffsets;
      this.indexSize = indexSize;
    }

    /**
     * Before this method is called, {@code this.dis} is seeked to the offset of a serialized index
     * entry. This method should deserialize the index entry by reading from {@code this.dis} and
     * return it.
     */
    protected abstract T newValue() throws IOException;

    @Override
    public T get(int index) {
      try {
        int offset;
        if (offsets == null) {
          if (index < 0 || index >= numOffsets) {
            throw new IndexOutOfBoundsException("index:" + index + " numOffsets:" + numOffsets);
          }
          sbais.seek(offsetsOffset + index * 4);
          offset = dis.readInt();
        } else {
          offset = offsets[index];
        }

        sbais.seek(indexOffset + offset);
        return newValue();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public int size() {
      if (offsets == null) {
        return numOffsets;
      } else {
        return offsets.length;
      }
    }

  }

  // a list that deserializes index entries on demand
  private static class SerializedIndex extends SerializedIndexBase<IndexEntry> {

    private boolean newFormat;

    SerializedIndex(int[] offsets, byte[] data, boolean newFormat) {
      super(offsets, data);
      this.newFormat = newFormat;
    }

    SerializedIndex(byte[] data, int offsetsOffset, int numOffsets, int indexOffset,
        int indexSize) {
      super(data, offsetsOffset, numOffsets, indexOffset, indexSize);
      this.newFormat = true;
    }

    public long sizeInBytes() {
      if (offsets == null) {
        return indexSize + 4 * numOffsets;
      } else {
        return data.length + 4 * offsets.length;
      }
    }

    @Override
    protected IndexEntry newValue() throws IOException {
      IndexEntry ie = new IndexEntry(newFormat);
      ie.readFields(dis);
      return ie;
    }

  }

  private static class KeyIndex extends SerializedIndexBase<Key> {

    KeyIndex(int[] offsets, byte[] data) {
      super(offsets, data);
    }

    KeyIndex(byte[] data, int offsetsOffset, int numOffsets, int indexOffset, int indexSize) {
      super(data, offsetsOffset, numOffsets, indexOffset, indexSize);
    }

    @Override
    protected Key newValue() throws IOException {
      Key key = new Key();
      key.readFields(dis);
      return key;
    }
  }

  static class IndexBlock {

    private ByteArrayOutputStream indexBytes;
    private DataOutputStream indexOut;

    private ArrayList<Integer> offsets;
    private int level;
    private int offset;
    private boolean hasNext;

    private byte data[];
    private int[] offsetsArray;
    private int numOffsets;
    private int offsetsOffset;
    private int indexSize;
    private int indexOffset;
    private boolean newFormat;

    public IndexBlock(int level, int totalAdded) {
      // System.out.println("IndexBlock("+level+","+levelCount+","+totalAdded+")");

      this.level = level;
      this.offset = totalAdded;

      indexBytes = new ByteArrayOutputStream();
      indexOut = new DataOutputStream(indexBytes);
      offsets = new ArrayList<>();
    }

    public IndexBlock() {}

    public void add(Key key, int value, long offset, long compressedSize, long rawSize)
        throws IOException {
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

      if (version == RFile.RINDEX_VER_6 || version == RFile.RINDEX_VER_7
          || version == RFile.RINDEX_VER_8) {
        level = in.readInt();
        offset = in.readInt();
        hasNext = in.readBoolean();

        ABlockReader abr = (ABlockReader) in;
        if (abr.isIndexable()) {
          // this block is cahced, so avoid copy
          data = abr.getBuffer();
          // use offset data in serialized form and avoid copy
          numOffsets = abr.readInt();
          offsetsOffset = abr.getPosition();
          int skipped = abr.skipBytes(numOffsets * 4);
          if (skipped != numOffsets * 4) {
            throw new IOException("Skipped less than expected " + skipped + " " + (numOffsets * 4));
          }
          indexSize = in.readInt();
          indexOffset = abr.getPosition();
          skipped = abr.skipBytes(indexSize);
          if (skipped != indexSize) {
            throw new IOException("Skipped less than expected " + skipped + " " + indexSize);
          }
        } else {
          numOffsets = in.readInt();
          offsetsArray = new int[numOffsets];

          for (int i = 0; i < numOffsets; i++)
            offsetsArray[i] = in.readInt();

          indexSize = in.readInt();
          data = new byte[indexSize];
          in.readFully(data);
          newFormat = true;
        }
      } else if (version == RFile.RINDEX_VER_3) {
        level = 0;
        offset = 0;
        hasNext = false;

        int size = in.readInt();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ArrayList<Integer> oal = new ArrayList<>();

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

        data = baos.toByteArray();
        offsetsArray = oia;
        newFormat = false;
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

        data = indexData;
        offsetsArray = offsets;
        newFormat = false;
      } else {
        throw new RuntimeException("Unexpected version " + version);
      }

    }

    List<IndexEntry> getIndex() {
      // create SerializedIndex on demand as each has an internal input stream over byte array...
      // keeping a SerializedIndex ref for the object could lead to
      // problems with deep copies.
      if (offsetsArray == null) {
        return new SerializedIndex(data, offsetsOffset, numOffsets, indexOffset, indexSize);
      } else {
        return new SerializedIndex(offsetsArray, data, newFormat);
      }
    }

    public List<Key> getKeyIndex() {
      // create KeyIndex on demand as each has an internal input stream over byte array... keeping a
      // KeyIndex ref for the object could lead to problems with
      // deep copies.
      if (offsetsArray == null) {
        return new KeyIndex(data, offsetsOffset, numOffsets, indexOffset, indexSize);
      } else {
        return new KeyIndex(offsetsArray, data);
      }
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
   * this class buffers writes to the index so that chunks of index blocks are contiguous in the
   * file instead of having index blocks sprinkled throughout the file making scans of the entire
   * index slow.
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
        writer.add(ie.getKey(), ie.getNumEntries(), ie.getOffset(), ie.getCompressedSize(),
            ie.getRawSize());
      }

      buffered = 0;
      baos = new ByteArrayOutputStream(1 << 20);
      buffer = new DataOutputStream(baos);

    }

    public void add(Key key, int data, long offset, long compressedSize, long rawSize)
        throws IOException {
      if (buffer.size() > (10 * 1 << 20)) {
        flush();
      }

      new IndexEntry(key, data, offset, compressedSize, rawSize).write(buffer);
      buffered++;
    }

    public void addLast(Key key, int data, long offset, long compressedSize, long rawSize)
        throws IOException {
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
      levels = new ArrayList<>();
    }

    private void add(int level, Key key, int data, long offset, long compressedSize, long rawSize)
        throws IOException {
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

    public void add(Key key, int data, long offset, long compressedSize, long rawSize)
        throws IOException {
      totalAdded++;
      add(0, key, data, offset, compressedSize, rawSize);
      flush(0, key, false);
    }

    public void addLast(Key key, int data, long offset, long compressedSize, long rawSize)
        throws IOException {
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
      ABlockReader in =
          blockStore.getMetaBlock(ie.getOffset(), ie.getCompressedSize(), ie.getRawSize());
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

      if (version == RFile.RINDEX_VER_6 || version == RFile.RINDEX_VER_7
          || version == RFile.RINDEX_VER_8) {
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

    private void getIndexInfo(IndexBlock ib, Map<Integer,Long> sizesByLevel,
        Map<Integer,Long> countsByLevel) throws IOException {
      Long size = sizesByLevel.get(ib.getLevel());
      if (size == null)
        size = 0l;

      Long count = countsByLevel.get(ib.getLevel());
      if (count == null)
        count = 0l;

      List<IndexEntry> index = ib.getIndex();
      size += ((SerializedIndex) index).sizeInBytes();
      count++;

      sizesByLevel.put(ib.getLevel(), size);
      countsByLevel.put(ib.getLevel(), count);

      if (ib.getLevel() > 0) {
        for (IndexEntry ie : index) {
          IndexBlock cib = getIndexBlock(ie);
          getIndexInfo(cib, sizesByLevel, countsByLevel);
        }
      }
    }

    public void getIndexInfo(Map<Integer,Long> sizes, Map<Integer,Long> counts) throws IOException {
      getIndexInfo(rootBlock, sizes, counts);
    }

    private void printIndex(IndexBlock ib, String prefix, PrintStream out) throws IOException {
      List<IndexEntry> index = ib.getIndex();

      StringBuilder sb = new StringBuilder();
      sb.append(prefix);

      sb.append("Level: ");
      sb.append(ib.getLevel());

      int resetLen = sb.length();

      String recursePrefix = prefix + "  ";

      for (IndexEntry ie : index) {

        sb.setLength(resetLen);

        sb.append(" Key: ");
        sb.append(ie.key);
        sb.append(" NumEntries: ");
        sb.append(ie.entries);
        sb.append(" Offset: ");
        sb.append(ie.offset);
        sb.append(" CompressedSize: ");
        sb.append(ie.compressedSize);
        sb.append(" RawSize : ");
        sb.append(ie.rawSize);

        out.println(sb.toString());

        if (ib.getLevel() > 0) {
          IndexBlock cib = getIndexBlock(ie);
          printIndex(cib, recursePrefix, out);
        }
      }
    }

    public void printIndex(String prefix, PrintStream out) throws IOException {
      printIndex(rootBlock, prefix, out);
    }

    public Key getLastKey() {
      return rootBlock.getIndex().get(rootBlock.getIndex().size() - 1).getKey();
    }
  }
}
