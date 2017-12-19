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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.ABlockReader;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry.Weighbable;
import org.apache.accumulo.core.file.blockfile.cache.impl.ClassSize;
import org.apache.accumulo.core.file.blockfile.cache.impl.SizeConstants;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.IndexEntry;

/**
 *
 */
public class BlockIndex implements Weighbable {

  public static BlockIndex getIndex(ABlockReader cacheBlock, IndexEntry indexEntry) throws IOException {

    BlockIndex blockIndex = cacheBlock.getIndex(BlockIndex::new);
    if (blockIndex == null)
      return null;

    int accessCount = blockIndex.accessCount.incrementAndGet();

    // 1 is a power of two, but do not care about it
    if (accessCount >= 2 && isPowerOfTwo(accessCount)) {
      blockIndex.buildIndex(accessCount, cacheBlock, indexEntry);
      cacheBlock.indexWeightChanged();
    }

    if (blockIndex.blockIndex != null)
      return blockIndex;

    return null;
  }

  private static boolean isPowerOfTwo(int x) {
    return ((x > 0) && (x & (x - 1)) == 0);
  }

  private AtomicInteger accessCount = new AtomicInteger(0);
  private volatile BlockIndexEntry[] blockIndex = null;

  public static class BlockIndexEntry implements Comparable<BlockIndexEntry> {

    private Key prevKey;
    private int entriesLeft;
    private int pos;

    public BlockIndexEntry(int pos, int entriesLeft, Key prevKey) {
      this.pos = pos;
      this.entriesLeft = entriesLeft;
      this.prevKey = prevKey;
    }

    public BlockIndexEntry(Key key) {
      this.prevKey = key;
    }

    public int getEntriesLeft() {
      return entriesLeft;
    }

    @Override
    public int compareTo(BlockIndexEntry o) {
      return prevKey.compareTo(o.prevKey);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof BlockIndexEntry)
        return compareTo((BlockIndexEntry) o) == 0;
      return false;
    }

    @Override
    public String toString() {
      return prevKey + " " + entriesLeft + " " + pos;
    }

    public Key getPrevKey() {
      return prevKey;
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException("hashCode not designed");
    }

    int weight() {
      int keyWeight = ClassSize.align(prevKey.getSize()) + ClassSize.OBJECT + SizeConstants.SIZEOF_LONG + 4 * (ClassSize.ARRAY + ClassSize.REFERENCE);
      return 2 * SizeConstants.SIZEOF_INT + ClassSize.REFERENCE + ClassSize.OBJECT + keyWeight;
    }
  }

  public BlockIndexEntry seekBlock(Key startKey, ABlockReader cacheBlock) {

    // get a local ref to the index, another thread could change it
    BlockIndexEntry[] blockIndex = this.blockIndex;

    int pos = Arrays.binarySearch(blockIndex, new BlockIndexEntry(startKey));

    int index;

    if (pos < 0) {
      if (pos == -1)
        return null; // less than the first key in index, did not index the first key in block so just return null... code calling this will scan from beginning
                     // of block
      index = (pos * -1) - 2;
    } else {
      // found exact key in index
      index = pos;
      while (index > 0) {
        if (blockIndex[index].getPrevKey().equals(startKey))
          index--;
        else
          break;
      }
    }

    // handle case where multiple keys in block are exactly the same, want to find the earliest key in the index
    while (index - 1 > 0) {
      if (blockIndex[index].getPrevKey().equals(blockIndex[index - 1].getPrevKey()))
        index--;
      else
        break;

    }

    if (index == 0 && blockIndex[index].getPrevKey().equals(startKey))
      return null;

    BlockIndexEntry bie = blockIndex[index];
    cacheBlock.seek(bie.pos);
    return bie;
  }

  private synchronized void buildIndex(int indexEntries, ABlockReader cacheBlock, IndexEntry indexEntry) throws IOException {
    cacheBlock.seek(0);

    RelativeKey rk = new RelativeKey();
    Value val = new Value();

    int interval = indexEntry.getNumEntries() / indexEntries;

    if (interval <= 32)
      return;

    // multiple threads could try to create the index with different sizes, do not replace a large index with a smaller one
    if (this.blockIndex != null && this.blockIndex.length > indexEntries - 1)
      return;

    int count = 0;

    ArrayList<BlockIndexEntry> index = new ArrayList<>(indexEntries - 1);

    while (count < (indexEntry.getNumEntries() - interval + 1)) {

      Key myPrevKey = rk.getKey();
      int pos = cacheBlock.getPosition();
      rk.readFields(cacheBlock);
      val.readFields(cacheBlock);

      if (count > 0 && count % interval == 0) {
        index.add(new BlockIndexEntry(pos, indexEntry.getNumEntries() - count, myPrevKey));
      }

      count++;
    }

    this.blockIndex = index.toArray(new BlockIndexEntry[index.size()]);

    cacheBlock.seek(0);
  }

  BlockIndexEntry[] getIndexEntries() {
    return blockIndex;
  }

  @Override
  public synchronized int weight() {
    int weight = 0;
    if (blockIndex != null) {
      for (BlockIndexEntry blockIndexEntry : blockIndex) {
        weight += blockIndexEntry.weight();
      }
    }

    weight += ClassSize.ATOMIC_INTEGER + ClassSize.OBJECT + 2 * ClassSize.REFERENCE + ClassSize.ARRAY;
    return weight;
  }
}
