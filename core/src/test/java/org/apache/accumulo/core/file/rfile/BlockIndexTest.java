/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.file.rfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.BlockIndex.BlockIndexEntry;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.IndexEntry;
import org.apache.accumulo.core.spi.cache.CacheEntry;
import org.junit.jupiter.api.Test;

public class BlockIndexTest {

  private static class MyCacheEntry implements CacheEntry {
    Object idx;
    byte[] data;

    MyCacheEntry(byte[] d) {
      this.data = d;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Weighable> T getIndex(Supplier<T> indexSupplier) {
      if (idx == null) {
        idx = indexSupplier.get();
      }
      return (T) idx;
    }

    @Override
    public byte[] getBuffer() {
      return data;
    }

    @Override
    public void indexWeightChanged() {}
  }

  @Test
  public void test1() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    Key prevKey = null;

    int num = 1000;

    for (int i = 0; i < num; i++) {
      Key key = new Key(RFileTest.formatString("", i), "cf1", "cq1");
      new RelativeKey(prevKey, key).write(out);
      new Value().write(out);
      prevKey = key;
    }

    out.close();
    final byte[] data = baos.toByteArray();

    CacheEntry ce = new MyCacheEntry(data);

    CachableBlockFile.CachedBlockRead cacheBlock = new CachableBlockFile.CachedBlockRead(ce, data);
    BlockIndex blockIndex = null;

    for (int i = 0; i < 129; i++) {
      blockIndex = BlockIndex.getIndex(cacheBlock, new IndexEntry(prevKey, num, 0, 0, 0));
    }

    BlockIndexEntry[] indexEntries = blockIndex.getIndexEntries();

    for (int i = 0; i < indexEntries.length; i++) {
      int row = Integer.parseInt(indexEntries[i].getPrevKey().getRowData().toString());

      BlockIndexEntry bie;

      bie =
          blockIndex.seekBlock(new Key(RFileTest.formatString("", row), "cf1", "cq1"), cacheBlock);
      if (i == 0) {
        assertSame(null, bie);
      } else {
        assertSame(indexEntries[i - 1], bie);
      }

      assertSame(bie, blockIndex
          .seekBlock(new Key(RFileTest.formatString("", row - 1), "cf1", "cq1"), cacheBlock));

      bie = blockIndex.seekBlock(new Key(RFileTest.formatString("", row + 1), "cf1", "cq1"),
          cacheBlock);
      assertSame(indexEntries[i], bie);

      RelativeKey rk = new RelativeKey();
      rk.setPrevKey(bie.getPrevKey());
      rk.readFields(cacheBlock);

      assertEquals(rk.getKey(), new Key(RFileTest.formatString("", row + 1), "cf1", "cq1"));

    }
    cacheBlock.close();
  }

  @Test
  public void testSame() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    Key prevKey = null;

    int num = 1000;

    for (int i = 0; i < num; i++) {
      Key key = new Key(RFileTest.formatString("", 1), "cf1", "cq1");
      new RelativeKey(prevKey, key).write(out);
      new Value().write(out);
      prevKey = key;
    }

    for (int i = 0; i < num; i++) {
      Key key = new Key(RFileTest.formatString("", 3), "cf1", "cq1");
      new RelativeKey(prevKey, key).write(out);
      new Value().write(out);
      prevKey = key;
    }

    for (int i = 0; i < num; i++) {
      Key key = new Key(RFileTest.formatString("", 5), "cf1", "cq1");
      new RelativeKey(prevKey, key).write(out);
      new Value().write(out);
      prevKey = key;
    }

    out.close();
    final byte[] data = baos.toByteArray();

    CacheEntry ce = new MyCacheEntry(data);

    CachableBlockFile.CachedBlockRead cacheBlock = new CachableBlockFile.CachedBlockRead(ce, data);
    BlockIndex blockIndex = null;

    for (int i = 0; i < 257; i++) {
      blockIndex = BlockIndex.getIndex(cacheBlock, new IndexEntry(prevKey, num, 0, 0, 0));
    }

    assertSame(null,
        blockIndex.seekBlock(new Key(RFileTest.formatString("", 0), "cf1", "cq1"), cacheBlock));
    assertSame(null,
        blockIndex.seekBlock(new Key(RFileTest.formatString("", 1), "cf1", "cq1"), cacheBlock));

    for (int i = 2; i < 6; i++) {
      Key seekKey = new Key(RFileTest.formatString("", i), "cf1", "cq1");
      BlockIndexEntry bie = blockIndex.seekBlock(seekKey, cacheBlock);

      assertTrue(bie.getPrevKey().compareTo(seekKey) < 0);

      RelativeKey rk = new RelativeKey();
      rk.setPrevKey(bie.getPrevKey());
      rk.readFields(cacheBlock);

      assertTrue(rk.getKey().compareTo(seekKey) <= 0);
    }
    cacheBlock.close();
  }
}
