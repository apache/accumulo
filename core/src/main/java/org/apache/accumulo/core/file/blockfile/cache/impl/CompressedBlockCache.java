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
package org.apache.accumulo.core.file.blockfile.cache.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.CacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressedBlockCache implements BlockCache {

  private static final Logger log = LoggerFactory.getLogger(CompressedBlockCache.class);
  private final BlockCache delegate;

  public CompressedBlockCache(BlockCache delegate) {
    this.delegate = delegate;
  }

  private static byte[] compress(byte[] uncompressed) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(uncompressed.length / 2 + 16);
        GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
      gzip.write(uncompressed);
      gzip.finish();
      return baos.toByteArray();
    } catch (IOException e) {
      log.warn("Failed to compress block for cache storage", e);
      return null;
    }
  }

  private final byte[] uncompress(byte[] compressed) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(compressed);
        GZIPInputStream gzip = new GZIPInputStream(bais);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(compressed.length * 3)) {
      byte[] buffer = new byte[8192];
      int len;
      while ((len = gzip.read(buffer)) != -1) {
        baos.write(buffer, 0, len);
      }
      return baos.toByteArray();
    } catch (IOException e) {
      log.warn("Failed to decompress block from cache", e);
      return null;
    }
  }

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf) {
    byte[] compressed = compress(buf);
    if (compressed == null) {
      // compression failed, skip caching
      return null;
    }

    if (log.isTraceEnabled()) {
      log.trace("Caching block {} compressed: {} -> {} bytes (ratio {:.2f}", blockName, buf.length,
          compressed.length, (double) buf.length / compressed.length);
    }
    CacheEntry entry = delegate.cacheBlock(blockName, compressed);
    if (entry == null) {
      return null;
    }
    return new DecompressingCacheEntry(entry, buf);
  }

  @Override
  public CacheEntry getBlock(String blockName) {
    CacheEntry entry = delegate.getBlock(blockName);
    if (entry == null) {
      return null;
    }
    byte[] uncompressed = uncompress(entry.getBuffer());
    return new DecompressingCacheEntry(entry, uncompressed);
  }

  @Override
  public CacheEntry getBlock(String blockName, Loader loader) {
    CacheEntry existing = delegate.getBlock(blockName);
    if (existing != null) {
      byte[] uncompressed = uncompress(existing.getBuffer());
      return new DecompressingCacheEntry(existing, uncompressed);
    }

    Loader compressingLoader = new Loader() {
      @Override
      public Map<String,Loader> getDependencies() {
        return loader.getDependencies();
      }

      @Override
      public byte[] load(int maxSize, Map<String,byte[]> dependencies) {
        byte[] uncompressed = loader.load(maxSize, dependencies);
        if (uncompressed == null) {
          return null;
        }
        byte[] compressed = compress(uncompressed);
        return compressed;
      }
    };

    CacheEntry entry = delegate.getBlock(blockName, compressingLoader);
    if (entry == null) {
      return null;
    }
    byte[] uncompressed = uncompress(entry.getBuffer());
    return new DecompressingCacheEntry(entry, uncompressed);
  }

  @Override
  public long getMaxHeapSize() {
    return 0;
  }

  @Override
  public long getMaxSize() {
    return delegate.getMaxSize();
  }

  @Override
  public Stats getStats() {
    return delegate.getStats();
  }

  private static final class DecompressingCacheEntry implements CacheEntry {

    private final CacheEntry compressedEntry;
    private final byte[] uncompressedBuffer;

    public DecompressingCacheEntry(CacheEntry compressedEntry, byte[] uncompressedBuffer) {
      this.compressedEntry = compressedEntry;
      this.uncompressedBuffer = uncompressedBuffer;
    }

    @Override
    public byte[] getBuffer() {
      return uncompressedBuffer;
    }

    @Override
    public <T extends Weighable> T getIndex(Supplier<T> supplier) {
      return null;
    }

    @Override
    public void indexWeightChanged() {
      compressedEntry.indexWeightChanged();
    }
  }
}
