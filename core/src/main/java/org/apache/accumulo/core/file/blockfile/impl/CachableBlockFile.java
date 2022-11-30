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
package org.apache.accumulo.core.file.blockfile.impl;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.accumulo.core.file.rfile.BlockIndex;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile.Reader.BlockReader;
import org.apache.accumulo.core.file.rfile.bcfile.MetaBlockDoesNotExist;
import org.apache.accumulo.core.file.streams.RateLimitedInputStream;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.BlockCache.Loader;
import org.apache.accumulo.core.spi.cache.CacheEntry;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;

/**
 * This is a wrapper class for BCFile that includes a cache for independent caches for datablocks
 * and metadatablocks
 */
public class CachableBlockFile {

  private CachableBlockFile() {}

  private static final Logger log = LoggerFactory.getLogger(CachableBlockFile.class);

  private interface IoeSupplier<T> {
    T get() throws IOException;
  }

  public static String pathToCacheId(Path p) {
    return p.toString();
  }

  public static class CachableBuilder {
    String cacheId = null;
    IoeSupplier<InputStream> inputSupplier = null;
    IoeSupplier<Long> lengthSupplier = null;
    Cache<String,Long> fileLenCache = null;
    volatile CacheProvider cacheProvider = CacheProvider.NULL_PROVIDER;
    RateLimiter readLimiter = null;
    Configuration hadoopConf = null;
    CryptoService cryptoService = null;

    public CachableBuilder conf(Configuration hadoopConf) {
      this.hadoopConf = hadoopConf;
      return this;
    }

    public CachableBuilder fsPath(FileSystem fs, Path dataFile) {
      return fsPath(fs, dataFile, false);
    }

    public CachableBuilder fsPath(FileSystem fs, Path dataFile, boolean dropCacheBehind) {
      this.cacheId = pathToCacheId(dataFile);
      this.inputSupplier = () -> {
        FSDataInputStream is = fs.open(dataFile);
        if (dropCacheBehind) {
          // Tell the DataNode that the write ahead log does not need to be cached in the OS page
          // cache
          try {
            is.setDropBehind(Boolean.TRUE);
            log.trace("Called setDropBehind(TRUE) for stream reading file {}", dataFile);
          } catch (UnsupportedOperationException e) {
            log.debug("setDropBehind not enabled for wal file: {}", dataFile);
          } catch (IOException e) {
            log.debug("IOException setting drop behind for file: {}, msg: {}", dataFile,
                e.getMessage());
          }
        }
        return is;
      };
      this.lengthSupplier = () -> fs.getFileStatus(dataFile).getLen();
      return this;
    }

    public CachableBuilder input(InputStream is, String cacheId) {
      this.cacheId = cacheId;
      this.inputSupplier = () -> is;
      return this;
    }

    public CachableBuilder length(long len) {
      this.lengthSupplier = () -> len;
      return this;
    }

    public CachableBuilder fileLen(Cache<String,Long> cache) {
      this.fileLenCache = cache;
      return this;
    }

    public CachableBuilder cacheProvider(CacheProvider cacheProvider) {
      this.cacheProvider = cacheProvider;
      return this;
    }

    public CachableBuilder readLimiter(RateLimiter readLimiter) {
      this.readLimiter = readLimiter;
      return this;
    }

    public CachableBuilder cryptoService(CryptoService cryptoService) {
      this.cryptoService = cryptoService;
      return this;
    }
  }

  /**
   * Class wraps the BCFile reader.
   */
  public static class Reader implements Closeable {
    private final RateLimiter readLimiter;
    // private BCFile.Reader _bc;
    private final String cacheId;
    private CacheProvider cacheProvider;
    private Cache<String,Long> fileLenCache = null;
    private volatile InputStream fin = null;
    private boolean closed = false;
    private final Configuration conf;
    private final CryptoService cryptoService;

    private final IoeSupplier<InputStream> inputSupplier;
    private final IoeSupplier<Long> lengthSupplier;
    private final AtomicReference<BCFile.Reader> bcfr = new AtomicReference<>();

    private static final String ROOT_BLOCK_NAME = "!RootData";

    // ACCUMULO-4716 - Define MAX_ARRAY_SIZE smaller than Integer.MAX_VALUE to prevent possible
    // OutOfMemory
    // errors when allocating arrays - described in stackoverflow post:
    // https://stackoverflow.com/a/8381338
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private long getCachedFileLen() throws IOException {
      try {
        return fileLenCache.get(cacheId, lengthSupplier::get);
      } catch (ExecutionException e) {
        throw new IOException("Failed to get " + cacheId + " len from cache ", e);
      }
    }

    private BCFile.Reader getBCFile(byte[] serializedMetadata) throws IOException {

      BCFile.Reader reader = bcfr.get();
      if (reader == null) {
        RateLimitedInputStream fsIn =
            new RateLimitedInputStream((InputStream & Seekable) inputSupplier.get(), readLimiter);
        BCFile.Reader tmpReader = null;
        if (serializedMetadata == null) {
          if (fileLenCache == null) {
            tmpReader = new BCFile.Reader(fsIn, lengthSupplier.get(), conf, cryptoService);
          } else {
            long len = getCachedFileLen();
            try {
              tmpReader = new BCFile.Reader(fsIn, len, conf, cryptoService);
            } catch (Exception e) {
              log.debug("Failed to open {}, clearing file length cache and retrying", cacheId, e);
              fileLenCache.invalidate(cacheId);
            }

            if (tmpReader == null) {
              len = getCachedFileLen();
              tmpReader = new BCFile.Reader(fsIn, len, conf, cryptoService);
            }
          }
        } else {
          tmpReader = new BCFile.Reader(serializedMetadata, fsIn, conf, cryptoService);
        }

        if (bcfr.compareAndSet(null, tmpReader)) {
          fin = fsIn;
          return tmpReader;
        } else {
          fsIn.close();
          tmpReader.close();
          return bcfr.get();
        }
      }

      return reader;
    }

    private BCFile.Reader getBCFile() throws IOException {
      BlockCache _iCache = cacheProvider.getIndexCache();
      if (_iCache != null) {
        CacheEntry mce = _iCache.getBlock(cacheId + ROOT_BLOCK_NAME, new BCFileLoader());
        if (mce != null) {
          return getBCFile(mce.getBuffer());
        }
      }

      return getBCFile(null);
    }

    private class BCFileLoader implements Loader {

      @Override
      public Map<String,Loader> getDependencies() {
        return Collections.emptyMap();
      }

      @Override
      public byte[] load(int maxSize, Map<String,byte[]> dependencies) {
        try {
          return getBCFile(null).serializeMetadata(maxSize);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    private class RawBlockLoader extends BaseBlockLoader {
      private long offset;
      private long compressedSize;
      private long rawSize;

      private RawBlockLoader(long offset, long compressedSize, long rawSize, boolean loadingMeta) {
        super(loadingMeta);
        this.offset = offset;
        this.compressedSize = compressedSize;
        this.rawSize = rawSize;
      }

      @Override
      BlockReader getBlockReader(int maxSize, BCFile.Reader bcfr) throws IOException {
        if (rawSize > Math.min(maxSize, MAX_ARRAY_SIZE)) {
          return null;
        }
        return bcfr.getDataBlock(offset, compressedSize, rawSize);
      }

      @Override
      String getBlockId() {
        return "raw-(" + offset + "," + compressedSize + "," + rawSize + ")";
      }
    }

    private class OffsetBlockLoader extends BaseBlockLoader {
      private int blockIndex;

      private OffsetBlockLoader(int blockIndex, boolean loadingMeta) {
        super(loadingMeta);
        this.blockIndex = blockIndex;
      }

      @Override
      BlockReader getBlockReader(int maxSize, BCFile.Reader bcfr) throws IOException {
        if (bcfr.getDataBlockRawSize(blockIndex) > Math.min(maxSize, MAX_ARRAY_SIZE)) {
          return null;
        }
        return bcfr.getDataBlock(blockIndex);
      }

      @Override
      String getBlockId() {
        return "bi-" + blockIndex;
      }
    }

    private class MetaBlockLoader extends BaseBlockLoader {
      String blockName;

      MetaBlockLoader(String blockName) {
        super(true);
        this.blockName = blockName;
      }

      @Override
      BlockReader getBlockReader(int maxSize, BCFile.Reader bcfr) throws IOException {
        if (bcfr.getMetaBlockRawSize(blockName) > Math.min(maxSize, MAX_ARRAY_SIZE)) {
          return null;
        }
        return bcfr.getMetaBlock(blockName);
      }

      @Override
      String getBlockId() {
        return "meta-" + blockName;
      }
    }

    private abstract class BaseBlockLoader implements Loader {

      abstract BlockReader getBlockReader(int maxSize, BCFile.Reader bcfr) throws IOException;

      abstract String getBlockId();

      private boolean loadingMetaBlock;

      public BaseBlockLoader(boolean loadingMetaBlock) {
        this.loadingMetaBlock = loadingMetaBlock;
      }

      @Override
      public Map<String,Loader> getDependencies() {
        if (bcfr.get() == null && loadingMetaBlock) {
          String _lookup = cacheId + ROOT_BLOCK_NAME;
          return Collections.singletonMap(_lookup, new BCFileLoader());
        }
        return Collections.emptyMap();
      }

      @Override
      public byte[] load(int maxSize, Map<String,byte[]> dependencies) {

        try {
          BCFile.Reader reader = bcfr.get();
          if (reader == null) {
            if (loadingMetaBlock) {
              byte[] serializedMetadata = dependencies.get(cacheId + ROOT_BLOCK_NAME);
              reader = getBCFile(serializedMetadata);
            } else {
              reader = getBCFile();
            }
          }

          BlockReader _currBlock = getBlockReader(maxSize, reader);
          if (_currBlock == null) {
            return null;
          }

          byte[] b = null;
          try {
            b = new byte[(int) _currBlock.getRawSize()];
            _currBlock.readFully(b);
          } catch (IOException e) {
            log.debug("Error full blockRead for file " + cacheId + " for block " + getBlockId(), e);
            throw new UncheckedIOException(e);
          } finally {
            _currBlock.close();
          }

          return b;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    public Reader(CachableBuilder b) {
      this.cacheId = Objects.requireNonNull(b.cacheId);
      this.inputSupplier = b.inputSupplier;
      this.lengthSupplier = b.lengthSupplier;
      this.fileLenCache = b.fileLenCache;
      this.cacheProvider = b.cacheProvider;
      this.readLimiter = b.readLimiter;
      this.conf = b.hadoopConf;
      this.cryptoService = Objects.requireNonNull(b.cryptoService);
    }

    /**
     * It is intended that once the BlockRead object is returned to the caller, that the caller will
     * read the entire block and then call close on the BlockRead class.
     */
    public CachedBlockRead getMetaBlock(String blockName) throws IOException {
      BlockCache _iCache = cacheProvider.getIndexCache();
      if (_iCache != null) {
        String _lookup = this.cacheId + "M" + blockName;
        try {
          CacheEntry ce = _iCache.getBlock(_lookup, new MetaBlockLoader(blockName));
          if (ce != null) {
            return new CachedBlockRead(ce, ce.getBuffer());
          }
        } catch (UncheckedIOException uioe) {
          if (uioe.getCause() instanceof MetaBlockDoesNotExist) {
            // When a block does not exists, its expected that MetaBlockDoesNotExist is thrown.
            // However do not want to throw cause, because stack trace info
            // would be lost. So rewrap and throw ino rder to preserve full stack trace.
            throw new MetaBlockDoesNotExist(uioe);
          }
          throw uioe;
        }
      }

      BlockReader _currBlock = getBCFile(null).getMetaBlock(blockName);
      return new CachedBlockRead(_currBlock);
    }

    public CachedBlockRead getMetaBlock(long offset, long compressedSize, long rawSize)
        throws IOException {
      BlockCache _iCache = cacheProvider.getIndexCache();
      if (_iCache != null) {
        String _lookup = this.cacheId + "R" + offset;
        CacheEntry ce =
            _iCache.getBlock(_lookup, new RawBlockLoader(offset, compressedSize, rawSize, true));
        if (ce != null) {
          return new CachedBlockRead(ce, ce.getBuffer());
        }
      }

      BlockReader _currBlock = getBCFile(null).getDataBlock(offset, compressedSize, rawSize);
      return new CachedBlockRead(_currBlock);
    }

    /**
     * It is intended that once the BlockRead object is returned to the caller, that the caller will
     * read the entire block and then call close on the BlockRead class.
     *
     * NOTE: In the case of multi-read threads: This method can do redundant work where an entry is
     * read from disk and other threads check the cache before it has been inserted.
     */

    public CachedBlockRead getDataBlock(int blockIndex) throws IOException {
      BlockCache _dCache = cacheProvider.getDataCache();
      if (_dCache != null) {
        String _lookup = this.cacheId + "O" + blockIndex;
        CacheEntry ce = _dCache.getBlock(_lookup, new OffsetBlockLoader(blockIndex, false));
        if (ce != null) {
          return new CachedBlockRead(ce, ce.getBuffer());
        }
      }

      BlockReader _currBlock = getBCFile().getDataBlock(blockIndex);
      return new CachedBlockRead(_currBlock);
    }

    public CachedBlockRead getDataBlock(long offset, long compressedSize, long rawSize)
        throws IOException {
      BlockCache _dCache = cacheProvider.getDataCache();
      if (_dCache != null) {
        String _lookup = this.cacheId + "R" + offset;
        CacheEntry ce =
            _dCache.getBlock(_lookup, new RawBlockLoader(offset, compressedSize, rawSize, false));
        if (ce != null) {
          return new CachedBlockRead(ce, ce.getBuffer());
        }
      }

      BlockReader _currBlock = getBCFile().getDataBlock(offset, compressedSize, rawSize);
      return new CachedBlockRead(_currBlock);
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed) {
        return;
      }

      closed = true;

      BCFile.Reader reader = bcfr.get();
      if (reader != null) {
        reader.close();
      }

      if (fin != null) {
        // synchronize on the FSDataInputStream to ensure thread safety with the
        // BoundedRangeFileInputStream
        synchronized (fin) {
          fin.close();
        }
      }
    }

    public void setCacheProvider(CacheProvider cacheProvider) {
      this.cacheProvider = cacheProvider;
    }

  }

  public static class CachedBlockRead extends DataInputStream {
    private SeekableByteArrayInputStream seekableInput;
    private final CacheEntry cb;
    boolean indexable;

    public CachedBlockRead(InputStream in) {
      super(in);
      cb = null;
      seekableInput = null;
      indexable = false;
    }

    public CachedBlockRead(CacheEntry cb, byte[] buf) {
      this(new SeekableByteArrayInputStream(buf), cb);
    }

    private CachedBlockRead(SeekableByteArrayInputStream seekableInput, CacheEntry cb) {
      super(seekableInput);
      this.seekableInput = seekableInput;
      this.cb = cb;
      indexable = true;
    }

    public void seek(int position) {
      seekableInput.seek(position);
    }

    public int getPosition() {
      return seekableInput.getPosition();
    }

    public boolean isIndexable() {
      return indexable;
    }

    public byte[] getBuffer() {
      return seekableInput.getBuffer();
    }

    public BlockIndex getIndex(Supplier<BlockIndex> indexSupplier) {
      return cb.getIndex(indexSupplier);
    }

    public void indexWeightChanged() {
      cb.indexWeightChanged();
    }
  }
}
