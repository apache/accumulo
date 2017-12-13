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
package org.apache.accumulo.core.file.blockfile.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.blockfile.ABlockReader;
import org.apache.accumulo.core.file.blockfile.ABlockWriter;
import org.apache.accumulo.core.file.blockfile.BlockFileReader;
import org.apache.accumulo.core.file.blockfile.BlockFileWriter;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache.Loader;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry.Weighbable;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile.Reader.BlockReader;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile.Writer.BlockAppender;
import org.apache.accumulo.core.file.rfile.bcfile.MetaBlockDoesNotExist;
import org.apache.accumulo.core.file.streams.PositionedOutput;
import org.apache.accumulo.core.file.streams.RateLimitedInputStream;
import org.apache.accumulo.core.file.streams.RateLimitedOutputStream;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This is a wrapper class for BCFile that includes a cache for independent caches for datablocks and metadatablocks
 */

public class CachableBlockFile {

  private CachableBlockFile() {}

  private static final Logger log = LoggerFactory.getLogger(CachableBlockFile.class);

  public static class Writer implements BlockFileWriter {
    private BCFile.Writer _bc;
    private BlockWrite _bw;
    private final PositionedOutput fsout;
    private long length = 0;

    public Writer(FileSystem fs, Path fName, String compressAlgor, RateLimiter writeLimiter, Configuration conf, AccumuloConfiguration accumuloConfiguration)
        throws IOException {
      this(new RateLimitedOutputStream(fs.create(fName), writeLimiter), compressAlgor, conf, accumuloConfiguration);
    }

    public <OutputStreamType extends OutputStream & PositionedOutput> Writer(OutputStreamType fsout, String compressAlgor, Configuration conf,
        AccumuloConfiguration accumuloConfiguration) throws IOException {
      this.fsout = fsout;
      init(fsout, compressAlgor, conf, accumuloConfiguration);
    }

    private <OutputStreamT extends OutputStream & PositionedOutput> void init(OutputStreamT fsout, String compressAlgor, Configuration conf,
        AccumuloConfiguration accumuloConfiguration) throws IOException {
      _bc = new BCFile.Writer(fsout, compressAlgor, conf, false, accumuloConfiguration);
    }

    @Override
    public ABlockWriter prepareMetaBlock(String name) throws IOException {
      _bw = new BlockWrite(_bc.prepareMetaBlock(name));
      return _bw;
    }

    @Override
    public ABlockWriter prepareDataBlock() throws IOException {
      _bw = new BlockWrite(_bc.prepareDataBlock());
      return _bw;
    }

    @Override
    public void close() throws IOException {

      _bw.close();
      _bc.close();

      length = this.fsout.position();
      ((OutputStream) this.fsout).close();
    }

    @Override
    public long getLength() throws IOException {
      return length;
    }

  }

  public static class BlockWrite extends DataOutputStream implements ABlockWriter {
    BlockAppender _ba;

    public BlockWrite(BlockAppender ba) {
      super(ba);
      this._ba = ba;
    }

    @Override
    public long getCompressedSize() throws IOException {
      return _ba.getCompressedSize();
    }

    @Override
    public long getRawSize() throws IOException {
      return _ba.getRawSize();
    }

    @Override
    public void close() throws IOException {

      _ba.close();
    }

    @Override
    public long getStartPos() throws IOException {
      return _ba.getStartPos();
    }

  }

  private static interface IoeSupplier<T> {
    T get() throws IOException;
  }

  /**
   * Class wraps the BCFile reader.
   */
  public static class Reader implements BlockFileReader {
    private final RateLimiter readLimiter;
    // private BCFile.Reader _bc;
    private final String cacheId;
    private final BlockCache _dCache;
    private final BlockCache _iCache;
    private volatile InputStream fin = null;
    private boolean closed = false;
    private final Configuration conf;
    private final AccumuloConfiguration accumuloConfiguration;

    private final IoeSupplier<InputStream> inputSupplier;
    private final IoeSupplier<Long> lengthSupplier;
    private final AtomicReference<BCFile.Reader> bcfr = new AtomicReference<>();

    private static final String ROOT_BLOCK_NAME = "!RootData";

    // ACCUMULO-4716 - Define MAX_ARRAY_SIZE smaller than Integer.MAX_VALUE to prevent possible OutOfMemory
    // errors when allocating arrays - described in stackoverflow post: https://stackoverflow.com/a/8381338
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private BCFile.Reader getBCFile(byte[] serializedMetadata) throws IOException {

      BCFile.Reader reader = bcfr.get();
      if (reader == null) {
        RateLimitedInputStream fsIn = new RateLimitedInputStream((InputStream & Seekable) inputSupplier.get(), readLimiter);
        BCFile.Reader tmpReader;
        if (serializedMetadata == null) {
          tmpReader = new BCFile.Reader(fsIn, lengthSupplier.get(), conf, accumuloConfiguration);
        } else {
          tmpReader = new BCFile.Reader(serializedMetadata, fsIn, conf, accumuloConfiguration);
        }

        if (!bcfr.compareAndSet(null, tmpReader)) {
          fsIn.close();
          tmpReader.close();
          return bcfr.get();
        } else {
          fin = fsIn;
          return tmpReader;
        }
      }

      return reader;
    }

    private BCFile.Reader getBCFile() throws IOException {
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
        super();
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

          byte b[] = null;
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

    private Reader(String cacheId, IoeSupplier<InputStream> inputSupplier, IoeSupplier<Long> lenghtSupplier, BlockCache data, BlockCache index,
        RateLimiter readLimiter, Configuration conf, AccumuloConfiguration accumuloConfiguration) {
      Preconditions.checkArgument(cacheId != null || (data == null && index == null));
      this.cacheId = cacheId;
      this.inputSupplier = inputSupplier;
      this.lengthSupplier = lenghtSupplier;
      this._dCache = data;
      this._iCache = index;
      this.readLimiter = readLimiter;
      this.conf = conf;
      this.accumuloConfiguration = accumuloConfiguration;
    }

    public Reader(FileSystem fs, Path dataFile, Configuration conf, BlockCache data, BlockCache index, AccumuloConfiguration accumuloConfiguration)
        throws IOException {
      this(fs, dataFile, conf, data, index, null, accumuloConfiguration);
    }

    public Reader(FileSystem fs, Path dataFile, Configuration conf, BlockCache data, BlockCache index, RateLimiter readLimiter,
        AccumuloConfiguration accumuloConfiguration) throws IOException {
      this(dataFile.toString(), () -> fs.open(dataFile), () -> fs.getFileStatus(dataFile).getLen(), data, index, readLimiter, conf, accumuloConfiguration);
    }

    public <InputStreamType extends InputStream & Seekable> Reader(String cacheId, InputStreamType fsin, long len, Configuration conf, BlockCache data,
        BlockCache index, AccumuloConfiguration accumuloConfiguration) throws IOException {
      this(cacheId, () -> fsin, () -> len, data, index, null, conf, accumuloConfiguration);
    }

    public <InputStreamType extends InputStream & Seekable> Reader(InputStreamType fsin, long len, Configuration conf,
        AccumuloConfiguration accumuloConfiguration) throws IOException {
      this(null, () -> fsin, () -> len, null, null, null, conf, accumuloConfiguration);
    }

    /**
     * It is intended that once the BlockRead object is returned to the caller, that the caller will read the entire block and then call close on the BlockRead
     * class.
     */
    @Override
    public BlockRead getMetaBlock(String blockName) throws IOException {
      if (_iCache != null) {
        String _lookup = this.cacheId + "M" + blockName;
        try {
          CacheEntry ce = _iCache.getBlock(_lookup, new MetaBlockLoader(blockName));
          if (ce != null) {
            return new CachedBlockRead(ce, ce.getBuffer());
          }
        } catch (UncheckedIOException uioe) {
          if (uioe.getCause() instanceof MetaBlockDoesNotExist) {
            // When a block does not exists, its expected that MetaBlockDoesNotExist is thrown. However do not want to throw cause, because stack trace info
            // would be lost. So rewrap and throw ino rder to preserve full stack trace.
            throw new MetaBlockDoesNotExist(uioe);
          }
          throw uioe;
        }
      }

      BlockReader _currBlock = getBCFile(null).getMetaBlock(blockName);
      return new BlockRead(_currBlock, _currBlock.getRawSize());
    }

    @Override
    public ABlockReader getMetaBlock(long offset, long compressedSize, long rawSize) throws IOException {
      if (_iCache != null) {
        String _lookup = this.cacheId + "R" + offset;
        CacheEntry ce = _iCache.getBlock(_lookup, new RawBlockLoader(offset, compressedSize, rawSize, true));
        if (ce != null) {
          return new CachedBlockRead(ce, ce.getBuffer());
        }
      }

      BlockReader _currBlock = getBCFile(null).getDataBlock(offset, compressedSize, rawSize);
      return new BlockRead(_currBlock, _currBlock.getRawSize());
    }

    /**
     * It is intended that once the BlockRead object is returned to the caller, that the caller will read the entire block and then call close on the BlockRead
     * class.
     *
     * NOTE: In the case of multi-read threads: This method can do redundant work where an entry is read from disk and other threads check the cache before it
     * has been inserted.
     */

    @Override
    public BlockRead getDataBlock(int blockIndex) throws IOException {
      if (_dCache != null) {
        String _lookup = this.cacheId + "O" + blockIndex;
        CacheEntry ce = _dCache.getBlock(_lookup, new OffsetBlockLoader(blockIndex, false));
        if (ce != null) {
          return new CachedBlockRead(ce, ce.getBuffer());
        }
      }

      BlockReader _currBlock = getBCFile().getDataBlock(blockIndex);
      return new BlockRead(_currBlock, _currBlock.getRawSize());
    }

    @Override
    public ABlockReader getDataBlock(long offset, long compressedSize, long rawSize) throws IOException {
      if (_dCache != null) {
        String _lookup = this.cacheId + "R" + offset;
        CacheEntry ce = _dCache.getBlock(_lookup, new RawBlockLoader(offset, compressedSize, rawSize, false));
        if (ce != null) {
          return new CachedBlockRead(ce, ce.getBuffer());
        }
      }

      BlockReader _currBlock = getBCFile().getDataBlock(offset, compressedSize, rawSize);
      return new BlockRead(_currBlock, _currBlock.getRawSize());
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed)
        return;

      closed = true;

      BCFile.Reader reader = bcfr.get();
      if (reader != null)
        reader.close();

      if (fin != null) {
        // synchronize on the FSDataInputStream to ensure thread safety with the BoundedRangeFileInputStream
        synchronized (fin) {
          fin.close();
        }
      }
    }

  }

  public static class CachedBlockRead extends BlockRead {
    private SeekableByteArrayInputStream seekableInput;
    private final CacheEntry cb;

    public CachedBlockRead(CacheEntry cb, byte buf[]) {
      this(new SeekableByteArrayInputStream(buf), buf.length, cb);
    }

    private CachedBlockRead(SeekableByteArrayInputStream seekableInput, long size, CacheEntry cb) {
      super(seekableInput, size);
      this.seekableInput = seekableInput;
      this.cb = cb;
    }

    @Override
    public void seek(int position) {
      seekableInput.seek(position);
    }

    @Override
    public int getPosition() {
      return seekableInput.getPosition();
    }

    @Override
    public boolean isIndexable() {
      return true;
    }

    @Override
    public byte[] getBuffer() {
      return seekableInput.getBuffer();
    }

    @Override
    public <T extends Weighbable> T getIndex(Supplier<T> indexSupplier) {
      return cb.getIndex(indexSupplier);
    }

    @Override
    public void indexWeightChanged() {
      cb.indexWeightChanged();
    }
  }

  /**
   *
   * Class provides functionality to read one block from the underlying BCFile Since We are caching blocks in the Reader class as bytearrays, this class will
   * wrap a DataInputStream(ByteArrayStream(cachedBlock)).
   *
   *
   */
  public static class BlockRead extends DataInputStream implements ABlockReader {

    public BlockRead(InputStream in, long size) {
      super(in);
    }

    /**
     * It is intended that the caller of this method will close the stream we also only intend that this be called once per BlockRead. This method is provide
     * for methods up stream that expect to receive a DataInputStream object.
     */
    @Override
    public DataInputStream getStream() throws IOException {
      return this;
    }

    @Override
    public boolean isIndexable() {
      return false;
    }

    @Override
    public void seek(int position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getPosition() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Weighbable> T getIndex(Supplier<T> clazz) {
      throw new UnsupportedOperationException();
    }

    /**
     * The byte array returned by this method is only for read optimizations, it should not be modified.
     */
    @Override
    public byte[] getBuffer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void indexWeightChanged() {
      throw new UnsupportedOperationException();
    }

  }
}
