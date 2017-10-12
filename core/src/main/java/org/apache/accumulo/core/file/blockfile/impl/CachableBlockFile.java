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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.SoftReference;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.blockfile.ABlockReader;
import org.apache.accumulo.core.file.blockfile.ABlockWriter;
import org.apache.accumulo.core.file.blockfile.BlockFileReader;
import org.apache.accumulo.core.file.blockfile.BlockFileWriter;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.CacheEntry;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile.Reader.BlockReader;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile.Writer.BlockAppender;
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

/**
 *
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

  /**
   *
   *
   * Class wraps the BCFile reader.
   *
   */
  public static class Reader implements BlockFileReader {
    private final RateLimiter readLimiter;
    private BCFile.Reader _bc;
    private String fileName = "not_available";
    private BlockCache _dCache = null;
    private BlockCache _iCache = null;
    private InputStream fin = null;
    private FileSystem fs;
    private Configuration conf;
    private boolean closed = false;
    private AccumuloConfiguration accumuloConfiguration = null;

    // ACCUMULO-4716 - Define MAX_ARRAY_SIZE smaller than Integer.MAX_VALUE to prevent possible OutOfMemory
    // errors when allocating arrays - described in stackoverflow post: https://stackoverflow.com/a/8381338
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private interface BlockLoader {
      BlockReader get() throws IOException;

      String getInfo();
    }

    private class OffsetBlockLoader implements BlockLoader {

      private int blockIndex;

      OffsetBlockLoader(int blockIndex) {
        this.blockIndex = blockIndex;
      }

      @Override
      public BlockReader get() throws IOException {
        return getBCFile(accumuloConfiguration).getDataBlock(blockIndex);
      }

      @Override
      public String getInfo() {
        return "" + blockIndex;
      }

    }

    private class RawBlockLoader implements BlockLoader {

      private long offset;
      private long compressedSize;
      private long rawSize;

      RawBlockLoader(long offset, long compressedSize, long rawSize) {
        this.offset = offset;
        this.compressedSize = compressedSize;
        this.rawSize = rawSize;
      }

      @Override
      public BlockReader get() throws IOException {
        return getBCFile(accumuloConfiguration).getDataBlock(offset, compressedSize, rawSize);
      }

      @Override
      public String getInfo() {
        return "" + offset + "," + compressedSize + "," + rawSize;
      }
    }

    private class MetaBlockLoader implements BlockLoader {

      private String name;
      private AccumuloConfiguration accumuloConfiguration;

      MetaBlockLoader(String name, AccumuloConfiguration accumuloConfiguration) {
        this.name = name;
        this.accumuloConfiguration = accumuloConfiguration;
      }

      @Override
      public BlockReader get() throws IOException {
        return getBCFile(accumuloConfiguration).getMetaBlock(name);
      }

      @Override
      public String getInfo() {
        return name;
      }
    }

    public Reader(FileSystem fs, Path dataFile, Configuration conf, BlockCache data, BlockCache index, AccumuloConfiguration accumuloConfiguration)
        throws IOException {
      this(fs, dataFile, conf, data, index, null, accumuloConfiguration);
    }

    public Reader(FileSystem fs, Path dataFile, Configuration conf, BlockCache data, BlockCache index, RateLimiter readLimiter,
        AccumuloConfiguration accumuloConfiguration) throws IOException {

      /*
       * Grab path create input stream grab len create file
       */

      fileName = dataFile.toString();
      this._dCache = data;
      this._iCache = index;
      this.fs = fs;
      this.conf = conf;
      this.accumuloConfiguration = accumuloConfiguration;
      this.readLimiter = readLimiter;
    }

    public <InputStreamType extends InputStream & Seekable> Reader(InputStreamType fsin, long len, Configuration conf, BlockCache data, BlockCache index,
        AccumuloConfiguration accumuloConfiguration) throws IOException {
      this._dCache = data;
      this._iCache = index;
      this.readLimiter = null;
      init(fsin, len, conf, accumuloConfiguration);
    }

    public <InputStreamType extends InputStream & Seekable> Reader(InputStreamType fsin, long len, Configuration conf,
        AccumuloConfiguration accumuloConfiguration) throws IOException {
      this.readLimiter = null;
      init(fsin, len, conf, accumuloConfiguration);
    }

    private <InputStreamT extends InputStream & Seekable> void init(InputStreamT fsin, long len, Configuration conf, AccumuloConfiguration accumuloConfiguration)
        throws IOException {
      this._bc = new BCFile.Reader(this, fsin, len, conf, accumuloConfiguration);
    }

    private synchronized BCFile.Reader getBCFile(AccumuloConfiguration accumuloConfiguration) throws IOException {
      if (closed)
        throw new IllegalStateException("File " + fileName + " is closed");

      if (_bc == null) {
        // lazily open file if needed
        Path path = new Path(fileName);
        RateLimitedInputStream fsIn = new RateLimitedInputStream(fs.open(path), this.readLimiter);
        fin = fsIn;
        init(fsIn, fs.getFileStatus(path).getLen(), conf, accumuloConfiguration);
      }

      return _bc;
    }

    public BlockRead getCachedMetaBlock(String blockName) throws IOException {
      String _lookup = fileName + "M" + blockName;

      if (_iCache != null) {
        CacheEntry cacheEntry = _iCache.getBlock(_lookup);

        if (cacheEntry != null) {
          return new CachedBlockRead(cacheEntry, cacheEntry.getBuffer());
        }

      }

      return null;
    }

    public BlockRead cacheMetaBlock(String blockName, BlockReader _currBlock) throws IOException {
      String _lookup = fileName + "M" + blockName;
      return cacheBlock(_lookup, _iCache, _currBlock, blockName);
    }

    public void cacheMetaBlock(String blockName, byte[] b) {

      if (_iCache == null)
        return;

      String _lookup = fileName + "M" + blockName;
      try {
        _iCache.cacheBlock(_lookup, b);
      } catch (Exception e) {
        log.warn("Already cached block: " + _lookup, e);
      }
    }

    private BlockRead getBlock(String _lookup, BlockCache cache, BlockLoader loader) throws IOException {

      BlockReader _currBlock;

      if (cache != null) {
        CacheEntry cb = null;
        cb = cache.getBlock(_lookup);

        if (cb != null) {
          return new CachedBlockRead(cb, cb.getBuffer());
        }

      }
      /**
       * grab the currBlock at this point the block is still in the data stream
       *
       */
      _currBlock = loader.get();

      /**
       * If the block is bigger than the cache just return the stream
       */
      return cacheBlock(_lookup, cache, _currBlock, loader.getInfo());

    }

    private BlockRead cacheBlock(String _lookup, BlockCache cache, BlockReader _currBlock, String block) throws IOException {

      if ((cache == null) || (_currBlock.getRawSize() > Math.min(cache.getMaxSize(), MAX_ARRAY_SIZE))) {
        return new BlockRead(_currBlock, _currBlock.getRawSize());
      } else {

        /**
         * Try to fully read block for meta data if error try to close file
         *
         */
        byte b[] = null;
        try {
          b = new byte[(int) _currBlock.getRawSize()];
          _currBlock.readFully(b);
        } catch (IOException e) {
          log.debug("Error full blockRead for file " + fileName + " for block " + block, e);
          throw e;
        } finally {
          _currBlock.close();
        }

        CacheEntry ce = null;
        try {
          ce = cache.cacheBlock(_lookup, b);
        } catch (Exception e) {
          log.warn("Already cached block: " + _lookup, e);
        }

        if (ce == null)
          return new BlockRead(new DataInputStream(new ByteArrayInputStream(b)), b.length);
        else
          return new CachedBlockRead(ce, ce.getBuffer());

      }
    }

    /**
     * It is intended that once the BlockRead object is returned to the caller, that the caller will read the entire block and then call close on the BlockRead
     * class.
     *
     * NOTE: In the case of multi-read threads: This method can do redundant work where an entry is read from disk and other threads check the cache before it
     * has been inserted.
     */
    @Override
    public BlockRead getMetaBlock(String blockName) throws IOException {
      String _lookup = this.fileName + "M" + blockName;
      return getBlock(_lookup, _iCache, new MetaBlockLoader(blockName, accumuloConfiguration));
    }

    @Override
    public ABlockReader getMetaBlock(long offset, long compressedSize, long rawSize) throws IOException {
      String _lookup = this.fileName + "R" + offset;
      return getBlock(_lookup, _iCache, new RawBlockLoader(offset, compressedSize, rawSize));
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
      String _lookup = this.fileName + "O" + blockIndex;
      return getBlock(_lookup, _dCache, new OffsetBlockLoader(blockIndex));

    }

    @Override
    public ABlockReader getDataBlock(long offset, long compressedSize, long rawSize) throws IOException {
      String _lookup = this.fileName + "R" + offset;
      return getBlock(_lookup, _dCache, new RawBlockLoader(offset, compressedSize, rawSize));
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed)
        return;

      closed = true;

      if (_bc != null)
        _bc.close();

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
    @SuppressWarnings("unchecked")
    public <T> T getIndex(Class<T> clazz) {
      T bi = null;
      synchronized (cb) {
        SoftReference<T> softRef = (SoftReference<T>) cb.getIndex();
        if (softRef != null)
          bi = softRef.get();

        if (bi == null) {
          try {
            bi = clazz.newInstance();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          cb.setIndex(new SoftReference<>(bi));
        }
      }

      return bi;
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
    public <T> T getIndex(Class<T> clazz) {
      throw new UnsupportedOperationException();
    }

    /**
     * The byte array returned by this method is only for read optimizations, it should not be modified.
     */
    @Override
    public byte[] getBuffer() {
      throw new UnsupportedOperationException();
    }

  }
}
