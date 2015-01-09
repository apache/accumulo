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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/***
 *
 * This is a wrapper class for BCFile that includes a cache for independent caches for datablocks and metadatablocks
 */

public class CachableBlockFile {

  private CachableBlockFile() {};

  private static final Logger log = Logger.getLogger(CachableBlockFile.class);

  public static class Writer implements BlockFileWriter {
    private BCFile.Writer _bc;
    private BlockWrite _bw;
    private FSDataOutputStream fsout = null;

    public Writer(FileSystem fs, Path fName, String compressAlgor, Configuration conf, AccumuloConfiguration accumuloConfiguration) throws IOException {
      this.fsout = fs.create(fName);
      init(fsout, compressAlgor, conf, accumuloConfiguration);
    }

    public Writer(FSDataOutputStream fsout, String compressAlgor, Configuration conf, AccumuloConfiguration accumuloConfiguration) throws IOException {
      this.fsout = fsout;
      init(fsout, compressAlgor, conf, accumuloConfiguration);
    }

    private void init(FSDataOutputStream fsout, String compressAlgor, Configuration conf, AccumuloConfiguration accumuloConfiguration) throws IOException {
      _bc = new BCFile.Writer(fsout, compressAlgor, conf, false, accumuloConfiguration);
    }

    public ABlockWriter prepareMetaBlock(String name) throws IOException {
      _bw = new BlockWrite(_bc.prepareMetaBlock(name));
      return _bw;
    }

    public ABlockWriter prepareMetaBlock(String name, String compressionName) throws IOException {
      _bw = new BlockWrite(_bc.prepareMetaBlock(name, compressionName));
      return _bw;
    }

    public ABlockWriter prepareDataBlock() throws IOException {
      _bw = new BlockWrite(_bc.prepareDataBlock());
      return _bw;
    }

    public void close() throws IOException {

      _bw.close();
      _bc.close();

      if (this.fsout != null) {
        this.fsout.close();
      }

    }

  }

  public static class BlockWrite extends DataOutputStream implements ABlockWriter {
    BlockAppender _ba;

    public BlockWrite(BlockAppender ba) {
      super(ba);
      this._ba = ba;
    };

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
    public DataOutputStream getStream() throws IOException {

      return this;
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
    private BCFile.Reader _bc;
    private String fileName = "not_available";
    private BlockCache _dCache = null;
    private BlockCache _iCache = null;
    private FSDataInputStream fin = null;
    private FileSystem fs;
    private Configuration conf;
    private boolean closed = false;
    private AccumuloConfiguration accumuloConfiguration = null;

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

      /*
       * Grab path create input stream grab len create file
       */

      fileName = dataFile.toString();
      this._dCache = data;
      this._iCache = index;
      this.fs = fs;
      this.conf = conf;
      this.accumuloConfiguration = accumuloConfiguration;
    }

    public Reader(FSDataInputStream fsin, long len, Configuration conf, BlockCache data, BlockCache index, AccumuloConfiguration accumuloConfiguration)
        throws IOException {
      this._dCache = data;
      this._iCache = index;
      init(fsin, len, conf, accumuloConfiguration);
    }

    public Reader(FSDataInputStream fsin, long len, Configuration conf, AccumuloConfiguration accumuloConfiguration) throws IOException {
      // this.fin = fsin;
      init(fsin, len, conf, accumuloConfiguration);
    }

    private void init(FSDataInputStream fsin, long len, Configuration conf, AccumuloConfiguration accumuloConfiguration) throws IOException {
      this._bc = new BCFile.Reader(this, fsin, len, conf, accumuloConfiguration);
    }

    private synchronized BCFile.Reader getBCFile(AccumuloConfiguration accumuloConfiguration) throws IOException {
      if (closed)
        throw new IllegalStateException("File " + fileName + " is closed");

      if (_bc == null) {
        // lazily open file if needed
        Path path = new Path(fileName);
        fin = fs.open(path);
        init(fin, fs.getFileStatus(path).getLen(), conf, accumuloConfiguration);
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

      if ((cache == null) || (_currBlock.getRawSize() > cache.getMaxSize())) {
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

    public BlockRead getDataBlock(int blockIndex) throws IOException {
      String _lookup = this.fileName + "O" + blockIndex;
      return getBlock(_lookup, _dCache, new OffsetBlockLoader(blockIndex));

    }

    @Override
    public ABlockReader getDataBlock(long offset, long compressedSize, long rawSize) throws IOException {
      String _lookup = this.fileName + "R" + offset;
      return getBlock(_lookup, _dCache, new RawBlockLoader(offset, compressedSize, rawSize));
    }

    public synchronized void close() throws IOException {
      if (closed)
        return;

      closed = true;

      if (_bc != null)
        _bc.close();

      if (fin != null) {
        fin.close();
      }
    }

  }

  static class SeekableByteArrayInputStream extends ByteArrayInputStream {

    public SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    public SeekableByteArrayInputStream(byte buf[], int offset, int length) {
      super(buf, offset, length);
      throw new UnsupportedOperationException("Seek code assumes offset is zero"); // do not need this constructor, documenting that seek will not work
                                                                                   // unless offset it kept track of
    }

    public void seek(int position) {
      if (pos < 0 || pos >= buf.length)
        throw new IllegalArgumentException("pos = " + pos + " buf.lenght = " + buf.length);
      this.pos = position;
    }

    public int getPosition() {
      return this.pos;
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
    public <T> T getIndex(Class<T> clazz) {
      T bi = null;
      synchronized (cb) {
        @SuppressWarnings("unchecked")
        SoftReference<T> softRef = (SoftReference<T>) cb.getIndex();
        if (softRef != null)
          bi = softRef.get();

        if (bi == null) {
          try {
            bi = clazz.newInstance();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          cb.setIndex(new SoftReference<T>(bi));
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
    private long size;

    public BlockRead(InputStream in, long size) {
      super(in);
      this.size = size;
    }

    /**
     * Size is the size of the bytearray that was read form the cache
     */
    public long getRawSize() {
      return size;
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

  }
}
