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

import org.apache.accumulo.core.file.blockfile.ABlockReader;
import org.apache.accumulo.core.file.blockfile.ABlockWriter;
import org.apache.accumulo.core.file.blockfile.BlockFileReader;
import org.apache.accumulo.core.file.blockfile.BlockFileWriter;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.file.blockfile.cache.LruBlockCache;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile.Reader.BlockReader;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile.Writer.BlockAppender;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
    
    public Writer(FileSystem fs, Path fName, String compressAlgor, Configuration conf) throws IOException {
      this.fsout = fs.create(fName);
      init(fsout, compressAlgor, conf);
    }
    
    public Writer(FSDataOutputStream fsout, String compressAlgor, Configuration conf) throws IOException {
      this.fsout = fsout;
      init(fsout, compressAlgor, conf);
    }
    
    private void init(FSDataOutputStream fsout, String compressAlgor, Configuration conf) throws IOException {
      _bc = new BCFile.Writer(fsout, compressAlgor, conf);
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
    private LruBlockCache _dCache = null;
    private LruBlockCache _iCache = null;
    private Long modTime = -1l;
    private FSDataInputStream fin = null;
    
    public Reader(FileSystem fs, Path dataFile, Configuration conf, BlockCache data, BlockCache index) throws IOException {
      
      /*
       * Grab path create input stream grab len create file
       */
      
      fileName = dataFile.toString();
      this._dCache = (LruBlockCache) data;
      this._iCache = (LruBlockCache) index;
      FileStatus fStatus = fs.getFileStatus(dataFile);
      fin = fs.open(dataFile);
      this.modTime = fStatus.getModificationTime() ^ (fStatus.getLen() << 32);
      
      init(fin, fs.getFileStatus(dataFile).getLen(), conf);
      
    }
    
    public Reader(FSDataInputStream fsin, long len, Configuration conf) throws IOException {
      // this.fin = fsin;
      init(fsin, len, conf);
    }
    
    private void init(FSDataInputStream fsin, long len, Configuration conf) throws IOException {
      this._bc = new BCFile.Reader(fsin, len, conf);
    }
    
    /**
     * It is intended that once the BlockRead object is returned to the caller, that the caller will read the entire block and then call close on the BlockRead
     * class.
     * 
     * NOTE: In the case of multi-read threads: This method can do redundant work where an entry is read from disk and other threads check the cache before it
     * has been inserted.
     */
    public BlockRead getMetaBlock(String blockName) throws IOException {
      String _lookup = fileName + blockName + this.modTime;
      byte b[] = null;
      BlockReader _currBlock;
      
      if (_iCache != null) {
        b = _iCache.getBlock(_lookup);
        
        if (b != null) {
          return new BlockRead(new DataInputStream(new ByteArrayInputStream(b)), b.length);
        }
        
      }
      /**
       * grab the currBlock at this point the block is still in the data stream
       * 
       */
      _currBlock = _bc.getMetaBlock(blockName);
      
      /**
       * If the block is bigger than the cache just return the stream
       */
      if ((_iCache == null) || (_currBlock.getRawSize() > _iCache.getMaxSize())) {
        return new BlockRead(_currBlock, _currBlock.getRawSize());
      } else {
        
        /**
         * Try to fully read block for meta data if error try to close file
         * 
         */
        try {
          b = new byte[(int) _currBlock.getRawSize()];
          _currBlock.readFully(b);
        } catch (IOException e) {
          log.debug("Error full blockRead for MetaBlock for file " + fileName + " for block name " + blockName, e);
          throw e;
        } finally {
          _currBlock.close();
        }
        
        if (_iCache != null) {
          try {
            _iCache.cacheBlock(_lookup, b);
          } catch (Exception e) {
            log.warn("Already cached block: " + _lookup, e);
          }
        }
        
        return new BlockRead(new DataInputStream(new ByteArrayInputStream(b)), b.length);
        
      }
      
    }
    
    /**
     * It is intended that once the BlockRead object is returned to the caller, that the caller will read the entire block and then call close on the BlockRead
     * class.
     * 
     * NOTE: In the case of multi-read threads: This method can do redundant work where an entry is read from disk and other threads check the cache before it
     * has been inserted.
     */
    
    public BlockRead getDataBlock(int blockIndex) throws IOException {
      String _lookup = this.fileName + blockIndex + this.modTime;
      byte b[] = null;
      BlockReader _currBlock;
      /**
       * If check is cache is defined.
       */
      if (_dCache != null) {
        
        b = _dCache.getBlock(_lookup);
        
        if (b != null) {
          return new BlockRead(new DataInputStream(new ByteArrayInputStream(b)), b.length);
        }
        
      }
      /**
       * grab the currBlock at this point the block is still in the data stream
       * 
       */
      _currBlock = _bc.getDataBlock(blockIndex);
      
      /**
       * If the block is bigger than the cache just return the stream
       */
      if ((_dCache == null) || (_currBlock.getRawSize() > _dCache.getMaxSize())) {
        return new BlockRead(_currBlock, _currBlock.getRawSize());
      } else {
        
        /**
         * Try to read block fully Incase of hdfs error catch close stream
         */
        try {
          b = new byte[(int) _currBlock.getRawSize()];
          _currBlock.readFully(b);
        } catch (IOException e) {
          log.error("Error full blockRead for DataBlock for file" + fileName + "for blocknumber " + blockIndex, e);
          throw e;
        } finally {
          _currBlock.close();
        }
        
        if (_dCache != null) {
          try {
            _dCache.cacheBlock(_lookup, b);
          } catch (Exception e) {
            log.warn("Already cached block: " + _lookup, e);
          }
        }
        
        return new BlockRead(new DataInputStream(new ByteArrayInputStream(b)), b.length);
      }
      
    }
    
    public long getBlockCount() {
      return _bc.getBlockCount();
    }
    
    public void close() throws IOException {
      
      _bc.close();
      if (fin != null) {
        fin.close();
      }
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
    
  }
}
