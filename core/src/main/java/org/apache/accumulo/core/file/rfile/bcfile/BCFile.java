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
package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.crypto.CryptoEnvironmentImpl;
import org.apache.accumulo.core.crypto.CryptoUtils;
import org.apache.accumulo.core.file.rfile.bcfile.Utils.Version;
import org.apache.accumulo.core.file.streams.BoundedRangeFileInputStream;
import org.apache.accumulo.core.file.streams.RateLimitedOutputStream;
import org.apache.accumulo.core.file.streams.SeekableDataInputStream;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment.Scope;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.FileDecrypter;
import org.apache.accumulo.core.spi.crypto.FileEncrypter;
import org.apache.accumulo.core.spi.crypto.NoFileDecrypter;
import org.apache.accumulo.core.spi.crypto.NoFileEncrypter;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * Block Compressed file, the underlying physical storage layer for TFile. BCFile provides the basic
 * block level compression for the data block and meta blocks. It is separated from TFile as it may
 * be used for other block-compressed file implementation.
 */
public final class BCFile {
  // the current version of BCFile impl, increment them (major or minor) made
  // enough changes
  /**
   * Simplified encryption interface. Allows more flexible encryption.
   *
   * @since 2.0
   */
  static final Version API_VERSION_3 = new Version((short) 3, (short) 0);
  /**
   * Experimental crypto parameters, not flexible. Do not use.
   */
  static final Version API_VERSION_2 = new Version((short) 2, (short) 0);
  /**
   * Original BCFile version, prior to encryption. Also, any files before 2.0 that didn't have
   * encryption were marked with this version.
   */
  static final Version API_VERSION_1 = new Version((short) 1, (short) 0);
  static final Log LOG = LogFactory.getLog(BCFile.class);

  private static final String FS_OUTPUT_BUF_SIZE_ATTR = "tfile.fs.output.buffer.size";
  private static final String FS_INPUT_BUF_SIZE_ATTR = "tfile.fs.input.buffer.size";

  private static int getFSOutputBufferSize(Configuration conf) {
    return conf.getInt(FS_OUTPUT_BUF_SIZE_ATTR, 256 * 1024);
  }

  private static int getFSInputBufferSize(Configuration conf) {
    return conf.getInt(FS_INPUT_BUF_SIZE_ATTR, 32 * 1024);
  }

  /**
   * Prevent the instantiation of BCFile objects.
   */
  private BCFile() {
    // nothing
  }

  /**
   * BCFile writer, the entry point for creating a new BCFile.
   */
  public static class Writer implements Closeable {
    private final RateLimitedOutputStream out;
    private final Configuration conf;
    private FileEncrypter encrypter;
    private CryptoEnvironmentImpl cryptoEnvironment;
    // the single meta block containing index of compressed data blocks
    final DataIndex dataIndex;
    // index for meta blocks
    final MetaIndex metaIndex;
    boolean blkInProgress = false;
    private boolean metaBlkSeen = false;
    private boolean closed = false;
    long errorCount = 0;
    // reusable buffers.
    private BytesWritable fsOutputBuffer;
    private long length = 0;

    public long getLength() {
      return this.length;
    }

    /**
     * Intermediate class that maintain the state of a Writable Compression Block.
     */
    private static final class WBlockState {
      private final CompressionAlgorithm compressAlgo;
      private Compressor compressor; // !null only if using native
      // Hadoop compression
      private final RateLimitedOutputStream fsOut;
      private final OutputStream cipherOut;
      private final long posStart;
      private final SimpleBufferedOutputStream fsBufferedOutput;
      private OutputStream out;

      public WBlockState(CompressionAlgorithm compressionAlgo, RateLimitedOutputStream fsOut,
          BytesWritable fsOutputBuffer, Configuration conf, FileEncrypter encrypter)
          throws IOException {
        this.compressAlgo = compressionAlgo;
        this.fsOut = fsOut;
        this.posStart = fsOut.position();

        fsOutputBuffer.setCapacity(getFSOutputBufferSize(conf));

        this.fsBufferedOutput =
            new SimpleBufferedOutputStream(this.fsOut, fsOutputBuffer.getBytes());
        this.compressor = compressAlgo.getCompressor();

        try {
          this.cipherOut = encrypter.encryptStream(fsBufferedOutput);
          this.out = compressionAlgo.createCompressionStream(cipherOut, compressor, 0);
        } catch (IOException e) {
          compressAlgo.returnCompressor(compressor);
          throw e;
        }
      }

      /**
       * Get the output stream for BlockAppender's consumption.
       *
       * @return the output stream suitable for writing block data.
       */
      OutputStream getOutputStream() {
        return out;
      }

      /**
       * Get the current position in file.
       *
       * @return The current byte offset in underlying file.
       */
      long getCurrentPos() {
        return fsOut.position() + fsBufferedOutput.size();
      }

      long getStartPos() {
        return posStart;
      }

      /**
       * Current size of compressed data.
       */
      long getCompressedSize() {
        return getCurrentPos() - posStart;
      }

      /**
       * Finishing up the current block.
       */
      public void finish() throws IOException {
        try {
          if (out != null) {
            out.flush();

            // If the cipherOut stream is different from the fsBufferedOutput stream, then we likely
            // have
            // an actual encrypted output stream that needs to be closed in order for it
            // to flush the final bytes to the output stream. We should have set the flag to
            // make sure that this close does *not* close the underlying stream, so calling
            // close here should do the write thing.

            if (fsBufferedOutput != cipherOut) {
              // Close the cipherOutputStream
              cipherOut.close();
            }

            out = null;
          }
        } finally {
          compressAlgo.returnCompressor(compressor);
          compressor = null;
        }
      }
    }

    /**
     * Access point to stuff data into a block.
     *
     */
    public class BlockAppender extends DataOutputStream {
      private final MetaBlockRegister metaBlockRegister;
      private final WBlockState wBlkState;
      private boolean closed = false;

      /**
       * Constructor
       *
       * @param metaBlockRegister the block register, which is called when the block is closed.
       * @param wbs The writable compression block state.
       */
      BlockAppender(MetaBlockRegister metaBlockRegister, WBlockState wbs) {
        super(wbs.getOutputStream());
        this.metaBlockRegister = metaBlockRegister;
        this.wBlkState = wbs;
      }

      BlockAppender(WBlockState wbs) {
        super(wbs.getOutputStream());
        this.metaBlockRegister = null;
        this.wBlkState = wbs;
      }

      /**
       * Get the raw size of the block.
       *
       * Caution: size() comes from DataOutputStream which returns Integer.MAX_VALUE on an overflow.
       * This results in a value of 2GiB meaning that an unknown amount of data, at least 2GiB
       * large, has been written. RFiles handle this issue by keeping track of the position of
       * blocks instead of relying on blocks to provide this information.
       *
       * @return the number of uncompressed bytes written through the BlockAppender so far.
       */
      public long getRawSize() {
        return size() & 0x00000000ffffffffL;
      }

      /**
       * Get the compressed size of the block in progress.
       *
       * @return the number of compressed bytes written to the underlying FS file. The size may be
       *         smaller than actual need to compress the all data written due to internal buffering
       *         inside the compressor.
       */
      public long getCompressedSize() throws IOException {
        return wBlkState.getCompressedSize();
      }

      public long getStartPos() {
        return wBlkState.getStartPos();
      }

      @Override
      public void flush() {
        // The down stream is a special kind of stream that finishes a
        // compression block upon flush. So we disable flush() here.
      }

      /**
       * Signaling the end of write to the block. The block register will be called for registering
       * the finished block.
       */
      @Override
      public void close() throws IOException {
        if (closed) {
          return;
        }
        try {
          ++errorCount;
          wBlkState.finish();
          if (metaBlockRegister != null) {
            metaBlockRegister.register(getRawSize(), wBlkState.getStartPos(),
                wBlkState.getCurrentPos());
          }
          --errorCount;
        } finally {
          closed = true;
          blkInProgress = false;
        }
      }
    }

    /**
     * Constructor
     *
     * @param fout FS output stream.
     * @param compressionName Name of the compression algorithm, which will be used for all data
     *        blocks.
     * @see Compression#getSupportedAlgorithms
     */
    public Writer(FSDataOutputStream fout, RateLimiter writeLimiter, String compressionName,
        Configuration conf, CryptoService cryptoService) throws IOException {
      if (fout.getPos() != 0) {
        throw new IOException("Output file not at zero offset.");
      }

      this.out = new RateLimitedOutputStream(fout, writeLimiter);
      this.conf = conf;
      dataIndex = new DataIndex(compressionName);
      metaIndex = new MetaIndex();
      fsOutputBuffer = new BytesWritable();
      Magic.write(this.out);
      this.cryptoEnvironment = new CryptoEnvironmentImpl(Scope.TABLE, null, null);
      this.encrypter = cryptoService.getFileEncrypter(this.cryptoEnvironment);
    }

    /**
     * Close the BCFile Writer. Attempting to use the Writer after calling <code>close</code> is not
     * allowed and may lead to undetermined results.
     */
    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }

      try {
        if (errorCount == 0) {
          if (blkInProgress) {
            throw new IllegalStateException("Close() called with active block appender.");
          }

          // add metaBCFileIndex to metaIndex as the last meta block
          try (BlockAppender appender =
              prepareMetaBlock(DataIndex.BLOCK_NAME, getDefaultCompressionAlgorithm())) {
            dataIndex.write(appender);
          }

          long offsetIndexMeta = out.position();
          metaIndex.write(out);

          long offsetCryptoParameter = out.position();
          byte[] cryptoParams = this.encrypter.getDecryptionParameters();
          out.writeInt(cryptoParams.length);
          out.write(cryptoParams);

          out.writeLong(offsetIndexMeta);
          out.writeLong(offsetCryptoParameter);
          API_VERSION_3.write(out);
          Magic.write(out);
          out.flush();
          length = out.position();
          out.close();
        }
      } finally {
        closed = true;
      }
    }

    private CompressionAlgorithm getDefaultCompressionAlgorithm() {
      return dataIndex.getDefaultCompressionAlgorithm();
    }

    private BlockAppender prepareMetaBlock(String name, CompressionAlgorithm compressAlgo)
        throws IOException, MetaBlockAlreadyExists {
      if (blkInProgress) {
        throw new IllegalStateException("Cannot create Meta Block until previous block is closed.");
      }

      if (metaIndex.getMetaByName(name) != null) {
        throw new MetaBlockAlreadyExists("name=" + name);
      }

      MetaBlockRegister mbr = new MetaBlockRegister(name, compressAlgo);
      WBlockState wbs = new WBlockState(compressAlgo, out, fsOutputBuffer, conf, encrypter);
      BlockAppender ba = new BlockAppender(mbr, wbs);
      blkInProgress = true;
      metaBlkSeen = true;
      return ba;
    }

    /**
     * Create a Meta Block and obtain an output stream for adding data into the block. The Meta
     * Block will be compressed with the same compression algorithm as data blocks. There can only
     * be one BlockAppender stream active at any time. Regular Blocks may not be created after the
     * first Meta Blocks. The caller must call BlockAppender.close() to conclude the block creation.
     *
     * @param name The name of the Meta Block. The name must not conflict with existing Meta Blocks.
     * @return The BlockAppender stream
     * @throws MetaBlockAlreadyExists If the meta block with the name already exists.
     */
    public BlockAppender prepareMetaBlock(String name) throws IOException, MetaBlockAlreadyExists {
      return prepareMetaBlock(name, getDefaultCompressionAlgorithm());
    }

    /**
     * Create a Data Block and obtain an output stream for adding data into the block. There can
     * only be one BlockAppender stream active at any time. Data Blocks may not be created after the
     * first Meta Blocks. The caller must call BlockAppender.close() to conclude the block creation.
     *
     * @return The BlockAppender stream
     */
    public BlockAppender prepareDataBlock() throws IOException {
      if (blkInProgress) {
        throw new IllegalStateException("Cannot create Data Block until previous block is closed.");
      }

      if (metaBlkSeen) {
        throw new IllegalStateException("Cannot create Data Block after Meta Blocks.");
      }

      WBlockState wbs =
          new WBlockState(getDefaultCompressionAlgorithm(), out, fsOutputBuffer, conf, encrypter);
      BlockAppender ba = new BlockAppender(wbs);
      blkInProgress = true;
      return ba;
    }

    /**
     * Callback to make sure a meta block is added to the internal list when its stream is closed.
     */
    private class MetaBlockRegister {
      private final String name;
      private final CompressionAlgorithm compressAlgo;

      MetaBlockRegister(String name, CompressionAlgorithm compressAlgo) {
        this.name = name;
        this.compressAlgo = compressAlgo;
      }

      public void register(long raw, long begin, long end) {
        metaIndex.addEntry(
            new MetaIndexEntry(name, compressAlgo, new BlockRegion(begin, end - begin, raw)));
      }
    }
  }

  /**
   * BCFile Reader, interface to read the file's data and meta blocks.
   */
  public static class Reader implements Closeable {
    private final SeekableDataInputStream in;
    private final Configuration conf;
    final DataIndex dataIndex;
    // Index for meta blocks
    final MetaIndex metaIndex;
    final Version version;
    private byte[] decryptionParams;
    private FileDecrypter decrypter;

    /**
     * Intermediate class that maintain the state of a Readable Compression Block.
     */
    private static final class RBlockState {
      private final CompressionAlgorithm compressAlgo;
      private Decompressor decompressor;
      private final BlockRegion region;
      private final InputStream in;
      private volatile boolean closed;

      public <InputStreamType extends InputStream & Seekable> RBlockState(
          CompressionAlgorithm compressionAlgo, InputStreamType fsin, BlockRegion region,
          Configuration conf, FileDecrypter decrypter) throws IOException {
        this.compressAlgo = compressionAlgo;
        this.region = region;
        this.decompressor = compressionAlgo.getDecompressor();

        BoundedRangeFileInputStream boundedRangeFileInputStream = new BoundedRangeFileInputStream(
            fsin, this.region.getOffset(), this.region.getCompressedSize());

        try {
          InputStream inputStreamToBeCompressed =
              decrypter.decryptStream(boundedRangeFileInputStream);
          this.in = compressAlgo.createDecompressionStream(inputStreamToBeCompressed, decompressor,
              getFSInputBufferSize(conf));
        } catch (IOException e) {
          compressAlgo.returnDecompressor(decompressor);
          throw e;
        }
        closed = false;
      }

      /**
       * Get the output stream for BlockAppender's consumption.
       *
       * @return the output stream suitable for writing block data.
       */
      public InputStream getInputStream() {
        return in;
      }

      public BlockRegion getBlockRegion() {
        return region;
      }

      public void finish() throws IOException {
        synchronized (in) {
          if (!closed) {
            try {
              in.close();
            } finally {
              closed = true;
              if (decompressor != null) {
                try {
                  compressAlgo.returnDecompressor(decompressor);
                } finally {
                  decompressor = null;
                }
              }
            }
          }
        }
      }
    }

    /**
     * Access point to read a block.
     */
    public static class BlockReader extends DataInputStream {
      private final RBlockState rBlkState;
      private boolean closed = false;

      BlockReader(RBlockState rbs) {
        super(rbs.getInputStream());
        rBlkState = rbs;
      }

      /**
       * Finishing reading the block. Release all resources.
       */
      @Override
      public void close() throws IOException {
        if (closed) {
          return;
        }
        try {
          // Do not set rBlkState to null. People may access stats after calling
          // close().
          rBlkState.finish();
        } finally {
          closed = true;
        }
      }

      /**
       * Get the uncompressed size of the block.
       *
       * @return uncompressed size of the block.
       */
      public long getRawSize() {
        return rBlkState.getBlockRegion().getRawSize();
      }
    }

    public byte[] serializeMetadata(int maxSize) {
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        metaIndex.write(out);
        if (out.size() > maxSize) {
          return null;
        }
        dataIndex.write(out);
        if (out.size() > maxSize) {
          return null;
        }

        CryptoUtils.writeParams(this.decryptionParams, out);

        if (out.size() > maxSize) {
          return null;
        }

        out.close();
        return baos.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    public <InputStreamType extends InputStream & Seekable> Reader(InputStreamType fin,
        long fileLength, Configuration conf, CryptoService cryptoService) throws IOException {
      this.in = new SeekableDataInputStream(fin);
      this.conf = conf;

      // Move the cursor to grab the version and the magic first
      this.in.seek(fileLength - Magic.size() - Version.size());
      version = new Version(this.in);
      Magic.readAndVerify(this.in);

      // Do a version check - API_VERSION_2 used experimental crypto parameters, no longer supported
      if (!version.compatibleWith(BCFile.API_VERSION_3)
          && !version.compatibleWith(BCFile.API_VERSION_1)) {
        throw new IOException("Unsupported BCFile Version found: " + version + ". "
            + "Only support " + API_VERSION_1 + " or " + API_VERSION_3);
      }

      // Read the right number offsets based on version
      long offsetIndexMeta = 0;
      long offsetCryptoParameters = 0;

      if (version.equals(API_VERSION_1)) {
        this.in.seek(fileLength - Magic.size() - Version.size() - Long.BYTES);
        offsetIndexMeta = this.in.readLong();
      } else {
        this.in.seek(fileLength - Magic.size() - Version.size() - 16); // 2 * Long.BYTES = 16
        offsetIndexMeta = this.in.readLong();
        offsetCryptoParameters = this.in.readLong();
      }

      // read meta index
      this.in.seek(offsetIndexMeta);
      metaIndex = new MetaIndex(this.in);

      CryptoEnvironment cryptoEnvironment = null;

      // backwards compatibility
      if (version.equals(API_VERSION_1)) {
        LOG.trace("Found a version 1 file to read.");
        decryptionParams = new NoFileEncrypter().getDecryptionParameters();
        this.decrypter = new NoFileDecrypter();
      } else {
        // read crypto parameters and get decrypter
        this.in.seek(offsetCryptoParameters);
        decryptionParams = CryptoUtils.readParams(this.in);
        cryptoEnvironment = new CryptoEnvironmentImpl(Scope.TABLE, null, decryptionParams);
        this.decrypter = cryptoService.getFileDecrypter(cryptoEnvironment);
      }

      // read data:BCFile.index, the data block index
      try (BlockReader blockR = getMetaBlock(DataIndex.BLOCK_NAME)) {
        dataIndex = new DataIndex(blockR);
      }
    }

    public <InputStreamType extends InputStream & Seekable> Reader(byte[] serializedMetadata,
        InputStreamType fin, Configuration conf, CryptoService cryptoService) throws IOException {
      this.in = new SeekableDataInputStream(fin);
      this.conf = conf;

      ByteArrayInputStream bais = new ByteArrayInputStream(serializedMetadata);
      DataInputStream dis = new DataInputStream(bais);

      version = null;

      metaIndex = new MetaIndex(dis);
      dataIndex = new DataIndex(dis);

      decryptionParams = CryptoUtils.readParams(dis);
      CryptoEnvironmentImpl env = new CryptoEnvironmentImpl(Scope.TABLE, null, decryptionParams);
      this.decrypter = cryptoService.getFileDecrypter(env);
    }

    /**
     * Finishing reading the BCFile. Release all resources.
     */
    @Override
    public void close() {
      // nothing to be done now
    }

    /**
     * Get the number of data blocks.
     *
     * @return the number of data blocks.
     */
    public int getBlockCount() {
      return dataIndex.getBlockRegionList().size();
    }

    /**
     * Stream access to a Meta Block.
     *
     * @param name meta block name
     * @return BlockReader input stream for reading the meta block.
     * @throws MetaBlockDoesNotExist The Meta Block with the given name does not exist.
     */
    public BlockReader getMetaBlock(String name) throws IOException, MetaBlockDoesNotExist {
      MetaIndexEntry imeBCIndex = metaIndex.getMetaByName(name);
      if (imeBCIndex == null) {
        throw new MetaBlockDoesNotExist("name=" + name);
      }

      BlockRegion region = imeBCIndex.getRegion();
      return createReader(imeBCIndex.getCompressionAlgorithm(), region);
    }

    public long getMetaBlockRawSize(String name) throws IOException, MetaBlockDoesNotExist {
      MetaIndexEntry imeBCIndex = metaIndex.getMetaByName(name);
      if (imeBCIndex == null) {
        throw new MetaBlockDoesNotExist("name=" + name);
      }

      return imeBCIndex.getRegion().getRawSize();
    }

    /**
     * Stream access to a Data Block.
     *
     * @param blockIndex 0-based data block index.
     * @return BlockReader input stream for reading the data block.
     */
    public BlockReader getDataBlock(int blockIndex) throws IOException {
      if (blockIndex < 0 || blockIndex >= getBlockCount()) {
        throw new IndexOutOfBoundsException(
            String.format("blockIndex=%d, numBlocks=%d", blockIndex, getBlockCount()));
      }

      BlockRegion region = dataIndex.getBlockRegionList().get(blockIndex);
      return createReader(dataIndex.getDefaultCompressionAlgorithm(), region);
    }

    public BlockReader getDataBlock(long offset, long compressedSize, long rawSize)
        throws IOException {
      BlockRegion region = new BlockRegion(offset, compressedSize, rawSize);
      return createReader(dataIndex.getDefaultCompressionAlgorithm(), region);
    }

    public long getDataBlockRawSize(int blockIndex) {
      if (blockIndex < 0 || blockIndex >= getBlockCount()) {
        throw new IndexOutOfBoundsException(
            String.format("blockIndex=%d, numBlocks=%d", blockIndex, getBlockCount()));
      }

      return dataIndex.getBlockRegionList().get(blockIndex).getRawSize();
    }

    private BlockReader createReader(CompressionAlgorithm compressAlgo, BlockRegion region)
        throws IOException {
      RBlockState rbs = new RBlockState(compressAlgo, in, region, conf, decrypter);
      return new BlockReader(rbs);
    }
  }

  /**
   * Index for all Meta blocks.
   */
  static class MetaIndex {
    // use a tree map, for getting a meta block entry by name
    final Map<String,MetaIndexEntry> index;

    // for write
    public MetaIndex() {
      index = new TreeMap<>();
    }

    // for read, construct the map from the file
    public MetaIndex(DataInput in) throws IOException {
      int count = Utils.readVInt(in);
      index = new TreeMap<>();

      for (int nx = 0; nx < count; nx++) {
        MetaIndexEntry indexEntry = new MetaIndexEntry(in);
        index.put(indexEntry.getMetaName(), indexEntry);
      }
    }

    public void addEntry(MetaIndexEntry indexEntry) {
      index.put(indexEntry.getMetaName(), indexEntry);
    }

    public MetaIndexEntry getMetaByName(String name) {
      return index.get(name);
    }

    public void write(DataOutput out) throws IOException {
      Utils.writeVInt(out, index.size());

      for (MetaIndexEntry indexEntry : index.values()) {
        indexEntry.write(out);
      }
    }
  }

  /**
   * An entry describes a meta block in the MetaIndex.
   */
  static final class MetaIndexEntry {
    private final String metaName;
    private final CompressionAlgorithm compressionAlgorithm;
    private static final String defaultPrefix = "data:";

    private final BlockRegion region;

    public MetaIndexEntry(DataInput in) throws IOException {
      String fullMetaName = Utils.readString(in);
      if (fullMetaName.startsWith(defaultPrefix)) {
        metaName = fullMetaName.substring(defaultPrefix.length(), fullMetaName.length());
      } else {
        throw new IOException("Corrupted Meta region Index");
      }

      compressionAlgorithm = Compression.getCompressionAlgorithmByName(Utils.readString(in));
      region = new BlockRegion(in);
    }

    public MetaIndexEntry(String metaName, CompressionAlgorithm compressionAlgorithm,
        BlockRegion region) {
      this.metaName = metaName;
      this.compressionAlgorithm = compressionAlgorithm;
      this.region = region;
    }

    public String getMetaName() {
      return metaName;
    }

    public CompressionAlgorithm getCompressionAlgorithm() {
      return compressionAlgorithm;
    }

    public BlockRegion getRegion() {
      return region;
    }

    public void write(DataOutput out) throws IOException {
      Utils.writeString(out, defaultPrefix + metaName);
      Utils.writeString(out, compressionAlgorithm.getName());

      region.write(out);
    }
  }

  /**
   * Index of all compressed data blocks.
   */
  static class DataIndex {
    static final String BLOCK_NAME = "BCFile.index";

    private final CompressionAlgorithm defaultCompressionAlgorithm;

    // for data blocks, each entry specifies a block's offset, compressed size
    // and raw size
    private final ArrayList<BlockRegion> listRegions;

    // for read, deserialized from a file
    public DataIndex(DataInput in) throws IOException {
      defaultCompressionAlgorithm = Compression.getCompressionAlgorithmByName(Utils.readString(in));

      int n = Utils.readVInt(in);
      listRegions = new ArrayList<>(n);

      for (int i = 0; i < n; i++) {
        BlockRegion region = new BlockRegion(in);
        listRegions.add(region);
      }
    }

    // for write
    public DataIndex(String defaultCompressionAlgorithmName) {
      this.defaultCompressionAlgorithm =
          Compression.getCompressionAlgorithmByName(defaultCompressionAlgorithmName);
      listRegions = new ArrayList<>();
    }

    public CompressionAlgorithm getDefaultCompressionAlgorithm() {
      return defaultCompressionAlgorithm;
    }

    public ArrayList<BlockRegion> getBlockRegionList() {
      return listRegions;
    }

    public void write(DataOutput out) throws IOException {
      Utils.writeString(out, defaultCompressionAlgorithm.getName());

      Utils.writeVInt(out, listRegions.size());

      for (BlockRegion region : listRegions) {
        region.write(out);
      }
    }
  }

  /**
   * Magic number uniquely identifying a BCFile in the header/footer.
   */
  static final class Magic {
    private static final byte[] AB_MAGIC_BCFILE = {
        // ... total of 16 bytes
        (byte) 0xd1, (byte) 0x11, (byte) 0xd3, (byte) 0x68, (byte) 0x91, (byte) 0xb5, (byte) 0xd7,
        (byte) 0xb6, (byte) 0x39, (byte) 0xdf, (byte) 0x41, (byte) 0x40, (byte) 0x92, (byte) 0xba,
        (byte) 0xe1, (byte) 0x50};

    public static void readAndVerify(DataInput in) throws IOException {
      byte[] abMagic = new byte[size()];
      in.readFully(abMagic);

      // check against AB_MAGIC_BCFILE, if not matching, throw an
      // Exception
      if (!Arrays.equals(abMagic, AB_MAGIC_BCFILE)) {
        throw new IOException("Not a valid BCFile.");
      }
    }

    public static void write(DataOutput out) throws IOException {
      out.write(AB_MAGIC_BCFILE);
    }

    public static int size() {
      return AB_MAGIC_BCFILE.length;
    }
  }

  /**
   * Block region.
   */
  static final class BlockRegion {
    private final long offset;
    private final long compressedSize;
    private final long rawSize;

    public BlockRegion(DataInput in) throws IOException {
      offset = Utils.readVLong(in);
      compressedSize = Utils.readVLong(in);
      rawSize = Utils.readVLong(in);
    }

    public BlockRegion(long offset, long compressedSize, long rawSize) {
      this.offset = offset;
      this.compressedSize = compressedSize;
      this.rawSize = rawSize;
    }

    public void write(DataOutput out) throws IOException {
      Utils.writeVLong(out, offset);
      Utils.writeVLong(out, compressedSize);
      Utils.writeVLong(out, rawSize);
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
  }
}
