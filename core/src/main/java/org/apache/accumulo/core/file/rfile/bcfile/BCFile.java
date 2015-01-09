/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.BlockRead;
import org.apache.accumulo.core.file.rfile.bcfile.CompareUtils.Scalar;
import org.apache.accumulo.core.file.rfile.bcfile.CompareUtils.ScalarComparator;
import org.apache.accumulo.core.file.rfile.bcfile.CompareUtils.ScalarLong;
import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.accumulo.core.file.rfile.bcfile.Utils.Version;
import org.apache.accumulo.core.security.crypto.CryptoModule;
import org.apache.accumulo.core.security.crypto.CryptoModuleFactory;
import org.apache.accumulo.core.security.crypto.CryptoModuleParameters;
import org.apache.accumulo.core.security.crypto.SecretKeyEncryptionStrategy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * Block Compressed file, the underlying physical storage layer for TFile. BCFile provides the basic block level compression for the data block and meta blocks.
 * It is separated from TFile as it may be used for other block-compressed file implementation.
 */
public final class BCFile {
  // the current version of BCFile impl, increment them (major or minor) made
  // enough changes
  static final Version API_VERSION = new Version((short) 2, (short) 0);
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
  static public class Writer implements Closeable {
    private final FSDataOutputStream out;
    private final Configuration conf;
    private final CryptoModule cryptoModule;
    private BCFileCryptoModuleParameters cryptoParams;
    private SecretKeyEncryptionStrategy secretKeyEncryptionStrategy;
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

    /**
     * Call-back interface to register a block after a block is closed.
     */
    private interface BlockRegister {
      /**
       * Register a block that is fully closed.
       *
       * @param raw
       *          The size of block in terms of uncompressed bytes.
       * @param offsetStart
       *          The start offset of the block.
       * @param offsetEnd
       *          One byte after the end of the block. Compressed block size is offsetEnd - offsetStart.
       */
      void register(long raw, long offsetStart, long offsetEnd);
    }

    /**
     * Intermediate class that maintain the state of a Writable Compression Block.
     */
    private static final class WBlockState {
      private final Algorithm compressAlgo;
      private Compressor compressor; // !null only if using native
      // Hadoop compression
      private final FSDataOutputStream fsOut;
      private final OutputStream cipherOut;
      private final long posStart;
      private final SimpleBufferedOutputStream fsBufferedOutput;
      private OutputStream out;

      /**
       * @param compressionAlgo
       *          The compression algorithm to be used to for compression.
       * @param cryptoModule
       *          the module to use to obtain cryptographic streams
       */
      public WBlockState(Algorithm compressionAlgo, FSDataOutputStream fsOut, BytesWritable fsOutputBuffer, Configuration conf, CryptoModule cryptoModule,
          CryptoModuleParameters cryptoParams) throws IOException {
        this.compressAlgo = compressionAlgo;
        this.fsOut = fsOut;
        this.posStart = fsOut.getPos();

        fsOutputBuffer.setCapacity(getFSOutputBufferSize(conf));

        this.fsBufferedOutput = new SimpleBufferedOutputStream(this.fsOut, fsOutputBuffer.getBytes());

        // *This* is very important. Without this, when the crypto stream is closed (in order to flush its last bytes),
        // the underlying RFile stream will *also* be closed, and that's undesirable as the cipher stream is closed for
        // every block written.
        cryptoParams.setCloseUnderylingStreamAfterCryptoStreamClose(false);

        // *This* is also very important. We don't want the underlying stream messed with.
        cryptoParams.setRecordParametersToStream(false);

        // It is also important to make sure we get a new initialization vector on every call in here,
        // so set any existing one to null, in case we're reusing a parameters object for its RNG or other bits
        cryptoParams.setInitializationVector(null);

        // Initialize the cipher including generating a new IV
        cryptoParams = cryptoModule.initializeCipher(cryptoParams);

        // Write the init vector in plain text, uncompressed, to the output stream. Due to the way the streams work out, there's no good way to write this
        // compressed, but it's pretty small.
        DataOutputStream tempDataOutputStream = new DataOutputStream(fsBufferedOutput);

        // Init vector might be null if the underlying cipher does not require one (NullCipher being a good example)
        if (cryptoParams.getInitializationVector() != null) {
          tempDataOutputStream.writeInt(cryptoParams.getInitializationVector().length);
          tempDataOutputStream.write(cryptoParams.getInitializationVector());
        } else {
          // Do nothing
        }

        // Initialize the cipher stream and get the IV
        cryptoParams.setPlaintextOutputStream(tempDataOutputStream);
        cryptoParams = cryptoModule.getEncryptingOutputStream(cryptoParams);

        if (cryptoParams.getEncryptedOutputStream() == tempDataOutputStream) {
          this.cipherOut = fsBufferedOutput;
        } else {
          this.cipherOut = cryptoParams.getEncryptedOutputStream();
        }

        this.compressor = compressAlgo.getCompressor();

        try {
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
      long getCurrentPos() throws IOException {
        return fsOut.getPos() + fsBufferedOutput.size();
      }

      long getStartPos() {
        return posStart;
      }

      /**
       * Current size of compressed data.
       */
      long getCompressedSize() throws IOException {
        long ret = getCurrentPos() - posStart;
        return ret;
      }

      /**
       * Finishing up the current block.
       */
      public void finish() throws IOException {
        try {
          if (out != null) {
            out.flush();

            // If the cipherOut stream is different from the fsBufferedOutput stream, then we likely have
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
      private final BlockRegister blockRegister;
      private final WBlockState wBlkState;
      private boolean closed = false;

      /**
       * Constructor
       *
       * @param register
       *          the block register, which is called when the block is closed.
       * @param wbs
       *          The writable compression block state.
       */
      BlockAppender(BlockRegister register, WBlockState wbs) {
        super(wbs.getOutputStream());
        this.blockRegister = register;
        this.wBlkState = wbs;
      }

      /**
       * Get the raw size of the block.
       *
       * @return the number of uncompressed bytes written through the BlockAppender so far.
       */
      public long getRawSize() throws IOException {
        /**
         * Expecting the size() of a block not exceeding 4GB. Assuming the size() will wrap to negative integer if it exceeds 2GB.
         */
        return size() & 0x00000000ffffffffL;
      }

      /**
       * Get the compressed size of the block in progress.
       *
       * @return the number of compressed bytes written to the underlying FS file. The size may be smaller than actual need to compress the all data written due
       *         to internal buffering inside the compressor.
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
       * Signaling the end of write to the block. The block register will be called for registering the finished block.
       */
      @Override
      public void close() throws IOException {
        if (closed == true) {
          return;
        }
        try {
          ++errorCount;
          wBlkState.finish();
          blockRegister.register(getRawSize(), wBlkState.getStartPos(), wBlkState.getCurrentPos());
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
     * @param fout
     *          FS output stream.
     * @param compressionName
     *          Name of the compression algorithm, which will be used for all data blocks.
     * @see Compression#getSupportedAlgorithms
     */
    public Writer(FSDataOutputStream fout, String compressionName, Configuration conf, boolean trackDataBlocks, AccumuloConfiguration accumuloConfiguration)
        throws IOException {
      if (fout.getPos() != 0) {
        throw new IOException("Output file not at zero offset.");
      }

      this.out = fout;
      this.conf = conf;
      dataIndex = new DataIndex(compressionName, trackDataBlocks);
      metaIndex = new MetaIndex();
      fsOutputBuffer = new BytesWritable();
      Magic.write(fout);

      // Set up crypto-related detail, including secret key generation and encryption

      this.cryptoModule = CryptoModuleFactory.getCryptoModule(accumuloConfiguration);
      this.cryptoParams = new BCFileCryptoModuleParameters();
      CryptoModuleFactory.fillParamsObjectFromConfiguration(cryptoParams, accumuloConfiguration);
      this.cryptoParams = (BCFileCryptoModuleParameters) cryptoModule.generateNewRandomSessionKey(cryptoParams);

      this.secretKeyEncryptionStrategy = CryptoModuleFactory.getSecretKeyEncryptionStrategy(accumuloConfiguration);
      this.cryptoParams = (BCFileCryptoModuleParameters) secretKeyEncryptionStrategy.encryptSecretKey(cryptoParams);

      // secretKeyEncryptionStrategy.encryptSecretKey(cryptoParameters);

    }

    /**
     * Close the BCFile Writer. Attempting to use the Writer after calling <code>close</code> is not allowed and may lead to undetermined results.
     */
    @Override
    public void close() throws IOException {
      if (closed == true) {
        return;
      }

      try {
        if (errorCount == 0) {
          if (blkInProgress == true) {
            throw new IllegalStateException("Close() called with active block appender.");
          }

          // add metaBCFileIndex to metaIndex as the last meta block
          BlockAppender appender = prepareMetaBlock(DataIndex.BLOCK_NAME, getDefaultCompressionAlgorithm());
          try {
            dataIndex.write(appender);
          } finally {
            appender.close();
          }

          long offsetIndexMeta = out.getPos();
          metaIndex.write(out);

          if (cryptoParams.getAlgorithmName() == null || cryptoParams.getAlgorithmName().equals(Property.CRYPTO_CIPHER_SUITE.getDefaultValue())) {
            out.writeLong(offsetIndexMeta);
            API_VERSION_1.write(out);
          } else {
            long offsetCryptoParameters = out.getPos();
            cryptoParams.write(out);

            // Meta Index, crypto params offsets and the trailing section are written out directly.
            out.writeLong(offsetIndexMeta);
            out.writeLong(offsetCryptoParameters);
            API_VERSION.write(out);
          }

          Magic.write(out);
          out.flush();
        }
      } finally {
        closed = true;
      }
    }

    private Algorithm getDefaultCompressionAlgorithm() {
      return dataIndex.getDefaultCompressionAlgorithm();
    }

    private BlockAppender prepareMetaBlock(String name, Algorithm compressAlgo) throws IOException, MetaBlockAlreadyExists {
      if (blkInProgress == true) {
        throw new IllegalStateException("Cannot create Meta Block until previous block is closed.");
      }

      if (metaIndex.getMetaByName(name) != null) {
        throw new MetaBlockAlreadyExists("name=" + name);
      }

      MetaBlockRegister mbr = new MetaBlockRegister(name, compressAlgo);
      WBlockState wbs = new WBlockState(compressAlgo, out, fsOutputBuffer, conf, cryptoModule, cryptoParams);
      BlockAppender ba = new BlockAppender(mbr, wbs);
      blkInProgress = true;
      metaBlkSeen = true;
      return ba;
    }

    /**
     * Create a Meta Block and obtain an output stream for adding data into the block. There can only be one BlockAppender stream active at any time. Regular
     * Blocks may not be created after the first Meta Blocks. The caller must call BlockAppender.close() to conclude the block creation.
     *
     * @param name
     *          The name of the Meta Block. The name must not conflict with existing Meta Blocks.
     * @param compressionName
     *          The name of the compression algorithm to be used.
     * @return The BlockAppender stream
     * @throws MetaBlockAlreadyExists
     *           If the meta block with the name already exists.
     */
    public BlockAppender prepareMetaBlock(String name, String compressionName) throws IOException, MetaBlockAlreadyExists {
      return prepareMetaBlock(name, Compression.getCompressionAlgorithmByName(compressionName));
    }

    /**
     * Create a Meta Block and obtain an output stream for adding data into the block. The Meta Block will be compressed with the same compression algorithm as
     * data blocks. There can only be one BlockAppender stream active at any time. Regular Blocks may not be created after the first Meta Blocks. The caller
     * must call BlockAppender.close() to conclude the block creation.
     *
     * @param name
     *          The name of the Meta Block. The name must not conflict with existing Meta Blocks.
     * @return The BlockAppender stream
     * @throws MetaBlockAlreadyExists
     *           If the meta block with the name already exists.
     */
    public BlockAppender prepareMetaBlock(String name) throws IOException, MetaBlockAlreadyExists {
      return prepareMetaBlock(name, getDefaultCompressionAlgorithm());
    }

    /**
     * Create a Data Block and obtain an output stream for adding data into the block. There can only be one BlockAppender stream active at any time. Data
     * Blocks may not be created after the first Meta Blocks. The caller must call BlockAppender.close() to conclude the block creation.
     *
     * @return The BlockAppender stream
     */
    public BlockAppender prepareDataBlock() throws IOException {
      if (blkInProgress == true) {
        throw new IllegalStateException("Cannot create Data Block until previous block is closed.");
      }

      if (metaBlkSeen == true) {
        throw new IllegalStateException("Cannot create Data Block after Meta Blocks.");
      }

      DataBlockRegister dbr = new DataBlockRegister();

      WBlockState wbs = new WBlockState(getDefaultCompressionAlgorithm(), out, fsOutputBuffer, conf, cryptoModule, cryptoParams);
      BlockAppender ba = new BlockAppender(dbr, wbs);
      blkInProgress = true;
      return ba;
    }

    /**
     * Callback to make sure a meta block is added to the internal list when its stream is closed.
     */
    private class MetaBlockRegister implements BlockRegister {
      private final String name;
      private final Algorithm compressAlgo;

      MetaBlockRegister(String name, Algorithm compressAlgo) {
        this.name = name;
        this.compressAlgo = compressAlgo;
      }

      @Override
      public void register(long raw, long begin, long end) {
        metaIndex.addEntry(new MetaIndexEntry(name, compressAlgo, new BlockRegion(begin, end - begin, raw)));
      }
    }

    /**
     * Callback to make sure a data block is added to the internal list when it's being closed.
     *
     */
    private class DataBlockRegister implements BlockRegister {
      DataBlockRegister() {
        // do nothing
      }

      @Override
      public void register(long raw, long begin, long end) {
        dataIndex.addBlockRegion(new BlockRegion(begin, end - begin, raw));
      }
    }
  }

  private static class BCFileCryptoModuleParameters extends CryptoModuleParameters {

    public void write(DataOutput out) throws IOException {
      // Write out the context
      out.writeInt(getAllOptions().size());
      for (String key : getAllOptions().keySet()) {
        out.writeUTF(key);
        out.writeUTF(getAllOptions().get(key));
      }

      // Write the opaque ID
      out.writeUTF(getOpaqueKeyEncryptionKeyID());

      // Write the encrypted secret key
      out.writeInt(getEncryptedKey().length);
      out.write(getEncryptedKey());

    }

    public void read(DataInput in) throws IOException {

      Map<String,String> optionsFromFile = new HashMap<String,String>();

      int numContextEntries = in.readInt();
      for (int i = 0; i < numContextEntries; i++) {
        optionsFromFile.put(in.readUTF(), in.readUTF());
      }

      CryptoModuleFactory.fillParamsObjectFromStringMap(this, optionsFromFile);

      // Read opaque key encryption ID
      setOpaqueKeyEncryptionKeyID(in.readUTF());

      // Read encrypted secret key
      int encryptedSecretKeyLength = in.readInt();
      byte[] encryptedSecretKey = new byte[encryptedSecretKeyLength];
      in.readFully(encryptedSecretKey);
      setEncryptedKey(encryptedSecretKey);

    }

  }

  /**
   * BCFile Reader, interface to read the file's data and meta blocks.
   */
  static public class Reader implements Closeable {
    private static final String META_NAME = "BCFile.metaindex";
    private static final String CRYPTO_BLOCK_NAME = "BCFile.cryptoparams";
    private final FSDataInputStream in;
    private final Configuration conf;
    final DataIndex dataIndex;
    // Index for meta blocks
    final MetaIndex metaIndex;
    final Version version;
    private BCFileCryptoModuleParameters cryptoParams;
    private CryptoModule cryptoModule;
    private SecretKeyEncryptionStrategy secretKeyEncryptionStrategy;

    /**
     * Intermediate class that maintain the state of a Readable Compression Block.
     */
    static private final class RBlockState {
      private final Algorithm compressAlgo;
      private Decompressor decompressor;
      private final BlockRegion region;
      private final InputStream in;

      public RBlockState(Algorithm compressionAlgo, FSDataInputStream fsin, BlockRegion region, Configuration conf, CryptoModule cryptoModule,
          Version bcFileVersion, CryptoModuleParameters cryptoParams) throws IOException {
        this.compressAlgo = compressionAlgo;
        this.region = region;
        this.decompressor = compressionAlgo.getDecompressor();

        BoundedRangeFileInputStream boundedRangeFileInputStream = new BoundedRangeFileInputStream(fsin, this.region.getOffset(),
            this.region.getCompressedSize());
        InputStream inputStreamToBeCompressed = boundedRangeFileInputStream;

        if (cryptoParams != null && cryptoModule != null) {
          DataInputStream tempDataInputStream = new DataInputStream(boundedRangeFileInputStream);
          // Read the init vector from the front of the stream before initializing the cipher stream

          int ivLength = tempDataInputStream.readInt();
          byte[] initVector = new byte[ivLength];
          tempDataInputStream.readFully(initVector);

          cryptoParams.setInitializationVector(initVector);
          cryptoParams.setEncryptedInputStream(boundedRangeFileInputStream);

          // These two flags mirror those in WBlockState, and are very necessary to set in order that the underlying stream be written and handled
          // correctly.
          cryptoParams.setCloseUnderylingStreamAfterCryptoStreamClose(false);
          cryptoParams.setRecordParametersToStream(false);

          cryptoParams = cryptoModule.getDecryptingInputStream(cryptoParams);
          inputStreamToBeCompressed = cryptoParams.getPlaintextInputStream();
        }

        try {
          this.in = compressAlgo.createDecompressionStream(inputStreamToBeCompressed, decompressor, getFSInputBufferSize(conf));
        } catch (IOException e) {
          compressAlgo.returnDecompressor(decompressor);
          throw e;
        }
      }

      /**
       * Get the output stream for BlockAppender's consumption.
       *
       * @return the output stream suitable for writing block data.
       */
      public InputStream getInputStream() {
        return in;
      }

      public String getCompressionName() {
        return compressAlgo.getName();
      }

      public BlockRegion getBlockRegion() {
        return region;
      }

      public void finish() throws IOException {
        try {
          in.close();
        } finally {
          compressAlgo.returnDecompressor(decompressor);
          decompressor = null;
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
        if (closed == true) {
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
       * Get the name of the compression algorithm used to compress the block.
       *
       * @return name of the compression algorithm.
       */
      public String getCompressionName() {
        return rBlkState.getCompressionName();
      }

      /**
       * Get the uncompressed size of the block.
       *
       * @return uncompressed size of the block.
       */
      public long getRawSize() {
        return rBlkState.getBlockRegion().getRawSize();
      }

      /**
       * Get the compressed size of the block.
       *
       * @return compressed size of the block.
       */
      public long getCompressedSize() {
        return rBlkState.getBlockRegion().getCompressedSize();
      }

      /**
       * Get the starting position of the block in the file.
       *
       * @return the starting position of the block in the file.
       */
      public long getStartPos() {
        return rBlkState.getBlockRegion().getOffset();
      }
    }

    /**
     * Constructor
     *
     * @param fin
     *          FS input stream.
     * @param fileLength
     *          Length of the corresponding file
     */
    public Reader(FSDataInputStream fin, long fileLength, Configuration conf, AccumuloConfiguration accumuloConfiguration) throws IOException {

      this.in = fin;
      this.conf = conf;

      // Move the cursor to grab the version and the magic first
      fin.seek(fileLength - Magic.size() - Version.size());
      version = new Version(fin);
      Magic.readAndVerify(fin);

      // Do a version check
      if (!version.compatibleWith(BCFile.API_VERSION) && !version.equals(BCFile.API_VERSION_1)) {
        throw new RuntimeException("Incompatible BCFile fileBCFileVersion.");
      }

      // Read the right number offsets based on version
      long offsetIndexMeta = 0;
      long offsetCryptoParameters = 0;

      if (version.equals(API_VERSION_1)) {
        fin.seek(fileLength - Magic.size() - Version.size() - (Long.SIZE / Byte.SIZE));
        offsetIndexMeta = fin.readLong();

      } else {
        fin.seek(fileLength - Magic.size() - Version.size() - (2 * (Long.SIZE / Byte.SIZE)));
        offsetIndexMeta = fin.readLong();
        offsetCryptoParameters = fin.readLong();
      }

      // read meta index
      fin.seek(offsetIndexMeta);
      metaIndex = new MetaIndex(fin);

      // If they exist, read the crypto parameters
      if (!version.equals(BCFile.API_VERSION_1)) {

        // read crypto parameters
        fin.seek(offsetCryptoParameters);
        cryptoParams = new BCFileCryptoModuleParameters();
        cryptoParams.read(fin);

        this.cryptoModule = CryptoModuleFactory.getCryptoModule(cryptoParams.getAllOptions().get(Property.CRYPTO_MODULE_CLASS.getKey()));

        // TODO: Do I need this? Hmmm, maybe I do.
        if (accumuloConfiguration.getBoolean(Property.CRYPTO_OVERRIDE_KEY_STRATEGY_WITH_CONFIGURED_STRATEGY)) {
          Map<String,String> cryptoConfFromAccumuloConf = accumuloConfiguration.getAllPropertiesWithPrefix(Property.CRYPTO_PREFIX);
          Map<String,String> instanceConf = accumuloConfiguration.getAllPropertiesWithPrefix(Property.INSTANCE_PREFIX);

          cryptoConfFromAccumuloConf.putAll(instanceConf);

          for (String name : cryptoParams.getAllOptions().keySet()) {
            if (!name.equals(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey())) {
              cryptoConfFromAccumuloConf.put(name, cryptoParams.getAllOptions().get(name));
            } else {
              cryptoParams.setKeyEncryptionStrategyClass(cryptoConfFromAccumuloConf.get(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey()));
            }
          }

          cryptoParams.setAllOptions(cryptoConfFromAccumuloConf);
        }

        this.secretKeyEncryptionStrategy = CryptoModuleFactory.getSecretKeyEncryptionStrategy(cryptoParams.getKeyEncryptionStrategyClass());

        // This call should put the decrypted session key within the cryptoParameters object
        cryptoParams = (BCFileCryptoModuleParameters) secretKeyEncryptionStrategy.decryptSecretKey(cryptoParams);

        // secretKeyEncryptionStrategy.decryptSecretKey(cryptoParameters);
      } else {
        LOG.trace("Found a version 1 file to read.");
      }

      // read data:BCFile.index, the data block index
      BlockReader blockR = getMetaBlock(DataIndex.BLOCK_NAME);
      try {
        dataIndex = new DataIndex(blockR);
      } finally {
        blockR.close();
      }
    }

    public Reader(CachableBlockFile.Reader cache, FSDataInputStream fin, long fileLength, Configuration conf, AccumuloConfiguration accumuloConfiguration)
        throws IOException {
      this.in = fin;
      this.conf = conf;

      BlockRead cachedMetaIndex = cache.getCachedMetaBlock(META_NAME);
      BlockRead cachedDataIndex = cache.getCachedMetaBlock(DataIndex.BLOCK_NAME);
      BlockRead cachedCryptoParams = cache.getCachedMetaBlock(CRYPTO_BLOCK_NAME);

      if (cachedMetaIndex == null || cachedDataIndex == null || cachedCryptoParams == null) {
        // move the cursor to the beginning of the tail, containing: offset to the
        // meta block index, version and magic
        // Move the cursor to grab the version and the magic first
        fin.seek(fileLength - Magic.size() - Version.size());
        version = new Version(fin);
        Magic.readAndVerify(fin);

        // Do a version check
        if (!version.compatibleWith(BCFile.API_VERSION) && !version.equals(BCFile.API_VERSION_1)) {
          throw new RuntimeException("Incompatible BCFile fileBCFileVersion.");
        }

        // Read the right number offsets based on version
        long offsetIndexMeta = 0;
        long offsetCryptoParameters = 0;

        if (version.equals(API_VERSION_1)) {
          fin.seek(fileLength - Magic.size() - Version.size() - (Long.SIZE / Byte.SIZE));
          offsetIndexMeta = fin.readLong();

        } else {
          fin.seek(fileLength - Magic.size() - Version.size() - (2 * (Long.SIZE / Byte.SIZE)));
          offsetIndexMeta = fin.readLong();
          offsetCryptoParameters = fin.readLong();
        }

        // read meta index
        fin.seek(offsetIndexMeta);
        metaIndex = new MetaIndex(fin);

        // If they exist, read the crypto parameters
        if (!version.equals(BCFile.API_VERSION_1) && cachedCryptoParams == null) {

          // read crypto parameters
          fin.seek(offsetCryptoParameters);
          cryptoParams = new BCFileCryptoModuleParameters();
          cryptoParams.read(fin);

          if (accumuloConfiguration.getBoolean(Property.CRYPTO_OVERRIDE_KEY_STRATEGY_WITH_CONFIGURED_STRATEGY)) {
            Map<String,String> cryptoConfFromAccumuloConf = accumuloConfiguration.getAllPropertiesWithPrefix(Property.CRYPTO_PREFIX);
            Map<String,String> instanceConf = accumuloConfiguration.getAllPropertiesWithPrefix(Property.INSTANCE_PREFIX);

            cryptoConfFromAccumuloConf.putAll(instanceConf);

            for (String name : cryptoParams.getAllOptions().keySet()) {
              if (!name.equals(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey())) {
                cryptoConfFromAccumuloConf.put(name, cryptoParams.getAllOptions().get(name));
              } else {
                cryptoParams.setKeyEncryptionStrategyClass(cryptoConfFromAccumuloConf.get(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey()));
              }
            }

            cryptoParams.setAllOptions(cryptoConfFromAccumuloConf);
          }

          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(baos);
          cryptoParams.write(dos);
          dos.close();
          cache.cacheMetaBlock(CRYPTO_BLOCK_NAME, baos.toByteArray());

          this.cryptoModule = CryptoModuleFactory.getCryptoModule(cryptoParams.getAllOptions().get(Property.CRYPTO_MODULE_CLASS.getKey()));
          this.secretKeyEncryptionStrategy = CryptoModuleFactory.getSecretKeyEncryptionStrategy(cryptoParams.getKeyEncryptionStrategyClass());

          // This call should put the decrypted session key within the cryptoParameters object
          // secretKeyEncryptionStrategy.decryptSecretKey(cryptoParameters);

          cryptoParams = (BCFileCryptoModuleParameters) secretKeyEncryptionStrategy.decryptSecretKey(cryptoParams);

        } else if (cachedCryptoParams != null) {
          cryptoParams = new BCFileCryptoModuleParameters();
          cryptoParams.read(cachedCryptoParams);

          this.cryptoModule = CryptoModuleFactory.getCryptoModule(cryptoParams.getAllOptions().get(Property.CRYPTO_MODULE_CLASS.getKey()));
          this.secretKeyEncryptionStrategy = CryptoModuleFactory.getSecretKeyEncryptionStrategy(cryptoParams.getKeyEncryptionStrategyClass());

          // This call should put the decrypted session key within the cryptoParameters object
          // secretKeyEncryptionStrategy.decryptSecretKey(cryptoParameters);

          cryptoParams = (BCFileCryptoModuleParameters) secretKeyEncryptionStrategy.decryptSecretKey(cryptoParams);

        }

        if (cachedMetaIndex == null) {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(baos);
          metaIndex.write(dos);
          dos.close();
          cache.cacheMetaBlock(META_NAME, baos.toByteArray());
        }

        // read data:BCFile.index, the data block index
        if (cachedDataIndex == null) {
          BlockReader blockR = getMetaBlock(DataIndex.BLOCK_NAME);
          cachedDataIndex = cache.cacheMetaBlock(DataIndex.BLOCK_NAME, blockR);
        }

        try {
          dataIndex = new DataIndex(cachedDataIndex);
        } catch (IOException e) {
          LOG.error("Got IOException when trying to create DataIndex block");
          throw e;
        } finally {
          cachedDataIndex.close();
        }

      } else {
        // We have cached versions of the metaIndex, dataIndex and cryptoParams objects.
        // Use them to fill out this reader's members.
        version = null;

        metaIndex = new MetaIndex(cachedMetaIndex);
        dataIndex = new DataIndex(cachedDataIndex);
        cryptoParams = new BCFileCryptoModuleParameters();
        cryptoParams.read(cachedCryptoParams);

        this.cryptoModule = CryptoModuleFactory.getCryptoModule(cryptoParams.getAllOptions().get(Property.CRYPTO_MODULE_CLASS.getKey()));
        this.secretKeyEncryptionStrategy = CryptoModuleFactory.getSecretKeyEncryptionStrategy(cryptoParams.getKeyEncryptionStrategyClass());

        // This call should put the decrypted session key within the cryptoParameters object
        cryptoParams = (BCFileCryptoModuleParameters) secretKeyEncryptionStrategy.decryptSecretKey(cryptoParams);

      }
    }

    /**
     * Get the name of the default compression algorithm.
     *
     * @return the name of the default compression algorithm.
     */
    public String getDefaultCompressionName() {
      return dataIndex.getDefaultCompressionAlgorithm().getName();
    }

    /**
     * Get version of BCFile file being read.
     *
     * @return version of BCFile file being read.
     */
    public Version getBCFileVersion() {
      return version;
    }

    /**
     * Get version of BCFile API.
     *
     * @return version of BCFile API.
     */
    public Version getAPIVersion() {
      return API_VERSION;
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
     * @param name
     *          meta block name
     * @return BlockReader input stream for reading the meta block.
     * @throws MetaBlockDoesNotExist
     *           The Meta Block with the given name does not exist.
     */
    public BlockReader getMetaBlock(String name) throws IOException, MetaBlockDoesNotExist {
      MetaIndexEntry imeBCIndex = metaIndex.getMetaByName(name);
      if (imeBCIndex == null) {
        throw new MetaBlockDoesNotExist("name=" + name);
      }

      BlockRegion region = imeBCIndex.getRegion();
      return createReader(imeBCIndex.getCompressionAlgorithm(), region);
    }

    /**
     * Stream access to a Data Block.
     *
     * @param blockIndex
     *          0-based data block index.
     * @return BlockReader input stream for reading the data block.
     */
    public BlockReader getDataBlock(int blockIndex) throws IOException {
      if (blockIndex < 0 || blockIndex >= getBlockCount()) {
        throw new IndexOutOfBoundsException(String.format("blockIndex=%d, numBlocks=%d", blockIndex, getBlockCount()));
      }

      BlockRegion region = dataIndex.getBlockRegionList().get(blockIndex);
      return createReader(dataIndex.getDefaultCompressionAlgorithm(), region);
    }

    public BlockReader getDataBlock(long offset, long compressedSize, long rawSize) throws IOException {
      BlockRegion region = new BlockRegion(offset, compressedSize, rawSize);
      return createReader(dataIndex.getDefaultCompressionAlgorithm(), region);
    }

    private BlockReader createReader(Algorithm compressAlgo, BlockRegion region) throws IOException {
      RBlockState rbs = new RBlockState(compressAlgo, in, region, conf, cryptoModule, version, cryptoParams);
      return new BlockReader(rbs);
    }

    /**
     * Find the smallest Block index whose starting offset is greater than or equal to the specified offset.
     *
     * @param offset
     *          User-specific offset.
     * @return the index to the data Block if such block exists; or -1 otherwise.
     */
    public int getBlockIndexNear(long offset) {
      ArrayList<BlockRegion> list = dataIndex.getBlockRegionList();
      int idx = Utils.lowerBound(list, new ScalarLong(offset), new ScalarComparator());

      if (idx == list.size()) {
        return -1;
      }

      return idx;
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
      index = new TreeMap<String,MetaIndexEntry>();
    }

    // for read, construct the map from the file
    public MetaIndex(DataInput in) throws IOException {
      int count = Utils.readVInt(in);
      index = new TreeMap<String,MetaIndexEntry>();

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
    private final Algorithm compressionAlgorithm;
    private final static String defaultPrefix = "data:";

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

    public MetaIndexEntry(String metaName, Algorithm compressionAlgorithm, BlockRegion region) {
      this.metaName = metaName;
      this.compressionAlgorithm = compressionAlgorithm;
      this.region = region;
    }

    public String getMetaName() {
      return metaName;
    }

    public Algorithm getCompressionAlgorithm() {
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
    final static String BLOCK_NAME = "BCFile.index";

    private final Algorithm defaultCompressionAlgorithm;

    // for data blocks, each entry specifies a block's offset, compressed size
    // and raw size
    private final ArrayList<BlockRegion> listRegions;

    private boolean trackBlocks;

    // for read, deserialized from a file
    public DataIndex(DataInput in) throws IOException {
      defaultCompressionAlgorithm = Compression.getCompressionAlgorithmByName(Utils.readString(in));

      int n = Utils.readVInt(in);
      listRegions = new ArrayList<BlockRegion>(n);

      for (int i = 0; i < n; i++) {
        BlockRegion region = new BlockRegion(in);
        listRegions.add(region);
      }
    }

    // for write
    public DataIndex(String defaultCompressionAlgorithmName, boolean trackBlocks) {
      this.trackBlocks = trackBlocks;
      this.defaultCompressionAlgorithm = Compression.getCompressionAlgorithmByName(defaultCompressionAlgorithmName);
      listRegions = new ArrayList<BlockRegion>();
    }

    public Algorithm getDefaultCompressionAlgorithm() {
      return defaultCompressionAlgorithm;
    }

    public ArrayList<BlockRegion> getBlockRegionList() {
      return listRegions;
    }

    public void addBlockRegion(BlockRegion region) {
      if (trackBlocks)
        listRegions.add(region);
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
    private final static byte[] AB_MAGIC_BCFILE = {
        // ... total of 16 bytes
        (byte) 0xd1, (byte) 0x11, (byte) 0xd3, (byte) 0x68, (byte) 0x91, (byte) 0xb5, (byte) 0xd7, (byte) 0xb6, (byte) 0x39, (byte) 0xdf, (byte) 0x41,
        (byte) 0x40, (byte) 0x92, (byte) 0xba, (byte) 0xe1, (byte) 0x50};

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
  static final class BlockRegion implements Scalar {
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

    @Override
    public long magnitude() {
      return offset;
    }
  }
}
