/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;

/**
 * Compression related stuff.
 */
public final class Compression {

  private static final Logger log = LoggerFactory.getLogger(Compression.class);

  /**
   * Prevent the instantiation of this class.
   */
  private Compression() {
    throw new UnsupportedOperationException();
  }

  static class FinishOnFlushCompressionStream extends FilterOutputStream {

    FinishOnFlushCompressionStream(CompressionOutputStream cout) {
      super(cout);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      CompressionOutputStream cout = (CompressionOutputStream) out;
      cout.finish();
      cout.flush();
      cout.resetState();
    }
  }

  /**
   * Compression: zStandard
   */
  public static final String COMPRESSION_ZSTD = "zstd";

  /**
   * Compression: snappy
   **/
  public static final String COMPRESSION_SNAPPY = "snappy";

  /**
   * Compression: gzip
   */
  public static final String COMPRESSION_GZ = "gz";

  /**
   * Compression: lzo
   */
  public static final String COMPRESSION_LZO = "lzo";

  /**
   * compression: none
   */
  public static final String COMPRESSION_NONE = "none";

  /**
   * Compression algorithms. There is a static initializer, below the values defined in the
   * enumeration, that calls the initializer of all defined codecs within {@link Algorithm}. This
   * promotes a model of the following call graph of initialization by the static initializer,
   * followed by calls to {@link #getCodec()},
   * {@link #createCompressionStream(OutputStream, Compressor, int)}, and
   * {@link #createDecompressionStream(InputStream, Decompressor, int)}. In some cases, the
   * compression and decompression call methods will include a different buffer size for the stream.
   * Note that if the compressed buffer size requested in these calls is zero, we will not set the
   * buffer size for that algorithm. Instead, we will use the default within the codec.
   * <p>
   * The buffer size is configured in the Codec by way of a Hadoop {@link Configuration} reference.
   * One approach may be to use the same Configuration object, but when calls are made to
   * {@code createCompressionStream} and {@code createDecompressionStream} with non default buffer
   * sizes, the configuration object must be changed. In this case, concurrent calls to
   * {@code createCompressionStream} and {@code createDecompressionStream} would mutate the
   * configuration object beneath each other, requiring synchronization to avoid undesirable
   * activity via co-modification. To avoid synchronization entirely, we will create Codecs with
   * their own Configuration object and cache them for re-use. A default codec will be statically
   * created, as mentioned above to ensure we always have a codec available at loader
   * initialization.
   * <p>
   * There is a Guava cache defined within Algorithm that allows us to cache Codecs for re-use.
   * Since they will have their own configuration object and thus do not need to be mutable, there
   * is no concern for using them concurrently; however, the Guava cache exists to ensure a maximal
   * size of the cache and efficient and concurrent read/write access to the cache itself.
   * <p>
   * To provide Algorithm specific details and to describe what is in code:
   * <p>
   * LZO will always have the default LZO codec because the buffer size is never overridden within
   * it.
   * <p>
   * GZ will use the default GZ codec for the compression stream, but can potentially use a
   * different codec instance for the decompression stream if the requested buffer size does not
   * match the default GZ buffer size of 32k.
   * <p>
   * Snappy will use the default Snappy codec with the default buffer size of 64k for the
   * compression stream, but will use a cached codec if the buffer size differs from the default.
   */
  public enum Algorithm {

    LZO(COMPRESSION_LZO) {

      /**
       * The default codec class.
       */
      private static final String DEFAULT_CLAZZ = "org.apache.hadoop.io.compress.LzoCodec";

      /**
       * Configuration option for LZO buffer size.
       */
      private static final String BUFFER_SIZE_OPT = "io.compression.codec.lzo.buffersize";

      /**
       * Default buffer size.
       */
      private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

      /**
       * Whether or not the codec status has been checked. Ensures the default codec is not
       * recreated.
       */
      private final AtomicBoolean checked = new AtomicBoolean(false);

      private transient CompressionCodec codec = null;

      @Override
      public boolean isSupported() {
        return codec != null;
      }

      @Override
      public void initializeDefaultCodec() {
        codec = initCodec(checked, DEFAULT_BUFFER_SIZE, codec);
      }

      @Override
      CompressionCodec createNewCodec(int bufferSize) {
        return createNewCodec(CONF_LZO_CLASS, DEFAULT_CLAZZ, bufferSize, BUFFER_SIZE_OPT);
      }

      @Override
      CompressionCodec getCodec() {
        return codec;
      }

      @Override
      public InputStream createDecompressionStream(InputStream downStream,
          Decompressor decompressor, int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException("LZO codec class not specified. Did you forget to set property "
              + CONF_LZO_CLASS + "?");
        }
        InputStream bis = bufferStream(downStream, downStreamBufferSize);
        CompressionInputStream cis = codec.createInputStream(bis, decompressor);
        return new BufferedInputStream(cis, DATA_IBUF_SIZE);
      }

      @Override
      public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException("LZO codec class not specified. Did you forget to set property "
              + CONF_LZO_CLASS + "?");
        }
        return createFinishedOnFlushCompressionStream(downStream, compressor, downStreamBufferSize);
      }

    },

    GZ(COMPRESSION_GZ) {

      private transient DefaultCodec codec = null;

      /**
       * Configuration option for gz buffer size
       */
      private static final String BUFFER_SIZE_OPT = "io.file.buffer.size";

      /**
       * Default buffer size
       */
      private static final int DEFAULT_BUFFER_SIZE = 32 * 1024;

      @Override
      CompressionCodec getCodec() {
        return codec;
      }

      @Override
      public void initializeDefaultCodec() {
        codec = (DefaultCodec) createNewCodec(DEFAULT_BUFFER_SIZE);
      }

      /**
       * Creates a new GZ codec
       */
      @Override
      protected CompressionCodec createNewCodec(final int bufferSize) {
        Configuration newConfig = new Configuration(conf);
        updateBuffer(conf, BUFFER_SIZE_OPT, bufferSize);
        DefaultCodec newCodec = new DefaultCodec();
        newCodec.setConf(newConfig);
        return newCodec;
      }

      @Override
      public InputStream createDecompressionStream(InputStream downStream,
          Decompressor decompressor, int downStreamBufferSize) throws IOException {
        return createDecompressionStream(downStream, decompressor, downStreamBufferSize,
            DEFAULT_BUFFER_SIZE, GZ, codec);
      }

      @Override
      public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        return createFinishedOnFlushCompressionStream(downStream, compressor, downStreamBufferSize);
      }

      @Override
      public boolean isSupported() {
        return true;
      }
    },

    NONE(COMPRESSION_NONE) {
      @Override
      CompressionCodec getCodec() {
        return null;
      }

      @Override
      public InputStream createDecompressionStream(InputStream downStream,
          Decompressor decompressor, int downStreamBufferSize) {
        return bufferStream(downStream, downStreamBufferSize);
      }

      @Override
      public void initializeDefaultCodec() {}

      @Override
      protected CompressionCodec createNewCodec(final int bufferSize) {
        return null;
      }

      @Override
      public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) {
        return bufferStream(downStream, downStreamBufferSize);
      }

      @Override
      public boolean isSupported() {
        return true;
      }
    },

    SNAPPY(COMPRESSION_SNAPPY) {

      /**
       * The default codec class.
       */
      private static final String DEFAULT_CLAZZ = "org.apache.hadoop.io.compress.SnappyCodec";

      /**
       * Configuration option for LZO buffer size.
       */
      private static final String BUFFER_SIZE_OPT = "io.compression.codec.snappy.buffersize";

      /**
       * Default buffer size.
       */
      private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

      /**
       * Whether or not the codec status has been checked. Ensures the default codec is not
       * recreated.
       */
      private final AtomicBoolean checked = new AtomicBoolean(false);

      private transient CompressionCodec codec = null;

      @Override
      public CompressionCodec getCodec() {
        return codec;
      }

      @Override
      public void initializeDefaultCodec() {
        codec = initCodec(checked, DEFAULT_BUFFER_SIZE, codec);
      }

      /**
       * Creates a new snappy codec.
       */
      @Override
      protected CompressionCodec createNewCodec(final int bufferSize) {
        return createNewCodec(CONF_SNAPPY_CLASS, DEFAULT_CLAZZ, bufferSize, BUFFER_SIZE_OPT);
      }

      @Override
      public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException("SNAPPY codec class not specified. Did you forget to set property "
              + CONF_SNAPPY_CLASS + "?");
        }
        return createFinishedOnFlushCompressionStream(downStream, compressor, downStreamBufferSize);
      }

      @Override
      public InputStream createDecompressionStream(InputStream downStream,
          Decompressor decompressor, int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException("SNAPPY codec class not specified. Did you forget to set property "
              + CONF_SNAPPY_CLASS + "?");
        }
        return createDecompressionStream(downStream, decompressor, downStreamBufferSize,
            DEFAULT_BUFFER_SIZE, SNAPPY, codec);
      }

      @Override
      public boolean isSupported() {
        return codec != null;
      }
    },

    ZSTANDARD(COMPRESSION_ZSTD) {

      /**
       * The default codec class.
       */
      private static final String DEFAULT_CLAZZ = "org.apache.hadoop.io.compress.ZStandardCodec";

      /**
       * Configuration option for LZO buffer size.
       */
      private static final String BUFFER_SIZE_OPT = "io.compression.codec.zstd.buffersize";

      /**
       * Default buffer size.
       */
      private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

      /**
       * Whether or not the codec status has been checked. Ensures the default codec is not
       * recreated.
       */
      private final AtomicBoolean checked = new AtomicBoolean(false);

      private transient CompressionCodec codec = null;

      @Override
      public CompressionCodec getCodec() {
        return codec;
      }

      @Override
      public void initializeDefaultCodec() {
        codec = initCodec(checked, DEFAULT_BUFFER_SIZE, codec);
      }

      /**
       * Creates a new ZStandard codec.
       */
      @Override
      protected CompressionCodec createNewCodec(final int bufferSize) {
        return createNewCodec(CONF_ZSTD_CLASS, DEFAULT_CLAZZ, bufferSize, BUFFER_SIZE_OPT);
      }

      @Override
      public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException(
              "ZStandard codec class not specified. Did you forget to set property "
                  + CONF_ZSTD_CLASS + "?");
        }
        return createFinishedOnFlushCompressionStream(downStream, compressor, downStreamBufferSize);
      }

      @Override
      public InputStream createDecompressionStream(InputStream downStream,
          Decompressor decompressor, int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException(
              "ZStandard codec class not specified. Did you forget to set property "
                  + CONF_ZSTD_CLASS + "?");
        }
        return createDecompressionStream(downStream, decompressor, downStreamBufferSize,
            DEFAULT_BUFFER_SIZE, ZSTANDARD, codec);
      }

      @Override
      public boolean isSupported() {
        return codec != null;
      }
    };

    /**
     * Guava cache to have a limited factory pattern defined in the Algorithm enum.
     */
    private static LoadingCache<Entry<Algorithm,Integer>,CompressionCodec> codecCache =
        CacheBuilder.newBuilder().maximumSize(25).build(new CacheLoader<>() {
          @Override
          public CompressionCodec load(Entry<Algorithm,Integer> key) {
            return key.getKey().createNewCodec(key.getValue());
          }
        });

    public static final String CONF_LZO_CLASS = "io.compression.codec.lzo.class";
    public static final String CONF_SNAPPY_CLASS = "io.compression.codec.snappy.class";
    public static final String CONF_ZSTD_CLASS = "io.compression.codec.zstd.class";

    // All compression-related settings are required to be configured statically in the
    // Configuration object.
    protected static final Configuration conf;

    // The model defined by the static block below creates a singleton for each defined codec in the
    // Algorithm enumeration. By creating the codecs, each call to isSupported shall return
    // true/false depending on if the codec singleton is defined. The static initializer, below,
    // will ensure this occurs when the Enumeration is loaded. Furthermore, calls to getCodec will
    // return the singleton, whether it is null or not.
    //
    // Calls to createCompressionStream and createDecompressionStream may return a different codec
    // than getCodec, if the incoming downStreamBufferSize is different than the default. In such a
    // case, we will place the resulting codec into the codecCache, defined below, to ensure we have
    // cache codecs.
    //
    // Since codecs are immutable, there is no concern about concurrent access to the
    // CompressionCodec objects within the guava cache.
    static {
      conf = new Configuration();
      for (final Algorithm al : Algorithm.values()) {
        al.initializeDefaultCodec();
      }
    }

    // Data input buffer size to absorb small reads from application.
    private static final int DATA_IBUF_SIZE = 1024;

    // Data output buffer size to absorb small writes from application.
    private static final int DATA_OBUF_SIZE = 4 * 1024;

    // The name of the compression algorithm.
    private final String name;

    Algorithm(String name) {
      this.name = name;
    }

    public abstract InputStream createDecompressionStream(InputStream downStream,
        Decompressor decompressor, int downStreamBufferSize) throws IOException;

    public abstract OutputStream createCompressionStream(OutputStream downStream,
        Compressor compressor, int downStreamBufferSize) throws IOException;

    public abstract boolean isSupported();

    abstract CompressionCodec getCodec();

    /**
     * Create the default codec object.
     */
    abstract void initializeDefaultCodec();

    /**
     * Shared function to create new codec objects. It is expected that if buffersize is invalid, a
     * codec will be created with the default buffer size.
     */
    abstract CompressionCodec createNewCodec(int bufferSize);

    public Compressor getCompressor() {
      CompressionCodec codec = getCodec();
      if (codec != null) {
        Compressor compressor = CodecPool.getCompressor(codec);
        if (compressor != null) {
          if (compressor.finished()) {
            // Somebody returns the compressor to CodecPool but is still using it.
            log.warn("Compressor obtained from CodecPool already finished()");
          } else {
            log.trace("Got a compressor: {}", compressor.hashCode());
          }
          // The following statement is necessary to get around bugs in 0.18 where a compressor is
          // referenced after it's
          // returned back to the codec pool.
          compressor.reset();
        }
        return compressor;
      }
      return null;
    }

    public void returnCompressor(final Compressor compressor) {
      if (compressor != null) {
        log.trace("Return a compressor: {}", compressor.hashCode());
        CodecPool.returnCompressor(compressor);
      }
    }

    public Decompressor getDecompressor() {
      CompressionCodec codec = getCodec();
      if (codec != null) {
        Decompressor decompressor = CodecPool.getDecompressor(codec);
        if (decompressor != null) {
          if (decompressor.finished()) {
            // Somebody returns the decompressor to CodecPool but is still using it.
            log.warn("Decompressor obtained from CodecPool already finished()");
          } else {
            log.trace("Got a decompressor: {}", decompressor.hashCode());
          }
          // The following statement is necessary to get around bugs in 0.18 where a decompressor is
          // referenced after
          // it's returned back to the codec pool.
          decompressor.reset();
        }
        return decompressor;
      }
      return null;
    }

    /**
     * Returns the specified {@link Decompressor} to the codec cache if it is not null.
     */
    public void returnDecompressor(final Decompressor decompressor) {
      if (decompressor != null) {
        log.trace("Returned a decompressor: {}", decompressor.hashCode());
        CodecPool.returnDecompressor(decompressor);
      }
    }

    /**
     * Returns the name of the compression algorithm.
     *
     * @return the name
     */
    public String getName() {
      return name;
    }

    /**
     * Initializes and returns a new codec with the specified buffer size if and only if the
     * specified {@link AtomicBoolean} has a value of false, or returns the specified original coded
     * otherwise.
     */
    CompressionCodec initCodec(final AtomicBoolean checked, final int bufferSize,
        final CompressionCodec originalCodec) {
      if (!checked.get()) {
        checked.set(true);
        return createNewCodec(bufferSize);
      }
      return originalCodec;
    }

    /**
     * Returns a new {@link CompressionCodec} of the specified type, or the default type if no
     * primary type is specified. If the specified buffer size is greater than 0, the specified
     * buffer size configuration option will be updated in the codec's configuration with the buffer
     * size. If the neither the specified codec type or the default codec type can be found, null
     * will be returned.
     */
    CompressionCodec createNewCodec(final String codecClazzProp, final String defaultClazz,
        final int bufferSize, final String bufferSizeConfigOpt) {
      String extClazz =
          (conf.get(codecClazzProp) == null ? System.getProperty(codecClazzProp) : null);
      String clazz = (extClazz != null) ? extClazz : defaultClazz;
      try {
        log.info("Trying to load codec class {} for {}", clazz, codecClazzProp);
        Configuration config = new Configuration(conf);
        updateBuffer(config, bufferSizeConfigOpt, bufferSize);
        return (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), config);
      } catch (ClassNotFoundException e) {
        // This is okay.
      }
      return null;
    }

    InputStream createDecompressionStream(final InputStream stream, final Decompressor decompressor,
        final int bufferSize, final int defaultBufferSize, final Algorithm algorithm,
        CompressionCodec codec) throws IOException {
      // If the default buffer size is not being used, pull from the loading cache.
      if (bufferSize != defaultBufferSize) {
        Entry<Algorithm,Integer> sizeOpt = Maps.immutableEntry(algorithm, bufferSize);
        try {
          codec = codecCache.get(sizeOpt);
        } catch (ExecutionException e) {
          throw new IOException(e);
        }
      }
      CompressionInputStream cis = codec.createInputStream(stream, decompressor);
      return new BufferedInputStream(cis, DATA_IBUF_SIZE);
    }

    /**
     * Returns a new {@link FinishOnFlushCompressionStream} initialized for the specified output
     * stream and compressor.
     */
    OutputStream createFinishedOnFlushCompressionStream(final OutputStream downStream,
        final Compressor compressor, final int downStreamBufferSize) throws IOException {
      OutputStream out = bufferStream(downStream, downStreamBufferSize);
      CompressionOutputStream cos = getCodec().createOutputStream(out, compressor);
      return new BufferedOutputStream(new FinishOnFlushCompressionStream(cos), DATA_OBUF_SIZE);
    }

    /**
     * Return the given stream wrapped as a {@link BufferedOutputStream} with the given buffer size
     * if the buffer size is greater than 0, or return the original stream otherwise.
     */
    OutputStream bufferStream(final OutputStream stream, final int bufferSize) {
      if (bufferSize > 0) {
        return new BufferedOutputStream(stream, bufferSize);
      }
      return stream;
    }

    /**
     * Return the given stream wrapped as a {@link BufferedInputStream} with the given buffer size
     * if the buffer size is greater than 0, or return the original stream otherwise.
     */
    InputStream bufferStream(final InputStream stream, final int bufferSize) {
      if (bufferSize > 0) {
        return new BufferedInputStream(stream, bufferSize);
      }
      return stream;
    }

    /**
     * Updates the value of the specified buffer size opt in the given {@link Configuration} if the
     * new buffer size is greater than 0.
     */
    void updateBuffer(final Configuration config, final String bufferSizeOpt,
        final int bufferSize) {
      // Use the buffersize only if it is greater than 0, otherwise use the default defined within
      // the codec.
      if (bufferSize > 0) {
        config.setInt(bufferSizeOpt, bufferSize);
      }
    }
  }

  public static String[] getSupportedAlgorithms() {
    Algorithm[] algorithms = Algorithm.class.getEnumConstants();
    ArrayList<String> supportedAlgorithms = new ArrayList<>();
    for (Algorithm algorithm : algorithms) {
      if (algorithm.isSupported()) {
        supportedAlgorithms.add(algorithm.getName());
      }
    }
    return supportedAlgorithms.toArray(new String[0]);
  }

  static Algorithm getCompressionAlgorithmByName(final String name) {
    Algorithm[] algorithms = Algorithm.class.getEnumConstants();
    for (Algorithm algorithm : algorithms) {
      if (algorithm.getName().equals(name)) {
        return algorithm;
      }
    }
    throw new IllegalArgumentException("Unsupported compression algorithm name: " + name);
  }
}
