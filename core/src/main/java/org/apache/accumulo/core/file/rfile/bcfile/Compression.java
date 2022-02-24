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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.spi.file.rfile.compression.Algorithm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;

/**
 * Compression related stuff.
 */
public final class Compression {

  /**
   * Prevent the instantiation of this class.
   */
  private Compression() {
    throw new UnsupportedOperationException();
  }

  /**
   * Compression: bzip2
   */
  public static final String COMPRESSION_BZIP2 = "bzip2";

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
   * Compression: lz4
   */
  public static final String COMPRESSION_LZ4 = "lz4";

  /**
   * compression: none
   */
  public static final String COMPRESSION_NONE = "none";

  public static class Bzip2 extends Algorithm {

    public Bzip2() {
      super(COMPRESSION_BZIP2);
    }

    /**
     * The default codec class.
     */
    private static final String DEFAULT_CLAZZ = "org.apache.hadoop.io.compress.BZip2Codec";

    /**
     * Configuration option for BZip2 buffer size. Uses the default FS buffer size.
     */
    private static final String BUFFER_SIZE_OPT = "io.file.buffer.size";

    /**
     * Default buffer size. Changed from default of 4096.
     */
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    /**
     * Whether or not the codec status has been checked. Ensures the default codec is not recreated.
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
    public CompressionCodec createNewCodec(int bufferSize) {
      return createNewCodec(CONF_BZIP2_CLASS, DEFAULT_CLAZZ, bufferSize, BUFFER_SIZE_OPT);
    }

    @Override
    public CompressionCodec getCodec() {
      return codec;
    }

    @Override
    public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor,
        int downStreamBufferSize) throws IOException {
      if (!isSupported()) {
        throw new IOException("BZip2 codec class not specified. Did you forget to set property "
            + CONF_BZIP2_CLASS + "?");
      }
      InputStream bis = bufferStream(downStream, downStreamBufferSize);
      CompressionInputStream cis = codec.createInputStream(bis, decompressor);
      return new BufferedInputStream(cis, DATA_IBUF_SIZE);
    }

    @Override
    public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor,
        int downStreamBufferSize) throws IOException {
      if (!isSupported()) {
        throw new IOException("BZip2 codec class not specified. Did you forget to set property "
            + CONF_BZIP2_CLASS + "?");
      }
      return createFinishedOnFlushCompressionStream(downStream, compressor, downStreamBufferSize);
    }

  }

  public static class Lzo extends Algorithm {

    public Lzo() {
      super(COMPRESSION_LZO);
    }

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
     * Whether or not the codec status has been checked. Ensures the default codec is not recreated.
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
    public CompressionCodec createNewCodec(int bufferSize) {
      return createNewCodec(CONF_LZO_CLASS, DEFAULT_CLAZZ, bufferSize, BUFFER_SIZE_OPT);
    }

    @Override
    public CompressionCodec getCodec() {
      return codec;
    }

    @Override
    public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor,
        int downStreamBufferSize) throws IOException {
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

  }

  public static class Lz4 extends Algorithm {

    public Lz4() {
      super(COMPRESSION_LZ4);
    }

    /**
     * The default codec class.
     */
    private static final String DEFAULT_CLAZZ = "org.apache.hadoop.io.compress.Lz4Codec";

    /**
     * Configuration option for LZ4 buffer size.
     */
    private static final String BUFFER_SIZE_OPT = "io.compression.codec.lz4.buffersize";

    /**
     * Default buffer size.
     */
    private static final int DEFAULT_BUFFER_SIZE = 256 * 1024;

    /**
     * Whether or not the codec status has been checked. Ensures the default codec is not recreated.
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
    public CompressionCodec createNewCodec(int bufferSize) {
      return createNewCodec(CONF_LZ4_CLASS, DEFAULT_CLAZZ, bufferSize, BUFFER_SIZE_OPT);
    }

    @Override
    public CompressionCodec getCodec() {
      return codec;
    }

    @Override
    public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor,
        int downStreamBufferSize) throws IOException {
      if (!isSupported()) {
        throw new IOException("LZ4 codec class not specified. Did you forget to set property "
            + CONF_LZ4_CLASS + "?");
      }
      InputStream bis = bufferStream(downStream, downStreamBufferSize);
      CompressionInputStream cis = codec.createInputStream(bis, decompressor);
      return new BufferedInputStream(cis, DATA_IBUF_SIZE);
    }

    @Override
    public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor,
        int downStreamBufferSize) throws IOException {
      if (!isSupported()) {
        throw new IOException("LZ4 codec class not specified. Did you forget to set property "
            + CONF_LZ4_CLASS + "?");
      }
      return createFinishedOnFlushCompressionStream(downStream, compressor, downStreamBufferSize);
    }

  }

  public static class Gz extends Algorithm {

    public Gz() {
      super(COMPRESSION_GZ);
    }

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
    public CompressionCodec getCodec() {
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
    public CompressionCodec createNewCodec(final int bufferSize) {
      Configuration newConfig = new Configuration(conf);
      if (bufferSize > 0) {
        conf.setInt(BUFFER_SIZE_OPT, bufferSize);
      }
      DefaultCodec newCodec = new DefaultCodec();
      newCodec.setConf(newConfig);
      return newCodec;
    }

    @Override
    public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor,
        int downStreamBufferSize) throws IOException {
      return createDecompressionStream(downStream, decompressor, downStreamBufferSize,
          DEFAULT_BUFFER_SIZE, this, codec);
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
  }

  public static class None extends Algorithm {

    public None() {
      super(COMPRESSION_NONE);
    }

    @Override
    public CompressionCodec getCodec() {
      return null;
    }

    @Override
    public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor,
        int downStreamBufferSize) {
      return bufferStream(downStream, downStreamBufferSize);
    }

    @Override
    public void initializeDefaultCodec() {}

    @Override
    public CompressionCodec createNewCodec(final int bufferSize) {
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
  }

  public static class Snappy extends Algorithm {

    public Snappy() {
      super(COMPRESSION_SNAPPY);
    }

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
     * Whether or not the codec status has been checked. Ensures the default codec is not recreated.
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
    public CompressionCodec createNewCodec(final int bufferSize) {
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
    public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor,
        int downStreamBufferSize) throws IOException {
      if (!isSupported()) {
        throw new IOException("SNAPPY codec class not specified. Did you forget to set property "
            + CONF_SNAPPY_CLASS + "?");
      }
      return createDecompressionStream(downStream, decompressor, downStreamBufferSize,
          DEFAULT_BUFFER_SIZE, this, codec);
    }

    @Override
    public boolean isSupported() {
      return codec != null;
    }
  }

  public static class ZStandard extends Algorithm {

    public ZStandard() {
      super(COMPRESSION_ZSTD);
    }

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
     * Whether or not the codec status has been checked. Ensures the default codec is not recreated.
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
    public CompressionCodec createNewCodec(final int bufferSize) {
      return createNewCodec(CONF_ZSTD_CLASS, DEFAULT_CLAZZ, bufferSize, BUFFER_SIZE_OPT);
    }

    @Override
    public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor,
        int downStreamBufferSize) throws IOException {
      if (!isSupported()) {
        throw new IOException("ZStandard codec class not specified. Did you forget to set property "
            + CONF_ZSTD_CLASS + "?");
      }
      return createFinishedOnFlushCompressionStream(downStream, compressor, downStreamBufferSize);
    }

    @Override
    public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor,
        int downStreamBufferSize) throws IOException {
      if (!isSupported()) {
        throw new IOException("ZStandard codec class not specified. Did you forget to set property "
            + CONF_ZSTD_CLASS + "?");
      }
      return createDecompressionStream(downStream, decompressor, downStreamBufferSize,
          DEFAULT_BUFFER_SIZE, this, codec);
    }

    @Override
    public boolean isSupported() {
      return codec != null;
    }
  }

  public static final String CONF_BZIP2_CLASS = "io.compression.codec.bzip2.class";
  public static final String CONF_LZO_CLASS = "io.compression.codec.lzo.class";
  public static final String CONF_LZ4_CLASS = "io.compression.codec.lz4.class";
  public static final String CONF_SNAPPY_CLASS = "io.compression.codec.snappy.class";
  public static final String CONF_ZSTD_CLASS = "io.compression.codec.zstd.class";

  private static final Map<String,Algorithm> CONFIGURED_ALGORITHMS = new HashMap<>();

  private static final ServiceLoader<Algorithm> CUSTOM_ALGORITHMS =
      ServiceLoader.load(Algorithm.class);

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

    // Add known set of Algorithms
    Algorithm bzip = new Bzip2();
    bzip.setHadoopConfiguration(conf);
    bzip.initializeDefaultCodec();
    CONFIGURED_ALGORITHMS.put(bzip.getName(), bzip);

    Algorithm gz = new Gz();
    gz.setHadoopConfiguration(conf);
    gz.initializeDefaultCodec();
    CONFIGURED_ALGORITHMS.put(gz.getName(), gz);

    Algorithm lz4 = new Lz4();
    lz4.setHadoopConfiguration(conf);
    lz4.initializeDefaultCodec();
    CONFIGURED_ALGORITHMS.put(lz4.getName(), lz4);

    Algorithm lzo = new Lzo();
    lzo.setHadoopConfiguration(conf);
    lzo.initializeDefaultCodec();
    CONFIGURED_ALGORITHMS.put(lzo.getName(), lzo);

    Algorithm none = new None();
    none.setHadoopConfiguration(conf);
    none.initializeDefaultCodec();
    CONFIGURED_ALGORITHMS.put(none.getName(), none);

    Algorithm snappy = new Snappy();
    snappy.setHadoopConfiguration(conf);
    snappy.initializeDefaultCodec();
    CONFIGURED_ALGORITHMS.put(snappy.getName(), snappy);

    Algorithm zstd = new ZStandard();
    zstd.setHadoopConfiguration(conf);
    zstd.initializeDefaultCodec();
    CONFIGURED_ALGORITHMS.put(zstd.getName(), zstd);

    CUSTOM_ALGORITHMS.forEach(algo -> {
      algo.setHadoopConfiguration(conf);
      algo.initializeDefaultCodec();
      CONFIGURED_ALGORITHMS.put(algo.getName(), algo);
    });

  }

  public static String[] getSupportedAlgorithms() {
    ArrayList<String> supportedAlgorithms = new ArrayList<>();
    CONFIGURED_ALGORITHMS.forEach((k, v) -> {
      if (v.isSupported()) {
        supportedAlgorithms.add(k);
      }
    });
    return supportedAlgorithms.toArray(new String[0]);
  }

  public static Algorithm getCompressionAlgorithmByName(final String name) {
    Algorithm algorithm = CONFIGURED_ALGORITHMS.get(name);
    if (algorithm != null) {
      return algorithm;
    }
    throw new IllegalArgumentException("Unsupported compression algorithm name: " + name);
  }
}
