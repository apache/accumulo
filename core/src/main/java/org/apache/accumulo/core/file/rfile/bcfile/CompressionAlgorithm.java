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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.spi.file.rfile.compression.CompressionAlgorithmConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;

/**
 * There is a static initializer in {@link Compression} that finds all implementations of
 * {@link CompressionAlgorithmConfiguration} and initializes a {@link CompressionAlgorithm}
 * instance. This promotes a model of the following call graph of initialization by the static
 * initializer, followed by calls to {@link #getCodec()},
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
 * configuration object beneath each other, requiring synchronization to avoid undesirable activity
 * via co-modification. To avoid synchronization entirely, we will create Codecs with their own
 * Configuration object and cache them for re-use. A default codec will be statically created, as
 * mentioned above to ensure we always have a codec available at loader initialization.
 * <p>
 * There is a Guava cache defined within Algorithm that allows us to cache Codecs for re-use. Since
 * they will have their own configuration object and thus do not need to be mutable, there is no
 * concern for using them concurrently; however, the Guava cache exists to ensure a maximal size of
 * the cache and efficient and concurrent read/write access to the cache itself.
 * <p>
 * To provide Algorithm specific details and to describe what is in code:
 * <p>
 * LZO will always have the default LZO codec because the buffer size is never overridden within it.
 * <p>
 * LZ4 will always have the default LZ4 codec because the buffer size is never overridden within it.
 * <p>
 * GZ will use the default GZ codec for the compression stream, but can potentially use a different
 * codec instance for the decompression stream if the requested buffer size does not match the
 * default GZ buffer size of 32k.
 * <p>
 * Snappy will use the default Snappy codec with the default buffer size of 64k for the compression
 * stream, but will use a cached codec if the buffer size differs from the default.
 */
public class CompressionAlgorithm extends Configured {

  public static class FinishOnFlushCompressionStream extends FilterOutputStream {

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

  private static final Logger LOG = LoggerFactory.getLogger(CompressionAlgorithm.class);

  /**
   * Guava cache to have a limited factory pattern defined in the Algorithm enum.
   */
  private static LoadingCache<Entry<CompressionAlgorithm,Integer>,CompressionCodec> codecCache =
      CacheBuilder.newBuilder().maximumSize(25).build(new CacheLoader<>() {
        @Override
        public CompressionCodec load(Entry<CompressionAlgorithm,Integer> key) {
          return key.getKey().createNewCodec(key.getValue());
        }
      });

  // Data input buffer size to absorb small reads from application.
  protected static final int DATA_IBUF_SIZE = 1024;

  // Data output buffer size to absorb small writes from application.
  protected static final int DATA_OBUF_SIZE = 4 * 1024;

  // The name of the compression algorithm.
  private final CompressionAlgorithmConfiguration algorithm;

  private final AtomicBoolean checked = new AtomicBoolean(false);

  private transient CompressionCodec codec = null;

  public CompressionAlgorithm(CompressionAlgorithmConfiguration algorithm, Configuration conf) {
    this.algorithm = algorithm;
    setConf(conf);
    codec = initCodec(checked, algorithm.getDefaultBufferSize(), codec);
  }

  /**
   * Shared function to create new codec objects. It is expected that if buffersize is invalid, a
   * codec will be created with the default buffer size.
   */
  CompressionCodec createNewCodec(int bufferSize) {
    return createNewCodec(algorithm.getCodecClassNameProperty(), algorithm.getCodecClassName(),
        bufferSize, algorithm.getBufferSizeProperty());
  }

  public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor,
      int downStreamBufferSize) throws IOException {
    if (!isSupported()) {
      throw new IOException("codec class not specified. Did you forget to set property "
          + algorithm.getCodecClassNameProperty() + "?");
    }
    if (algorithm.cacheCodecsWithNonDefaultSizes()) {
      return createDecompressionStream(downStream, decompressor, downStreamBufferSize,
          algorithm.getDefaultBufferSize(), this, codec);
    } else {
      InputStream bis = bufferStream(downStream, downStreamBufferSize);
      CompressionInputStream cis = codec.createInputStream(bis, decompressor);
      return new BufferedInputStream(cis, DATA_IBUF_SIZE);
    }
  }

  private InputStream createDecompressionStream(final InputStream stream,
      final Decompressor decompressor, final int bufferSize, final int defaultBufferSize,
      final CompressionAlgorithm algorithm, CompressionCodec codec) throws IOException {
    // If the default buffer size is not being used, pull from the loading cache.
    if (bufferSize != defaultBufferSize) {
      Entry<CompressionAlgorithm,Integer> sizeOpt = Maps.immutableEntry(algorithm, bufferSize);
      try {
        codec = codecCache.get(sizeOpt);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }
    CompressionInputStream cis = codec.createInputStream(stream, decompressor);
    return new BufferedInputStream(cis, DATA_IBUF_SIZE);
  }

  public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor,
      int downStreamBufferSize) throws IOException {
    if (!isSupported()) {
      throw new IOException("codec class not specified. Did you forget to set property "
          + algorithm.getCodecClassNameProperty() + "?");
    }
    return createFinishedOnFlushCompressionStream(downStream, compressor, downStreamBufferSize);

  }

  boolean isSupported() {
    return codec != null;
  }

  CompressionCodec getCodec() {
    return codec;
  }

  public Compressor getCompressor() {
    CompressionCodec codec = getCodec();
    if (codec != null) {
      Compressor compressor = CodecPool.getCompressor(codec);
      if (compressor != null) {
        if (compressor.finished()) {
          // Somebody returns the compressor to CodecPool but is still using it.
          LOG.warn("Compressor obtained from CodecPool already finished()");
        } else {
          LOG.trace("Got a compressor: {}", compressor.hashCode());
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
      LOG.trace("Return a compressor: {}", compressor.hashCode());
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
          LOG.warn("Decompressor obtained from CodecPool already finished()");
        } else {
          LOG.trace("Got a decompressor: {}", decompressor.hashCode());
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
      LOG.trace("Returned a decompressor: {}", decompressor.hashCode());
      CodecPool.returnDecompressor(decompressor);
    }
  }

  /**
   * Returns the name of the compression algorithm.
   *
   * @return the name
   */
  public String getName() {
    return algorithm.getName();
  }

  /**
   * Initializes and returns a new codec with the specified buffer size if and only if the specified
   * {@link AtomicBoolean} has a value of false, or returns the specified original coded otherwise.
   */
  private CompressionCodec initCodec(final AtomicBoolean checked, final int bufferSize,
      final CompressionCodec originalCodec) {
    if (!checked.get()) {
      checked.set(true);
      return createNewCodec(bufferSize);
    }
    return originalCodec;
  }

  /**
   * Returns a new {@link CompressionCodec} of the specified type, or the default type if no primary
   * type is specified. If the specified buffer size is greater than 0, the specified buffer size
   * configuration option will be updated in the codec's configuration with the buffer size. If the
   * neither the specified codec type or the default codec type can be found, null will be returned.
   */
  private CompressionCodec createNewCodec(final String codecClazzProp, final String defaultClazz,
      final int bufferSize, final String bufferSizeConfigOpt) {
    String clazz = defaultClazz;
    if (codecClazzProp != null) {
      clazz = System.getProperty(codecClazzProp, getConf().get(codecClazzProp, defaultClazz));
    }
    try {
      LOG.info("Trying to load codec class {}", clazz);
      Configuration config = new Configuration(getConf());
      updateBuffer(config, bufferSizeConfigOpt, bufferSize);
      return (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), config);
    } catch (ClassNotFoundException e) {
      LOG.debug(
          "ClassNotFoundException creating codec class {} for {}. Enable trace logging for stacktrace.",
          clazz, codecClazzProp);
      LOG.trace("Unable to load codec class due to ", e);
    }
    return null;
  }

  /**
   * Returns a new {@link FinishOnFlushCompressionStream} initialized for the specified output
   * stream and compressor.
   */
  private OutputStream createFinishedOnFlushCompressionStream(final OutputStream downStream,
      final Compressor compressor, final int downStreamBufferSize) throws IOException {
    OutputStream out = bufferStream(downStream, downStreamBufferSize);
    CompressionOutputStream cos = getCodec().createOutputStream(out, compressor);
    return new BufferedOutputStream(new FinishOnFlushCompressionStream(cos), DATA_OBUF_SIZE);
  }

  /**
   * Return the given stream wrapped as a {@link BufferedOutputStream} with the given buffer size if
   * the buffer size is greater than 0, or return the original stream otherwise.
   */
  private OutputStream bufferStream(final OutputStream stream, final int bufferSize) {
    if (bufferSize > 0) {
      return new BufferedOutputStream(stream, bufferSize);
    }
    return stream;
  }

  /**
   * Return the given stream wrapped as a {@link BufferedInputStream} with the given buffer size if
   * the buffer size is greater than 0, or return the original stream otherwise.
   */
  private InputStream bufferStream(final InputStream stream, final int bufferSize) {
    if (bufferSize > 0) {
      return new BufferedInputStream(stream, bufferSize);
    }
    return stream;
  }

  /**
   * Updates the value of the specified buffer size opt in the given {@link Configuration} if the
   * new buffer size is greater than 0.
   */
  private void updateBuffer(final Configuration config, final String bufferSizeOpt,
      final int bufferSize) {
    // Use the buffersize only if it is greater than 0, otherwise use the default defined within
    // the codec.
    if (bufferSize > 0) {
      config.setInt(bufferSizeOpt, bufferSize);
    }
  }
}
