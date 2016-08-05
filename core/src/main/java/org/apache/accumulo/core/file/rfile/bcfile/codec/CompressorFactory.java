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
package org.apache.accumulo.core.file.rfile.bcfile.codec;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Compressor Factory is a base class which creates compressors based on the supplied algorithm. Extensions may allow for alternative factory methods, such as
 * object pooling.
 */
public class CompressorFactory implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(CompressorFactory.class);

  public CompressorFactory(AccumuloConfiguration acuConf) {}

  /**
   * Provides the caller a compressor object.
   *
   * @param compressionAlgorithm
   *          compressor's algorithm.
   * @return compressor.
   * @throws IOException
   *           I/O Exception during factory implementation
   */
  public Compressor getCompressor(Algorithm compressionAlgorithm) throws IOException {
    if (compressionAlgorithm != null) {
      Compressor compressor = compressionAlgorithm.getCodec().createCompressor();
      if (compressor != null) {

        LOG.debug("Got a decompressor: " + compressor.hashCode());

      }
      return compressor;
    }
    return null;
  }

  /**
   * Method to release a compressor. This implementation will call end on the compressor.
   *
   * @param algorithm
   *          Supplied compressor's Algorithm.
   * @param compressor
   *          Compressor object
   */
  public void releaseCompressor(Algorithm algorithm, Compressor compressor) {
    Preconditions.checkNotNull(algorithm, "Algorithm cannot be null");
    Preconditions.checkNotNull(compressor, "Compressor should not be null");
    compressor.end();
  }

  /**
   * Method to release the decompressor. This implementation will call end on the decompressor.
   *
   * @param algorithm
   *          Supplied decompressor's Algorithm.
   * @param decompressor
   *          decompressor object.
   */
  public void releaseDecompressor(Algorithm algorithm, Decompressor decompressor) {
    Preconditions.checkNotNull(algorithm, "Algorithm cannot be null");
    Preconditions.checkNotNull(decompressor, "Deompressor should not be null");
    decompressor.end();
  }

  /**
   * Provides the caller a decompressor object.
   *
   * @param compressionAlgorithm
   *          decompressor's algorithm.
   * @return decompressor.
   * @throws IOException
   *           I/O Exception during factory implementation
   */
  public Decompressor getDecompressor(Algorithm compressionAlgorithm) {
    if (compressionAlgorithm != null) {
      Decompressor decompressor = compressionAlgorithm.getCodec().createDecompressor();
      if (decompressor != null) {

        LOG.debug("Got a decompressor: " + decompressor.hashCode());

      }
      return decompressor;
    }
    return null;
  }

  /**
   * Implementations may choose to have a close call implemented.
   */
  public void close() {

  }

  /**
   * Provides the capability to update the compression factory
   *
   * @param acuConf
   *          accumulo configuration
   */
  public void update(final AccumuloConfiguration acuConf) {

  }

}
