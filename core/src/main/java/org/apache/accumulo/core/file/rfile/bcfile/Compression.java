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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.accumulo.core.spi.file.rfile.compression.CompressionAlgorithmConfiguration;
import org.apache.hadoop.conf.Configuration;

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

  private static final Map<String,DefaultCompressionAlgorithm> CONFIGURED_ALGORITHMS =
      new HashMap<>();

  private static final ServiceLoader<CompressionAlgorithmConfiguration> COMPRESSION_ALGORITHMS =
      ServiceLoader.load(CompressionAlgorithmConfiguration.class);

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

    COMPRESSION_ALGORITHMS.forEach(a -> {
      DefaultCompressionAlgorithm algo = new DefaultCompressionAlgorithm(a, conf);
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

  public static DefaultCompressionAlgorithm getCompressionAlgorithmByName(final String name) {
    DefaultCompressionAlgorithm algorithm = CONFIGURED_ALGORITHMS.get(name);
    if (algorithm != null) {
      return algorithm;
    }
    throw new IllegalArgumentException("Unsupported compression algorithm name: " + name);
  }
}
