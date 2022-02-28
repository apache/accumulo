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
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

  // All compression-related settings are required to be configured statically in the
  // Configuration object.
  protected static final Configuration conf = new Configuration();

  private static final ServiceLoader<CompressionAlgorithmConfiguration> COMPRESSION_ALGORITHMS =
      ServiceLoader.load(CompressionAlgorithmConfiguration.class);

  private static final Map<String,DefaultCompressionAlgorithm> CONFIGURED_ALGORITHMS =
      StreamSupport.stream(COMPRESSION_ALGORITHMS.spliterator(), false)
          .map(a -> new DefaultCompressionAlgorithm(a, conf))
          .collect(Collectors.toMap(algo -> algo.getName(), algo -> algo));

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
