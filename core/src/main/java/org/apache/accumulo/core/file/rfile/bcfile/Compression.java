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

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.spi.file.rfile.compression.Bzip2;
import org.apache.accumulo.core.spi.file.rfile.compression.CompressionAlgorithmConfiguration;
import org.apache.accumulo.core.spi.file.rfile.compression.Gz;
import org.apache.accumulo.core.spi.file.rfile.compression.Lz4;
import org.apache.accumulo.core.spi.file.rfile.compression.Lzo;
import org.apache.accumulo.core.spi.file.rfile.compression.NoCompression;
import org.apache.accumulo.core.spi.file.rfile.compression.Snappy;
import org.apache.accumulo.core.spi.file.rfile.compression.ZStandard;
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

  private static final ServiceLoader<CompressionAlgorithmConfiguration> FOUND_ALGOS =
      ServiceLoader.load(CompressionAlgorithmConfiguration.class);

  private static final Set<CompressionAlgorithmConfiguration> BUILTIN_ALGOS = Set.of(new Gz(),
      new Bzip2(), new Lz4(), new Lzo(), new NoCompression(), new Snappy(), new ZStandard());

  private static final Map<String,
      CompressionAlgorithm> CONFIGURED_ALGORITHMS = Stream
          .concat(BUILTIN_ALGOS.stream(), StreamSupport.stream(FOUND_ALGOS.spliterator(), false))
          .map(a -> new CompressionAlgorithm(a, conf))
          .collect(Collectors.toMap(CompressionAlgorithm::getName, Function.identity()));

  public static List<String> getSupportedAlgorithms() {
    return CONFIGURED_ALGORITHMS.entrySet().stream().filter(e -> e.getValue().isSupported())
        .map(Map.Entry::getKey).collect(Collectors.toList());
  }

  public static CompressionAlgorithm getCompressionAlgorithmByName(final String name) {
    CompressionAlgorithm algorithm = CONFIGURED_ALGORITHMS.get(name);
    if (algorithm != null) {
      return algorithm;
    }
    throw new IllegalArgumentException("Unsupported compression algorithm name: " + name);
  }
}
