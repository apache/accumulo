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
package org.apache.accumulo.server.iterators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.rfile.bcfile.Compression;
import org.apache.accumulo.core.file.rfile.bcfile.CompressionAlgorithm;
import org.apache.accumulo.server.ServerContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerIteratorOptions {

  private static final Logger LOG = LoggerFactory.getLogger(ServerIteratorOptions.class);
  private static final String COMPRESSION_ALGO = "__COMPRESSION_ALGO";
  private static final String COMPRESSED_KEYS = "__COMPRESSED_KEYS";

  public static Map<String,String> compressOptions(final ServerContext ctx,
      final Map<String,String> options, final Set<String> optionsToCompress) {
    if (options.isEmpty()) {
      return Map.of();
    }

    final String algo =
        ctx.getConfiguration().get(Property.GENERAL_SERVER_ITERATOR_OPTIONS_COMPRESSION_ALGO);
    if (algo.isBlank()) {
      return options;
    }

    final Map<String,String> tmpOpts = new HashMap<>(options.size());
    final Set<String> compressedOptions = new HashSet<>(optionsToCompress);
    final CompressionAlgorithm ca = Compression.getCompressionAlgorithmByName(algo);
    final Compressor c = ca.getCompressor();
    try {
      for (Entry<String,String> e : options.entrySet()) {
        final String key = e.getKey();
        final String value = e.getValue();
        if (!optionsToCompress.contains(key)) {
          tmpOpts.put(key, value);
        } else {
          String tmpValue = value;
          try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
              OutputStream os = ca.createCompressionStream(baos, c, 4096)) {
            os.write(tmpValue.getBytes(StandardCharsets.UTF_8));
            os.close();
            tmpValue = Base64.getEncoder().encodeToString(baos.toByteArray());
            tmpOpts.put(key, tmpValue);
          } catch (IOException ioe) {
            LOG.debug("Error compressing {}:{} using algo: {}", key, tmpValue, algo, ioe);
            // some error occurred, put original value
            tmpOpts.put(key, tmpValue);
            compressedOptions.remove(key);
          }
        }
      }
    } finally {
      ca.returnCompressor(c);
    }
    tmpOpts.put(COMPRESSION_ALGO, algo);
    tmpOpts.put(COMPRESSED_KEYS, StringUtils.join(compressedOptions, ","));
    return Map.copyOf(tmpOpts);
  }

  public static Map<String,String> decompressOptions(ServerContext ctx,
      Map<String,String> options) {
    if (options.isEmpty()) {
      return Map.of();
    }

    final String compressedOptions = options.remove(COMPRESSED_KEYS);
    if (compressedOptions == null) {
      return options;
    }
    final List<String> optionsToDecompress = Arrays.asList(compressedOptions.split(","));

    final String algo = options.remove(COMPRESSION_ALGO);
    if (algo == null || algo.isBlank()) {
      return options;
    }

    final Map<String,String> tmpOpts = new HashMap<>(options.size());
    final CompressionAlgorithm ca = Compression.getCompressionAlgorithmByName(algo);
    final Decompressor d = ca.getDecompressor();
    try {
      for (Entry<String,String> e : options.entrySet()) {
        final String key = e.getKey();
        final String value = e.getValue();
        if (!optionsToDecompress.contains(key)) {
          tmpOpts.put(key, value);
        } else {
          final byte[] data = Base64.getDecoder().decode(value);
          try (ByteArrayInputStream baos = new ByteArrayInputStream(data);
              InputStream is = ca.createDecompressionStream(baos, d, 4096)) {
            final byte[] decompressedData = is.readAllBytes();
            tmpOpts.put(key, new String(decompressedData, StandardCharsets.UTF_8));
          } catch (IOException ioe) {
            LOG.debug("Error decompressing {}:{} using algo: {}", key, value, algo, ioe);
          }
        }
      }
    } finally {
      ca.returnDecompressor(d);
    }
    return Map.copyOf(tmpOpts);
  }

}
