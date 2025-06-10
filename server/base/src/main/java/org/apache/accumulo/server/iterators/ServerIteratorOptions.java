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
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.rfile.bcfile.Compression;
import org.apache.accumulo.core.file.rfile.bcfile.CompressionAlgorithm;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

public class ServerIteratorOptions {
  private static final String COMPRESSION_ALGO = "__COMPRESSION_ALGO";

    public static void compressOption(final AccumuloConfiguration config, IteratorSetting iteratorSetting, String option, Consumer<DataOutput> serializer) {
    final String algo =
            config.get(Property.GENERAL_SERVER_ITERATOR_OPTIONS_COMPRESSION_ALGO);
    final CompressionAlgorithm ca = Compression.getCompressionAlgorithmByName(algo);
    final Compressor c = ca.getCompressor();

    if(iteratorSetting.getOptions().containsKey(COMPRESSION_ALGO)){
      Preconditions.checkArgument(iteratorSetting.getOptions().get(COMPRESSION_ALGO).equals(algo));
    }else{
      iteratorSetting.addOption(COMPRESSION_ALGO, algo);
    }

    try(ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(ca.createCompressionStream(baos, c, 4096))) {
      serializer.accept(dos);
      dos.close();
      var val = Base64.getEncoder().encodeToString(baos.toByteArray());
      iteratorSetting.addOption(option, val);
    }catch (IOException ioe){
      throw new UncheckedIOException(ioe);
    }finally {
      ca.returnCompressor(c);
    }
  }

  public static <T> T decompressOption(Map<String, String> options, String option, Function<DataInput, T> deserializer) {
      var val = options.get(option);
      if(val == null){
        return null;
      }
      var algo = options.getOrDefault(COMPRESSION_ALGO, "none");
    final byte[] data = Base64.getDecoder().decode(val);
    final CompressionAlgorithm ca = Compression.getCompressionAlgorithmByName(algo);
    final Decompressor d = ca.getDecompressor();
    try (ByteArrayInputStream baos = new ByteArrayInputStream(data);
         DataInputStream dais = new DataInputStream(ca.createDecompressionStream(baos, d, 4096))) {
      return deserializer.apply(dais);
    } catch (IOException ioe){
      throw new UncheckedIOException(ioe);
    }finally {
      ca.returnDecompressor(d);
    }
  }
}
