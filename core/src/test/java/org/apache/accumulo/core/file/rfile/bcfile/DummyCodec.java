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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

public class DummyCodec implements Configurable, CompressionCodec {

  Configuration conf;

  /**
   * Set the configuration to be used by this object.
   *
   * @param conf the configuration object.
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Return the configuration used by this object.
   *
   * @return the configuration object used by this object.
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given {@link OutputStream}.
   *
   * @param out the location for the final output stream
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException unsupported operation
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given {@link OutputStream} with
   * the given {@link Compressor}.
   *
   * @param out the location for the final output stream
   * @param compressor compressor to use
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException unsupported operation
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of compressor needed by this codec.
   */
  @Override
  public Class<? extends Compressor> getCompressorType() {
    throw new UnsupportedOperationException();
  }

  /**
   * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
   *
   * @return a new compressor for use by this codec
   */
  @Override
  public Compressor createCompressor() {
    throw new UnsupportedOperationException();
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given input stream.
   *
   * @param in the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   * @throws IOException unsupported operation
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given {@link InputStream} with
   * the given {@link Decompressor}.
   *
   * @param in the stream to read compressed bytes from
   * @param decompressor decompressor to use
   * @return a stream to read uncompressed bytes from
   * @throws IOException unsupported operation
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the type of {@link Decompressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of decompressor needed by this codec.
   */
  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    throw new UnsupportedOperationException();
  }

  /**
   * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
   *
   * @return a new decompressor for use by this codec
   */
  @Override
  public Decompressor createDecompressor() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the default filename extension for this kind of compression.
   *
   * @return <code>.dummy</code>.
   */
  @Override
  public String getDefaultExtension() {
    return ".dummy";
  }

}
