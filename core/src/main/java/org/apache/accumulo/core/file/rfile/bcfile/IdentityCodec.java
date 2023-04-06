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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

public class IdentityCodec implements CompressionCodec {

  /*
   * Copied from org/apache/hadoop/io/compress/FakeCompressor.java
   */
  public static class IdentityCompressor implements Compressor {

    private boolean finish;
    private boolean finished;
    private int nread;
    private int nwrite;

    private byte[] userBuf;
    private int userBufOff;
    private int userBufLen;

    @Override
    public int compress(byte[] b, int off, int len) throws IOException {
      int n = Math.min(len, userBufLen);
      if (userBuf != null && b != null) {
        System.arraycopy(userBuf, userBufOff, b, off, n);
      }
      userBufOff += n;
      userBufLen -= n;
      nwrite += n;

      if (finish && userBufLen <= 0) {
        finished = true;
      }

      return n;
    }

    @Override
    public void end() {
      // nop
    }

    @Override
    public void finish() {
      finish = true;
    }

    @Override
    public boolean finished() {
      return finished;
    }

    @Override
    public long getBytesRead() {
      return nread;
    }

    @Override
    public long getBytesWritten() {
      return nwrite;
    }

    @Override
    public boolean needsInput() {
      return userBufLen <= 0;
    }

    @Override
    public void reset() {
      finish = false;
      finished = false;
      nread = 0;
      nwrite = 0;
      userBuf = null;
      userBufOff = 0;
      userBufLen = 0;
    }

    @Override
    public void setDictionary(byte[] b, int off, int len) {
      // nop
    }

    @Override
    public void setInput(byte[] b, int off, int len) {
      nread += len;
      userBuf = b;
      userBufOff = off;
      userBufLen = len;
    }

    @Override
    public void reinit(Configuration conf) {
      // nop
    }

  }

  /*
   * Copied from org/apache/hadoop/io/compress/FakeDecompressor.java
   */
  public static class IdentityDecompressor implements Decompressor {

    private boolean finish;
    private boolean finished;
    private int nread;
    private int nwrite;

    private byte[] userBuf;
    private int userBufOff;
    private int userBufLen;

    @Override
    public int decompress(byte[] b, int off, int len) throws IOException {
      int n = Math.min(len, userBufLen);
      if (userBuf != null && b != null) {
        System.arraycopy(userBuf, userBufOff, b, off, n);
      }
      userBufOff += n;
      userBufLen -= n;
      nwrite += n;

      if (finish && userBufLen <= 0) {
        finished = true;
      }

      return n;
    }

    @Override
    public void end() {
      // nop
    }

    @Override
    public boolean finished() {
      return finished;
    }

    public long getBytesRead() {
      return nread;
    }

    public long getBytesWritten() {
      return nwrite;
    }

    @Override
    public boolean needsDictionary() {
      return false;
    }

    @Override
    public boolean needsInput() {
      return userBufLen <= 0;
    }

    @Override
    public void reset() {
      finish = false;
      finished = false;
      nread = 0;
      nwrite = 0;
      userBuf = null;
      userBufOff = 0;
      userBufLen = 0;
    }

    @Override
    public void setDictionary(byte[] b, int off, int len) {
      // nop
    }

    @Override
    public void setInput(byte[] b, int off, int len) {
      nread += len;
      userBuf = b;
      userBufOff = off;
      userBufLen = len;
    }

    @Override
    public int getRemaining() {
      return 0;
    }

  }

  public static class IdentityCompressionInputStream extends CompressionInputStream {

    protected IdentityCompressionInputStream(InputStream in) throws IOException {
      super(in);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return in.read(b, off, len);
    }

    @Override
    public void resetState() throws IOException {}

    @Override
    public int read() throws IOException {
      return in.read();
    }

  }

  public static class IdentityCompressionOutputStream extends CompressionOutputStream {

    public IdentityCompressionOutputStream(OutputStream out) {
      super(out);
    }

    @Override
    public void finish() throws IOException {}

    @Override
    public void resetState() throws IOException {}

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }
  }

  @Override
  public Compressor createCompressor() {
    return new IdentityCompressor();
  }

  @Override
  public Decompressor createDecompressor() {
    return new IdentityDecompressor();
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    return new IdentityCompressionInputStream(in);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor d)
      throws IOException {
    return new IdentityCompressionInputStream(in);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream os) throws IOException {
    return new IdentityCompressionOutputStream(os);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream os, Compressor c)
      throws IOException {
    return new IdentityCompressionOutputStream(os);
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return IdentityCompressor.class;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return IdentityDecompressor.class;
  }

  @Override
  public String getDefaultExtension() {
    return ".identity";
  }

}
