/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.start.classloader.vfs.providers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Provides random access to content in an HdfsFileObject. Currently this only supports read operations. All write operations throw an
 * {@link UnsupportedOperationException}.
 *
 * @since 2.1
 */
public class HdfsRandomAccessContent implements RandomAccessContent {
  private final FileSystem fs;
  private final Path path;
  private final FSDataInputStream fis;

  /**
   *
   * @param path
   *          A Hadoop Path
   * @param fs
   *          A Hadoop FileSystem
   * @throws IOException
   *           when the path cannot be processed.
   */
  public HdfsRandomAccessContent(final Path path, final FileSystem fs) throws IOException {
    this.fs = fs;
    this.path = path;
    this.fis = this.fs.open(this.path);
  }

  /**
   * @see org.apache.commons.vfs2.RandomAccessContent#close()
   */
  @Override
  public void close() throws IOException {
    this.fis.close();
  }

  /**
   * @see org.apache.commons.vfs2.RandomAccessContent#getFilePointer()
   */
  @Override
  public long getFilePointer() throws IOException {
    return this.fis.getPos();
  }

  /**
   * @see org.apache.commons.vfs2.RandomAccessContent#getInputStream()
   */
  @Override
  public InputStream getInputStream() throws IOException {
    return this.fis;
  }

  /**
   * @see org.apache.commons.vfs2.RandomAccessContent#length()
   */
  @Override
  public long length() throws IOException {
    return this.fs.getFileStatus(this.path).getLen();
  }

  /**
   * @see java.io.DataInput#readBoolean()
   */
  @Override
  public boolean readBoolean() throws IOException {
    return this.fis.readBoolean();
  }

  /**
   * @see java.io.DataInput#readByte()
   */
  @Override
  public byte readByte() throws IOException {
    return this.fis.readByte();
  }

  /**
   * @see java.io.DataInput#readChar()
   */
  @Override
  public char readChar() throws IOException {
    return this.fis.readChar();
  }

  /**
   * @see java.io.DataInput#readDouble()
   */
  @Override
  public double readDouble() throws IOException {
    return this.fis.readDouble();
  }

  /**
   * @see java.io.DataInput#readFloat()
   */
  @Override
  public float readFloat() throws IOException {
    return this.fis.readFloat();
  }

  /**
   * @see java.io.DataInput#readFully(byte[])
   */
  @Override
  public void readFully(final byte[] b) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataInput#readFully(byte[], int, int)
   */
  @Override
  public void readFully(final byte[] b, final int off, final int len) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataInput#readInt()
   */
  @Override
  public int readInt() throws IOException {
    return this.fis.readInt();
  }

  /**
   * @see java.io.DataInput#readLine()
   */
  @Override
  public String readLine() throws IOException {
    BufferedReader d = new BufferedReader(new InputStreamReader(this.fis, Charset.forName("UTF-8")));
    return d.readLine();
  }

  /**
   * @see java.io.DataInput#readLong()
   */
  @Override
  public long readLong() throws IOException {
    return this.fis.readLong();
  }

  /**
   * @see java.io.DataInput#readShort()
   */
  @Override
  public short readShort() throws IOException {
    return this.fis.readShort();
  }

  /**
   * @see java.io.DataInput#readUnsignedByte()
   */
  @Override
  public int readUnsignedByte() throws IOException {
    return this.fis.readUnsignedByte();
  }

  /**
   * @see java.io.DataInput#readUnsignedShort()
   */
  @Override
  public int readUnsignedShort() throws IOException {
    return this.fis.readUnsignedShort();
  }

  /**
   * @see java.io.DataInput#readUTF()
   */
  @Override
  public String readUTF() throws IOException {
    return this.fis.readUTF();
  }

  /**
   * @see org.apache.commons.vfs2.RandomAccessContent#seek(long)
   */
  @Override
  public void seek(final long pos) throws IOException {
    this.fis.seek(pos);
  }

  /**
   * @see java.io.DataInput#skipBytes(int)
   */
  @Override
  public int skipBytes(final int n) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#write(byte[])
   */
  @Override
  public void write(final byte[] b) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#write(byte[], int, int)
   */
  @Override
  public void write(final byte[] b, final int off, final int len) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#write(int)
   */
  @Override
  public void write(final int b) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#writeBoolean(boolean)
   */
  @Override
  public void writeBoolean(final boolean v) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#writeByte(int)
   */
  @Override
  public void writeByte(final int v) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#writeBytes(java.lang.String)
   */
  @Override
  public void writeBytes(final String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#writeChar(int)
   */
  @Override
  public void writeChar(final int v) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#writeChars(java.lang.String)
   */
  @Override
  public void writeChars(final String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#writeDouble(double)
   */
  @Override
  public void writeDouble(final double v) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#writeFloat(float)
   */
  @Override
  public void writeFloat(final float v) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#writeInt(int)
   */
  @Override
  public void writeInt(final int v) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#writeLong(long)
   */
  @Override
  public void writeLong(final long v) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#writeShort(int)
   */
  @Override
  public void writeShort(final int v) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @see java.io.DataOutput#writeUTF(java.lang.String)
   */
  @Override
  public void writeUTF(final String s) throws IOException {
    throw new UnsupportedOperationException();
  }

}
