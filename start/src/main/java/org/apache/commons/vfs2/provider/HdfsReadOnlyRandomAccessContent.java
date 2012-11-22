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
package org.apache.commons.vfs2.provider;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsReadOnlyRandomAccessContent implements RandomAccessContent {

  private FileSystem fs = null;
  private Path path = null;
  private FSDataInputStream fis = null;
  
  public HdfsReadOnlyRandomAccessContent(Path path, FileSystem fs) throws IOException {
    this.fs = fs;
    this.path = path;
    this.fis = this.fs.open(this.path);
  }

  public void write(int b) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void write(byte[] b) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void write(byte[] b, int off, int len) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void writeBoolean(boolean v) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void writeByte(int v) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void writeShort(int v) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void writeChar(int v) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void writeInt(int v) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void writeLong(long v) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void writeFloat(float v) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void writeDouble(double v) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void writeBytes(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void writeChars(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void writeUTF(String s) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void readFully(byte[] b) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void readFully(byte[] b, int off, int len) throws IOException {
    throw new UnsupportedOperationException();
  }

  public int skipBytes(int n) throws IOException {
    throw new UnsupportedOperationException();
  }

  public boolean readBoolean() throws IOException {
    return this.fis.readBoolean();
  }

  public byte readByte() throws IOException {
    return this.fis.readByte();
  }

  public int readUnsignedByte() throws IOException {
    return this.fis.readUnsignedByte();
  }

  public short readShort() throws IOException {
    return this.fis.readShort();
  }

  public int readUnsignedShort() throws IOException {
    return this.fis.readUnsignedShort();
  }

  public char readChar() throws IOException {
    return this.fis.readChar();
  }

  public int readInt() throws IOException {
    return this.fis.readInt();
  }

  public long readLong() throws IOException {
    return this.fis.readLong();
  }

  public float readFloat() throws IOException {
    return this.fis.readFloat();
  }

  public double readDouble() throws IOException {
    return this.fis.readDouble();
  }

  @SuppressWarnings("deprecation")
  public String readLine() throws IOException {
    return this.fis.readLine();
  }

  public String readUTF() throws IOException {
    return this.fis.readUTF();
  }

  public long getFilePointer() throws IOException {
    return this.fis.getPos();
  }

  public void seek(long pos) throws IOException {
    this.fis.seek(pos);
  }

  public long length() throws IOException {
    return this.fs.getFileStatus(this.path).getLen();
  }

  public void close() throws IOException {
    this.fis.close();
  }

  public InputStream getInputStream() throws IOException {
    return fis;
  }
  
}
