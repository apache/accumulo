/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

/**
 * Supporting Utility classes used by TFile, and shared by users of TFile.
 */
public final class Utils {

  /**
   * Prevent the instantiation of Utils.
   */
  private Utils() {
    // nothing
  }

  /**
   * Encoding an integer into a variable-length encoding format. Synonymous to
   * <code>Utils#writeVLong(out, n)</code>.
   *
   * @param out
   *          output stream
   * @param n
   *          The integer to be encoded
   * @see Utils#writeVLong(DataOutput, long)
   */
  public static void writeVInt(DataOutput out, int n) throws IOException {
    writeVLong(out, n);
  }

  /**
   * Encoding a Long integer into a variable-length encoding format.
   * <ul>
   * <li>if n in [-32, 127): encode in one byte with the actual value. Otherwise,
   * <li>if n in [-20*2^8, 20*2^8): encode in two bytes: byte[0] = n/256 - 52; byte[1]=n&amp;0xff.
   * Otherwise,
   * <li>if n IN [-16*2^16, 16*2^16): encode in three bytes: byte[0]=n/2^16 - 88;
   * byte[1]=(n&gt;&gt;8)&amp;0xff; byte[2]=n&amp;0xff. Otherwise,
   * <li>if n in [-8*2^24, 8*2^24): encode in four bytes: byte[0]=n/2^24 - 112; byte[1] =
   * (n&gt;&gt;16)&amp;0xff; byte[2] = (n&gt;&gt;8)&amp;0xff; byte[3]=n&amp;0xff. Otherwise:
   * <li>if n in [-2^31, 2^31): encode in five bytes: byte[0]=-125; byte[1] =
   * (n&gt;&gt;24)&amp;0xff; byte[2]=(n&gt;&gt;16)&amp;0xff; byte[3]=(n&gt;&gt;8)&amp;0xff;
   * byte[4]=n&amp;0xff;
   * <li>if n in [-2^39, 2^39): encode in six bytes: byte[0]=-124; byte[1] = (n&gt;&gt;32)&amp;0xff;
   * byte[2]=(n&gt;&gt;24)&amp;0xff; byte[3]=(n&gt;&gt;16)&amp;0xff; byte[4]=(n&gt;&gt;8)&amp;0xff;
   * byte[5]=n&amp;0xff
   * <li>if n in [-2^47, 2^47): encode in seven bytes: byte[0]=-123; byte[1] =
   * (n&gt;&gt;40)&amp;0xff; byte[2]=(n&gt;&gt;32)&amp;0xff; byte[3]=(n&gt;&gt;24)&amp;0xff;
   * byte[4]=(n&gt;&gt;16)&amp;0xff; byte[5]=(n&gt;&gt;8)&amp;0xff; byte[6]=n&amp;0xff;
   * <li>if n in [-2^55, 2^55): encode in eight bytes: byte[0]=-122; byte[1] =
   * (n&gt;&gt;48)&amp;0xff; byte[2] = (n&gt;&gt;40)&amp;0xff; byte[3]=(n&gt;&gt;32)&amp;0xff;
   * byte[4]=(n&gt;&gt;24)&amp;0xff; byte[5]=(n&gt;&gt;16)&amp;0xff; byte[6]=(n&gt;&gt;8)&amp;0xff;
   * byte[7]=n&amp;0xff;
   * <li>if n in [-2^63, 2^63): encode in nine bytes: byte[0]=-121; byte[1] =
   * (n&gt;&gt;54)&amp;0xff; byte[2] = (n&gt;&gt;48)&amp;0xff; byte[3] = (n&gt;&gt;40)&amp;0xff;
   * byte[4]=(n&gt;&gt;32)&amp;0xff; byte[5]=(n&gt;&gt;24)&amp;0xff; byte[6]=(n&gt;&gt;16)&amp;0xff;
   * byte[7]=(n&gt;&gt;8)&amp;0xff; byte[8]=n&amp;0xff;
   * </ul>
   *
   * @param out
   *          output stream
   * @param n
   *          the integer number
   */
  @SuppressWarnings("fallthrough")
  public static void writeVLong(DataOutput out, long n) throws IOException {
    if ((n < 128) && (n >= -32)) {
      out.writeByte((int) n);
      return;
    }

    long un = (n < 0) ? ~n : n;
    // how many bytes do we need to represent the number with sign bit?
    int len = (Long.SIZE - Long.numberOfLeadingZeros(un)) / 8 + 1;
    int firstByte = (int) (n >> ((len - 1) * 8));
    switch (len) {
      case 1:
        // fall it through to firstByte==-1, len=2.
        firstByte >>= 8;
      case 2:
        if ((firstByte < 20) && (firstByte >= -20)) {
          out.writeByte(firstByte - 52);
          out.writeByte((int) n);
          return;
        }
        // fall it through to firstByte==0/-1, len=3.
        firstByte >>= 8;
      case 3:
        if ((firstByte < 16) && (firstByte >= -16)) {
          out.writeByte(firstByte - 88);
          out.writeShort((int) n);
          return;
        }
        // fall it through to firstByte==0/-1, len=4.
        firstByte >>= 8;
      case 4:
        if ((firstByte < 8) && (firstByte >= -8)) {
          out.writeByte(firstByte - 112);
          out.writeShort(((int) n) >>> 8);
          out.writeByte((int) n);
          return;
        }
        out.writeByte(len - 129);
        out.writeInt((int) n);
        return;
      case 5:
        out.writeByte(len - 129);
        out.writeInt((int) (n >>> 8));
        out.writeByte((int) n);
        return;
      case 6:
        out.writeByte(len - 129);
        out.writeInt((int) (n >>> 16));
        out.writeShort((int) n);
        return;
      case 7:
        out.writeByte(len - 129);
        out.writeInt((int) (n >>> 24));
        out.writeShort((int) (n >>> 8));
        out.writeByte((int) n);
        return;
      case 8:
        out.writeByte(len - 129);
        out.writeLong(n);
        return;
      default:
        throw new RuntimeException("Internel error");
    }
  }

  /**
   * Decoding the variable-length integer. Synonymous to <code>(int)Utils#readVLong(in)</code>.
   *
   * @param in
   *          input stream
   * @return the decoded integer
   *
   * @see Utils#readVLong(DataInput)
   */
  public static int readVInt(DataInput in) throws IOException {
    long ret = readVLong(in);
    if ((ret > Integer.MAX_VALUE) || (ret < Integer.MIN_VALUE)) {
      throw new RuntimeException("Number too large to be represented as Integer");
    }
    return (int) ret;
  }

  /**
   * Decoding the variable-length integer. Suppose the value of the first byte is FB, and the
   * following bytes are NB[*].
   * <ul>
   * <li>if (FB &gt;= -32), return (long)FB;
   * <li>if (FB in [-72, -33]), return (FB+52)&lt;&lt;8 + NB[0]&amp;0xff;
   * <li>if (FB in [-104, -73]), return (FB+88)&lt;&lt;16 + (NB[0]&amp;0xff)&lt;&lt;8 +
   * NB[1]&amp;0xff;
   * <li>if (FB in [-120, -105]), return (FB+112)&lt;&lt;24 + (NB[0]&amp;0xff)&lt;&lt;16 +
   * (NB[1]&amp;0xff)&lt;&lt;8 + NB[2]&amp;0xff;
   * <li>if (FB in [-128, -121]), return interpret NB[FB+129] as a signed big-endian integer.
   * </ul>
   *
   * @param in
   *          input stream
   * @return the decoded long integer.
   */

  public static long readVLong(DataInput in) throws IOException {
    int firstByte = in.readByte();
    if (firstByte >= -32) {
      return firstByte;
    }

    switch ((firstByte + 128) / 8) {
      case 11:
      case 10:
      case 9:
      case 8:
      case 7:
        return ((firstByte + 52) << 8) | in.readUnsignedByte();
      case 6:
      case 5:
      case 4:
      case 3:
        return ((firstByte + 88) << 16) | in.readUnsignedShort();
      case 2:
      case 1:
        return ((firstByte + 112) << 24) | (in.readUnsignedShort() << 8) | in.readUnsignedByte();
      case 0:
        int len = firstByte + 129;
        switch (len) {
          case 4:
            return in.readInt();
          case 5:
            return ((long) in.readInt()) << 8 | in.readUnsignedByte();
          case 6:
            return ((long) in.readInt()) << 16 | in.readUnsignedShort();
          case 7:
            return ((long) in.readInt()) << 24 | (in.readUnsignedShort() << 8)
                | in.readUnsignedByte();
          case 8:
            return in.readLong();
          default:
            throw new IOException("Corrupted VLong encoding");
        }
      default:
        throw new RuntimeException("Internal error");
    }
  }

  /**
   * Write a String as a VInt n, followed by n Bytes as in Text format.
   */
  public static void writeString(DataOutput out, String s) throws IOException {
    if (s != null) {
      Text text = new Text(s);
      byte[] buffer = text.getBytes();
      int len = text.getLength();
      writeVInt(out, len);
      out.write(buffer, 0, len);
    } else {
      writeVInt(out, -1);
    }
  }

  /**
   * Read a String as a VInt n, followed by n Bytes in Text format.
   *
   * @param in
   *          The input stream.
   * @return The string
   */
  public static String readString(DataInput in) throws IOException {
    int length = readVInt(in);
    if (length == -1)
      return null;
    byte[] buffer = new byte[length];
    in.readFully(buffer);
    return Text.decode(buffer);
  }

  /**
   * A generic Version class. We suggest applications built on top of TFile use this class to
   * maintain version information in their meta blocks.
   *
   * A version number consists of a major version and a minor version. The suggested usage of major
   * and minor version number is to increment major version number when the new storage format is
   * not backward compatible, and increment the minor version otherwise.
   */
  public static final class Version implements Comparable<Version> {
    private final short major;
    private final short minor;

    /**
     * Construct the Version object by reading from the input stream.
     *
     * @param in
     *          input stream
     */
    public Version(DataInput in) throws IOException {
      major = in.readShort();
      minor = in.readShort();
    }

    /**
     * Constructor.
     *
     * @param major
     *          major version.
     * @param minor
     *          minor version.
     */
    public Version(short major, short minor) {
      this.major = major;
      this.minor = minor;
    }

    /**
     * Write the object to a DataOutput. The serialized format of the Version is major version
     * followed by minor version, both as big-endian short integers.
     *
     * @param out
     *          The DataOutput object.
     */
    public void write(DataOutput out) throws IOException {
      out.writeShort(major);
      out.writeShort(minor);
    }

    /**
     * Get the size of the serialized Version object.
     *
     * @return serialized size of the version object.
     */
    public static int size() {
      return (Short.SIZE + Short.SIZE) / Byte.SIZE;
    }

    @Override
    public String toString() {
      return new StringBuilder("v").append(major).append(".").append(minor).toString();
    }

    /**
     * Test compatibility.
     *
     * @param other
     *          The Version object to test compatibility with.
     * @return true if both versions have the same major version number; false otherwise.
     */
    public boolean compatibleWith(Version other) {
      return major == other.major;
    }

    @Override
    public int compareTo(Version that) {
      if (major != that.major) {
        return major - that.major;
      }
      return minor - that.minor;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other)
        return true;
      if (!(other instanceof Version))
        return false;
      return compareTo((Version) other) == 0;
    }

    @Override
    public int hashCode() {
      return ((major << 16) + minor);
    }
  }

}
