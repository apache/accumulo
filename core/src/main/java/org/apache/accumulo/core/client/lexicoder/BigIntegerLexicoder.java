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
package org.apache.accumulo.core.client.lexicoder;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;

import org.apache.accumulo.core.clientImpl.lexicoder.AbstractLexicoder;
import org.apache.accumulo.core.clientImpl.lexicoder.FixedByteArrayOutputStream;

/**
 * A lexicoder to encode/decode a BigInteger to/from bytes that maintain its native Java sort order.
 *
 * @since 1.6.0
 */
public class BigIntegerLexicoder extends AbstractLexicoder<BigInteger> {

  @Override
  public byte[] encode(BigInteger v) {

    try {
      byte[] bytes = v.toByteArray();

      byte[] ret = new byte[4 + bytes.length];

      DataOutputStream dos = new DataOutputStream(new FixedByteArrayOutputStream(ret));

      // flip the sign bit
      bytes[0] = (byte) (0x80 ^ bytes[0]);

      int len = bytes.length;
      if (v.signum() < 0)
        len = -len;

      len = len ^ 0x80000000;

      dos.writeInt(len);
      dos.write(bytes);
      dos.close();

      return ret;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

  }

  @Override
  public BigInteger decode(byte[] b) {
    // This concrete implementation is provided for binary compatibility, since the corresponding
    // superclass method has type-erased return type Object. See ACCUMULO-3789 and #1285.
    return super.decode(b);
  }

  @Override
  protected BigInteger decodeUnchecked(byte[] b, int offset, int origLen) {

    try {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b, offset, origLen));
      int newLen = dis.readInt();
      newLen = newLen ^ 0x80000000;
      newLen = Math.abs(newLen);

      byte[] bytes = new byte[newLen];
      dis.readFully(bytes);

      bytes[0] = (byte) (0x80 ^ bytes[0]);

      return new BigInteger(bytes);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

}
