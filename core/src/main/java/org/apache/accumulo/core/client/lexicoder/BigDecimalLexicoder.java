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
package org.apache.accumulo.core.client.lexicoder;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.accumulo.core.clientImpl.lexicoder.FixedByteArrayOutputStream;

/**
 * A lexicoder to encode/decode a BigDecimal to/from bytes that maintain its native Java sort order.
 *
 * @since 2.1.0
 */
public class BigDecimalLexicoder extends AbstractLexicoder<BigDecimal> {

  private final BigIntegerLexicoder bigIntegerLexicoder = new BigIntegerLexicoder();

  @Override
  public BigDecimal decode(byte[] b) {
    // This concrete implementation is provided for binary compatibility, since the corresponding
    // superclass method has type-erased return type Object. See ACCUMULO-3789 and #1285.
    return super.decode(b);
  }

  @Override
  public byte[] encode(BigDecimal bd) {
    // To encode we separate out the scale and the unscaled value
    // serialize each value individually and store them
    int scale = bd.scale();
    BigInteger bigInt = bd.unscaledValue();
    byte[] encodedbigInt = bigIntegerLexicoder.encode(bigInt);
    // Length is set to size of encoded BigInteger + length of the scale value
    byte[] ret = new byte[4 + encodedbigInt.length];
    try (DataOutputStream dos = new DataOutputStream(new FixedByteArrayOutputStream(ret))) {
      scale = scale ^ 0x80000000;
      dos.write(encodedbigInt);
      dos.writeInt(scale);
      return ret;

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected BigDecimal decodeUnchecked(byte[] b, int offset, int origLen)
      throws IllegalArgumentException {

    try {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b, offset, origLen));
      byte[] bytes = new byte[origLen - 4];
      // read the encoded Unscaled Value first
      dis.readFully(bytes, 0, origLen - 4);
      // read the scale
      int scale = dis.readInt();
      scale = scale ^ 0x80000000;
      scale = Math.abs(scale);

      BigInteger bigInt = bigIntegerLexicoder.decodeUnchecked(bytes, 0, bytes.length);
      return new BigDecimal(bigInt, scale);
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    }

  }
}
