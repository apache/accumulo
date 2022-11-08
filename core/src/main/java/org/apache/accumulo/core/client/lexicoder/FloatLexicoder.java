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
package org.apache.accumulo.core.client.lexicoder;

/**
 * A lexicoder for preserving the native Java sort order of Float values.
 *
 * @since 1.8.0
 */
public class FloatLexicoder extends AbstractLexicoder<Float> {

  private UIntegerLexicoder intEncoder = new UIntegerLexicoder();

  @Override
  public byte[] encode(Float f) {
    int i = Float.floatToRawIntBits(f);
    if (i < 0) {
      i = ~i;
    } else {
      i = i ^ 0x80000000;
    }

    return intEncoder.encode(i);
  }

  @Override
  public Float decode(byte[] b) {
    // This concrete implementation is provided for binary compatibility, since the corresponding
    // superclass method has type-erased return type Object. See ACCUMULO-3789 and #1285.
    return super.decode(b);
  }

  @Override
  protected Float decodeUnchecked(byte[] b, int offset, int len) {
    int i = intEncoder.decodeUnchecked(b, offset, len);
    if (i < 0) {
      i = i ^ 0x80000000;
    } else {
      i = ~i;
    }

    return Float.intBitsToFloat(i);
  }

}
