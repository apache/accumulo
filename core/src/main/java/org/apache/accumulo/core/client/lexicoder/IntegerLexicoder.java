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

import org.apache.accumulo.core.client.lexicoder.impl.AbstractLexicoder;

/**
 * A lexicoder for signed integers. The encoding sorts Integer.MIN_VALUE first and Integer.MAX_VALUE last. The encoding sorts -2 before -1. It corresponds to
 * the sort order of Integer.
 *
 * @since 1.6.0
 */
public class IntegerLexicoder extends AbstractLexicoder<Integer> {

  private UIntegerLexicoder uil = new UIntegerLexicoder();

  @Override
  public byte[] encode(Integer i) {
    return uil.encode(i ^ 0x80000000);
  }

  @Override
  public Integer decode(byte[] b) {
    // This concrete implementation is provided for binary compatibility with 1.6; it can be removed in 2.0. See ACCUMULO-3789.
    return super.decode(b);
  }

  @Override
  protected Integer decodeUnchecked(byte[] data, int offset, int len) {
    return uil.decodeUnchecked(data, offset, len) ^ 0x80000000;
  }

}
