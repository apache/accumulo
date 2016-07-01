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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.client.lexicoder.impl.AbstractLexicoder;

/**
 * This lexicoder encodes/decodes a given String to/from bytes without further processing. It can be combined with other encoders like the
 * {@link ReverseLexicoder} to flip the default sort order.
 *
 * @since 1.6.0
 */

public class StringLexicoder extends AbstractLexicoder<String> {

  @Override
  public byte[] encode(String data) {
    return data.getBytes(UTF_8);
  }

  @Override
  public String decode(byte[] b) {
    // This concrete implementation is provided for binary compatibility with 1.6; it can be removed in 2.0. See ACCUMULO-3789.
    return super.decode(b);
  }

  @Override
  protected String decodeUnchecked(byte[] data, int offset, int len) {
    return new String(data, offset, len, UTF_8);
  }

}
