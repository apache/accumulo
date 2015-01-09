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

import static com.google.common.base.Charsets.UTF_8;

/**
 * This lexicoder encodes/decodes a given String to/from bytes without further processing. It can be combined with other encoders like the
 * {@link ReverseLexicoder} to flip the default sort order.
 *
 * @since 1.6.0
 */

public class StringLexicoder implements Lexicoder<String> {

  @Override
  public byte[] encode(String data) {
    return data.getBytes(UTF_8);
  }

  @Override
  public String decode(byte[] data) {
    return new String(data, UTF_8);
  }

}
