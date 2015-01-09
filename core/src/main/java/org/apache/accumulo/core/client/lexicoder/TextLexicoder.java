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

import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

/**
 * A lexicoder that preserves a Text's native sort order. It can be combined with other encoders like the {@link ReverseLexicoder} to flip the default sort
 * order.
 *
 * @since 1.6.0
 */

public class TextLexicoder implements Lexicoder<Text> {

  @Override
  public byte[] encode(Text data) {
    return TextUtil.getBytes(data);
  }

  @Override
  public Text decode(byte[] data) {
    return new Text(data);
  }

}
