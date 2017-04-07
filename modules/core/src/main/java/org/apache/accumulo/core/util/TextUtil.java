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
package org.apache.accumulo.core.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;

import org.apache.accumulo.core.Constants;
import org.apache.hadoop.io.Text;

public final class TextUtil {
  public static byte[] getBytes(Text text) {
    byte[] bytes = text.getBytes();
    if (bytes.length != text.getLength()) {
      bytes = new byte[text.getLength()];
      System.arraycopy(text.getBytes(), 0, bytes, 0, bytes.length);
    }
    return bytes;
  }

  public static ByteBuffer getByteBuffer(Text text) {
    if (text == null)
      return null;
    byte[] bytes = text.getBytes();
    return ByteBuffer.wrap(bytes, 0, text.getLength());
  }

  public static Text truncate(Text text, int maxLen) {
    if (text.getLength() > maxLen) {
      Text newText = new Text();
      newText.append(text.getBytes(), 0, maxLen);
      String suffix = "... TRUNCATED";
      newText.append(suffix.getBytes(UTF_8), 0, suffix.length());
      return newText;
    }

    return text;
  }

  public static Text truncate(Text row) {
    return truncate(row, Constants.MAX_DATA_TO_PRINT);
  }
}
