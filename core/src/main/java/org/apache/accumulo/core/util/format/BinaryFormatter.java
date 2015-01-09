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
package org.apache.accumulo.core.util.format;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class BinaryFormatter extends DefaultFormatter {
  private static int showLength;

  public String next() {
    checkState(true);
    return formatEntry(getScannerIterator().next(), isDoTimestamps());
  }

  // this should be replaced with something like Record.toString();
  // it would be great if we were able to combine code with DefaultFormatter.formatEntry, but that currently does not respect the showLength option.
  public static String formatEntry(Entry<Key,Value> entry, boolean showTimestamps) {
    StringBuilder sb = new StringBuilder();

    Key key = entry.getKey();

    // append row
    appendText(sb, key.getRow()).append(" ");

    // append column family
    appendText(sb, key.getColumnFamily()).append(":");

    // append column qualifier
    appendText(sb, key.getColumnQualifier()).append(" ");

    // append visibility expression
    sb.append(new ColumnVisibility(key.getColumnVisibility()));

    // append timestamp
    if (showTimestamps)
      sb.append(" ").append(entry.getKey().getTimestamp());

    // append value
    Value value = entry.getValue();
    if (value != null && value.getSize() > 0) {
      sb.append("\t");
      appendValue(sb, value);
    }
    return sb.toString();
  }

  public static StringBuilder appendText(StringBuilder sb, Text t) {
    return appendBytes(sb, t.getBytes(), 0, t.getLength());
  }

  static StringBuilder appendValue(StringBuilder sb, Value value) {
    return appendBytes(sb, value.get(), 0, value.get().length);
  }

  static StringBuilder appendBytes(StringBuilder sb, byte ba[], int offset, int len) {
    int length = Math.min(len, showLength);
    return DefaultFormatter.appendBytes(sb, ba, offset, length);
  }

  public static void getlength(int length) {
    showLength = length;
  }
}
