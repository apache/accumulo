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

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.interpret.ScanInterpreter;
import org.apache.hadoop.io.Text;

/**
 * A simple formatter that print the row, column family, column qualifier, and value as hex
 */
public class HexFormatter implements Formatter, ScanInterpreter {

  private char chars[] = new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  private Iterator<Entry<Key,Value>> iter;
  private FormatterConfig config;

  private void toHex(StringBuilder sb, byte[] bin) {

    for (int i = 0; i < bin.length; i++) {
      if (i > 0 && i % 2 == 0)
        sb.append('-');
      sb.append(chars[0x0f & (bin[i] >>> 4)]);
      sb.append(chars[0x0f & bin[i]]);
    }
  }

  private int fromChar(char b) {
    if (b >= '0' && b <= '9') {
      return (b - '0');
    } else if (b >= 'a' && b <= 'f') {
      return (b - 'a' + 10);
    }

    throw new IllegalArgumentException("Bad char " + b);
  }

  private byte[] toBinary(String hex) {
    hex = hex.replace("-", "");

    byte[] bin = new byte[(hex.length() / 2) + (hex.length() % 2)];

    int j = 0;
    for (int i = 0; i < bin.length; i++) {
      bin[i] = (byte) (fromChar(hex.charAt(j++)) << 4);
      if (j >= hex.length())
        break;
      bin[i] |= (byte) fromChar(hex.charAt(j++));
    }

    return bin;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public String next() {
    Entry<Key,Value> entry = iter.next();

    StringBuilder sb = new StringBuilder();

    toHex(sb, entry.getKey().getRowData().toArray());
    sb.append("  ");
    toHex(sb, entry.getKey().getColumnFamilyData().toArray());
    sb.append("  ");
    toHex(sb, entry.getKey().getColumnQualifierData().toArray());
    sb.append(" [");
    sb.append(entry.getKey().getColumnVisibilityData().toString());
    sb.append("] ");
    if (config.willPrintTimestamps()) {
      sb.append(Long.toString(entry.getKey().getTimestamp()));
      sb.append("  ");
    }
    toHex(sb, entry.getValue().get());

    return sb.toString();
  }

  @Override
  public void remove() {
    iter.remove();
  }

  @Override
  public void initialize(Iterable<Entry<Key,Value>> scanner, FormatterConfig config) {
    this.iter = scanner.iterator();
    this.config = new FormatterConfig(config);
  }

  @Override
  public Text interpretRow(Text row) {
    return new Text(toBinary(row.toString()));
  }

  @Override
  public Text interpretBeginRow(Text row) {
    return interpretRow(row);
  }

  @Override
  public Text interpretEndRow(Text row) {
    return interpretRow(row);
  }

  @Override
  public Text interpretColumnFamily(Text cf) {
    return interpretRow(cf);
  }

  @Override
  public Text interpretColumnQualifier(Text cq) {
    return interpretRow(cq);
  }
}
