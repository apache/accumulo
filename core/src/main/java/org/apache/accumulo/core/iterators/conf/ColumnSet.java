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
package org.apache.accumulo.core.iterators.conf;

import static com.google.common.base.Charsets.UTF_8;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.conf.ColumnUtil.ColFamHashKey;
import org.apache.accumulo.core.iterators.conf.ColumnUtil.ColHashKey;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;

public class ColumnSet {
  private Set<ColFamHashKey> objectsCF;
  private Set<ColHashKey> objectsCol;

  private ColHashKey lookupCol = new ColHashKey();
  private ColFamHashKey lookupCF = new ColFamHashKey();

  public ColumnSet() {
    objectsCF = new HashSet<ColFamHashKey>();
    objectsCol = new HashSet<ColHashKey>();
  }

  public ColumnSet(Collection<String> objectStrings) {
    this();

    for (String column : objectStrings) {
      Pair<Text,Text> pcic = ColumnSet.decodeColumns(column);

      if (pcic.getSecond() == null) {
        add(pcic.getFirst());
      } else {
        add(pcic.getFirst(), pcic.getSecond());
      }
    }
  }

  protected void add(Text colf) {
    objectsCF.add(new ColFamHashKey(new Text(colf)));
  }

  protected void add(Text colf, Text colq) {
    objectsCol.add(new ColHashKey(colf, colq));
  }

  public boolean contains(Key key) {
    // lookup column family and column qualifier
    if (objectsCol.size() > 0) {
      lookupCol.set(key);
      if (objectsCol.contains(lookupCol))
        return true;
    }

    // lookup just column family
    if (objectsCF.size() > 0) {
      lookupCF.set(key);
      return objectsCF.contains(lookupCF);
    }

    return false;
  }

  public boolean isEmpty() {
    return objectsCol.size() == 0 && objectsCF.size() == 0;
  }

  public static String encodeColumns(Text columnFamily, Text columnQualifier) {
    StringBuilder sb = new StringBuilder();

    encode(sb, columnFamily);
    if (columnQualifier != null) {
      sb.append(':');
      encode(sb, columnQualifier);
    }

    return sb.toString();
  }

  static void encode(StringBuilder sb, Text t) {
    for (int i = 0; i < t.getLength(); i++) {
      int b = (0xff & t.getBytes()[i]);

      // very inefficient code
      if ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '-') {
        sb.append((char) b);
      } else {
        sb.append('%');
        sb.append(String.format("%02x", b));
      }
    }
  }

  public static boolean isValidEncoding(String enc) {
    for (char c : enc.toCharArray()) {
      boolean validChar = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == ':' || c == '%';
      if (!validChar)
        return false;
    }

    return true;
  }

  public static Pair<Text,Text> decodeColumns(String columns) {
    if (!isValidEncoding(columns))
      throw new IllegalArgumentException("Invalid encoding " + columns);

    String[] cols = columns.split(":");

    if (cols.length == 1) {
      return new Pair<Text,Text>(decode(cols[0]), null);
    } else if (cols.length == 2) {
      return new Pair<Text,Text>(decode(cols[0]), decode(cols[1]));
    } else {
      throw new IllegalArgumentException(columns);
    }
  }

  static Text decode(String s) {
    Text t = new Text();

    byte[] sb = s.getBytes(UTF_8);

    // very inefficient code
    for (int i = 0; i < sb.length; i++) {
      if (sb[i] != '%') {
        t.append(new byte[] {sb[i]}, 0, 1);
      } else {
        byte hex[] = new byte[] {sb[++i], sb[++i]};
        String hs = new String(hex, UTF_8);
        int b = Integer.parseInt(hs, 16);
        t.append(new byte[] {(byte) b}, 0, 1);
      }
    }

    return t;
  }
}
