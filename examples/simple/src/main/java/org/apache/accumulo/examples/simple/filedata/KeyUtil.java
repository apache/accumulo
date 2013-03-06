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
package org.apache.accumulo.examples.simple.filedata;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;

/**
 * A utility for creating and parsing null-byte separated strings into/from Text objects.
 */
public class KeyUtil {
  public static final byte[] nullbyte = new byte[] {0};

  /**
   * Join some number of strings using a null byte separator into a text object.
   *
   * @param s
   *          strings
   * @return a text object containing the strings separated by null bytes
   */
  public static Text buildNullSepText(String... s) {
    Text t = new Text(s[0]);
    for (int i = 1; i < s.length; i++) {
      t.append(nullbyte, 0, 1);
      t.append(s[i].getBytes(), 0, s[i].length());
    }
    return t;
  }

  /**
   * Split a text object using a null byte separator into an array of strings.
   *
   * @param t
   *          null-byte separated text object
   * @return an array of strings
   */
  public static String[] splitNullSepText(Text t) {
    ArrayList<String> s = new ArrayList<String>();
    byte[] b = t.getBytes();
    int lastindex = 0;
    for (int i = 0; i < t.getLength(); i++) {
      if (b[i] == (byte) 0) {
        s.add(new String(b, lastindex, i - lastindex));
        lastindex = i + 1;
      }
    }
    s.add(new String(b, lastindex, t.getLength() - lastindex));
    return s.toArray(new String[s.size()]);
  }
}
