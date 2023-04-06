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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

public abstract class LexicoderTest {

  protected void assertEqualsB(byte[] ba1, byte[] ba2) {
    assertEquals(new Text(ba2), new Text(ba1));
  }

  public <T extends Comparable<T>> void assertSortOrder(Lexicoder<T> lexicoder, Comparator<T> comp,
      List<T> data) {
    List<T> list = new ArrayList<>();
    List<Text> encList = new ArrayList<>();

    for (T d : data) {
      list.add(d);
      encList.add(new Text(lexicoder.encode(d)));
    }

    if (comp != null) {
      list.sort(comp);
    } else {
      Collections.sort(list);
    }

    Collections.sort(encList);

    List<T> decodedList = new ArrayList<>();

    for (Text t : encList) {
      decodedList.add(lexicoder.decode(TextUtil.getBytes(t)));
    }

    assertEquals(list, decodedList);
  }

  public <T extends Comparable<T>> void assertSortOrder(Lexicoder<T> lexicoder, List<T> data) {
    assertSortOrder(lexicoder, null, data);
  }

  public static final byte[] START_PAD = "start".getBytes();
  public static final byte[] END_PAD = "end".getBytes();

  /** Asserts a value can be encoded and decoded back to original value */
  public static <T> void assertDecodes(Lexicoder<T> lexicoder, T expected) {
    byte[] encoded = lexicoder.encode(expected);

    // decode full array
    T result = lexicoder.decode(encoded);
    assertEquals(expected, result);
  }

  public void assertDecodesB(Lexicoder<byte[]> lexicoder, byte[] expected) {
    byte[] encoded = lexicoder.encode(expected);

    // decode full array
    byte[] result = lexicoder.decode(encoded);
    assertEqualsB(expected, result);
  }

}
