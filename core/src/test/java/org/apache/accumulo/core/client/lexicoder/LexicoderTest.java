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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import junit.framework.TestCase;

import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

public abstract class LexicoderTest extends TestCase {

  void assertEqualsB(byte[] ba1, byte[] ba2) {
    assertEquals(new Text(ba2), new Text(ba1));
  }

  public <T extends Comparable<T>> void assertSortOrder(Lexicoder<T> lexicoder, Comparator<T> comp, T... data) {
    List<T> list = new ArrayList<T>();
    List<Text> encList = new ArrayList<Text>();

    for (T d : data) {
      list.add(d);
      encList.add(new Text(lexicoder.encode(d)));
    }

    if (comp != null)
      Collections.sort(list, comp);
    else
      Collections.sort(list);

    Collections.sort(encList);

    List<T> decodedList = new ArrayList<T>();

    for (Text t : encList) {
      decodedList.add(lexicoder.decode(TextUtil.getBytes(t)));
    }

    assertEquals(list, decodedList);
  }

  public <T extends Comparable<T>> void assertSortOrder(Lexicoder<T> lexicoder, T... data) {
    assertSortOrder(lexicoder, null, data);
  }

}
