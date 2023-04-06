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

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.accumulo.core.clientImpl.lexicoder.AbstractLexicoderTest;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ListLexicoderTest extends AbstractLexicoderTest {

  private List<Long> data1 = new ArrayList<>();
  private List<Long> data2 = new ArrayList<>();
  private List<Long> data3 = new ArrayList<>();
  private List<Long> data4 = new ArrayList<>();
  private List<Long> data5 = new ArrayList<>();

  @BeforeEach
  public void setUp() {

    data1.add(1L);
    data1.add(2L);

    data2.add(1L);

    data3.add(1L);
    data3.add(3L);

    data4.add(1L);
    data4.add(2L);
    data4.add(3L);

    data5.add(2L);
    data5.add(1L);
  }

  @Test
  public void testSortOrder() {
    List<List<Long>> data = new ArrayList<>();

    // add list in expected sort order
    data.add(data2);
    data.add(data1);
    data.add(data4);
    data.add(data3);
    data.add(data5);

    TreeSet<Text> sortedEnc = new TreeSet<>();

    ListLexicoder<Long> listLexicoder = new ListLexicoder<>(new LongLexicoder());

    for (List<Long> list : data) {
      sortedEnc.add(new Text(listLexicoder.encode(list)));
    }

    List<List<Long>> unenc = new ArrayList<>();

    for (Text enc : sortedEnc) {
      unenc.add(listLexicoder.decode(TextUtil.getBytes(enc)));
    }

    assertEquals(data, unenc);

  }

  @Test
  public void testDecodes() {
    assertDecodes(new ListLexicoder<>(new LongLexicoder()), data1);
    assertDecodes(new ListLexicoder<>(new LongLexicoder()), data2);
    assertDecodes(new ListLexicoder<>(new LongLexicoder()), data3);
    assertDecodes(new ListLexicoder<>(new LongLexicoder()), data4);
    assertDecodes(new ListLexicoder<>(new LongLexicoder()), data5);
  }

  @Test
  public void testRejectsEmptyLists() {
    assertThrows(IllegalArgumentException.class,
        () -> new ListLexicoder<>(new LongLexicoder()).encode(emptyList()));
  }
}
