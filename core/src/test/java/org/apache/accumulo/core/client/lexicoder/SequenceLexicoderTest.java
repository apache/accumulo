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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.accumulo.core.clientImpl.lexicoder.AbstractLexicoderTest;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class SequenceLexicoderTest extends AbstractLexicoderTest {

  private final List<Long> data0 = new ArrayList<>();
  private final List<Long> data1 = new ArrayList<>();
  private final List<Long> data2 = new ArrayList<>();
  private final List<Long> data3 = new ArrayList<>();
  private final List<Long> data4 = new ArrayList<>();
  private final List<Long> data5 = new ArrayList<>();

  @Before
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
    final List<List<Long>> data = new ArrayList<>();

    // add list in expected sort order
    data.add(data0);
    data.add(data2);
    data.add(data1);
    data.add(data4);
    data.add(data3);
    data.add(data5);

    final TreeSet<Text> sortedEnc = new TreeSet<>();

    final SequenceLexicoder<Long> sequenceLexicoder = new SequenceLexicoder<>(new LongLexicoder());

    for (final List<Long> list : data) {
      sortedEnc.add(new Text(sequenceLexicoder.encode(list)));
    }

    final List<List<Long>> unenc = new ArrayList<>();

    for (final Text enc : sortedEnc) {
      unenc.add(sequenceLexicoder.decode(TextUtil.getBytes(enc)));
    }

    assertEquals(data, unenc);

  }

  @Test
  public void testDecodes() {
    assertDecodes(new SequenceLexicoder<>(new LongLexicoder()), data0);
    assertDecodes(new SequenceLexicoder<>(new LongLexicoder()), data1);
    assertDecodes(new SequenceLexicoder<>(new LongLexicoder()), data2);
    assertDecodes(new SequenceLexicoder<>(new LongLexicoder()), data3);
    assertDecodes(new SequenceLexicoder<>(new LongLexicoder()), data4);
    assertDecodes(new SequenceLexicoder<>(new LongLexicoder()), data5);
  }
}
