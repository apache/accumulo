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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.accumulo.core.clientImpl.lexicoder.AbstractLexicoderTest;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SequenceLexicoder}.
 */
public class SequenceLexicoderTest extends AbstractLexicoderTest {

  private final List<String> nodata = emptyList();
  private final List<String> data0 = singletonList("");
  private final List<String> data1 = asList("a", "b");
  private final List<String> data2 = singletonList("a");
  private final List<String> data3 = asList("a", "c");
  private final List<String> data4 = asList("a", "b", "c");
  private final List<String> data5 = asList("b", "a");

  @Test
  public void testSortOrder() {
    // expected sort order
    final List<List<String>> data = asList(nodata, data0, data2, data1, data4, data3, data5);
    final TreeSet<Text> sortedEnc = new TreeSet<>();
    final SequenceLexicoder<String> sequenceLexicoder =
        new SequenceLexicoder<>(new StringLexicoder());
    for (final List<String> list : data) {
      sortedEnc.add(new Text(sequenceLexicoder.encode(list)));
    }
    final List<List<String>> unenc = new ArrayList<>();
    for (final Text enc : sortedEnc) {
      unenc.add(sequenceLexicoder.decode(TextUtil.getBytes(enc)));
    }
    assertEquals(data, unenc);
  }

  @Test
  public void testDecodes() {
    assertDecodes(new SequenceLexicoder<>(new StringLexicoder()), nodata);
    assertDecodes(new SequenceLexicoder<>(new StringLexicoder()), data0);
    assertDecodes(new SequenceLexicoder<>(new StringLexicoder()), data1);
    assertDecodes(new SequenceLexicoder<>(new StringLexicoder()), data2);
    assertDecodes(new SequenceLexicoder<>(new StringLexicoder()), data3);
    assertDecodes(new SequenceLexicoder<>(new StringLexicoder()), data4);
    assertDecodes(new SequenceLexicoder<>(new StringLexicoder()), data5);
  }

  @Test
  public void tesRejectsTrailingBytes() {
    assertThrows(IllegalArgumentException.class,
        () -> new SequenceLexicoder<>(new StringLexicoder()).decode(new byte[] {10}));
  }
}
