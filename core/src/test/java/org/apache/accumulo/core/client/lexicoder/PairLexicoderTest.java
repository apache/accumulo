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

import java.util.Arrays;

import org.apache.accumulo.core.clientImpl.lexicoder.AbstractLexicoderTest;
import org.apache.accumulo.core.util.ComparablePair;
import org.junit.jupiter.api.Test;

public class PairLexicoderTest extends AbstractLexicoderTest {

  @Test
  public void testSortOrder() {
    PairLexicoder<String,String> plexc =
        new PairLexicoder<>(new StringLexicoder(), new StringLexicoder());

    assertSortOrder(plexc,
        Arrays.asList(new ComparablePair<>("a", "b"), new ComparablePair<>("a", "bc"),
            new ComparablePair<>("a", "c"), new ComparablePair<>("ab", "c"),
            new ComparablePair<>("ab", ""), new ComparablePair<>("ab", "d"),
            new ComparablePair<>("b", "f"), new ComparablePair<>("b", "a")));

    PairLexicoder<Long,String> plexc2 =
        new PairLexicoder<>(new LongLexicoder(), new StringLexicoder());

    assertSortOrder(plexc2,
        Arrays.asList(new ComparablePair<>(0x100L, "a"), new ComparablePair<>(0x100L, "ab"),
            new ComparablePair<>(0xf0L, "a"), new ComparablePair<>(0xf0L, "ab")));
  }

  @Test
  public void testDecodes() {
    PairLexicoder<String,String> plexc =
        new PairLexicoder<>(new StringLexicoder(), new StringLexicoder());
    assertDecodes(plexc, new ComparablePair<>("a", "b"));
  }
}
