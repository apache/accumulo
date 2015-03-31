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

import org.apache.accumulo.core.client.lexicoder.impl.AbstractLexicoderTest;
import org.apache.accumulo.core.util.ComparablePair;

import java.util.Arrays;

/**
 *
 */
public class PairLexicoderTest extends AbstractLexicoderTest {
  public void testSortOrder() {
    PairLexicoder<String,String> plexc = new PairLexicoder<String,String>(new StringLexicoder(), new StringLexicoder());

    assertSortOrder(plexc, Arrays.asList(new ComparablePair<String,String>("a", "b"), new ComparablePair<String,String>("a", "bc"),
        new ComparablePair<String,String>("a", "c"), new ComparablePair<String,String>("ab", "c"), new ComparablePair<String,String>("ab", ""),
        new ComparablePair<String,String>("ab", "d"), new ComparablePair<String,String>("b", "f"), new ComparablePair<String,String>("b", "a")));

    PairLexicoder<Long,String> plexc2 = new PairLexicoder<Long,String>(new LongLexicoder(), new StringLexicoder());

    assertSortOrder(plexc2, Arrays.asList(new ComparablePair<Long,String>(0x100l, "a"), new ComparablePair<Long,String>(0x100l, "ab"),
        new ComparablePair<Long,String>(0xf0l, "a"), new ComparablePair<Long,String>(0xf0l, "ab")));
  }

  public void testDecodes() {
    PairLexicoder<String,String> plexc = new PairLexicoder<String,String>(new StringLexicoder(), new StringLexicoder());
    assertDecodes(plexc, new ComparablePair<String,String>("a", "b"));
  }
}
