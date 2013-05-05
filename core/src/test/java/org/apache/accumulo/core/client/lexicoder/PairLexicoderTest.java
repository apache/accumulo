/**
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

import org.apache.accumulo.core.util.Pair;

/**
 * 
 */
public class PairLexicoderTest extends LexicoderTest {
  public void testSortOrder() {
    PairLexicoder<String,String> plexc = new PairLexicoder<String,String>(new StringLexicoder(), new StringLexicoder());
    
    assertSortOrder(plexc, new Pair<String,String>("a", "b"), new Pair<String,String>("a", "bc"), new Pair<String,String>("a", "c"), new Pair<String,String>(
        "ab", "c"), new Pair<String,String>("ab", ""), new Pair<String,String>("ab", "d"), new Pair<String,String>("b", "f"), new Pair<String,String>("b", "a"));
    
    PairLexicoder<Long,String> plexc2 = new PairLexicoder<Long,String>(new LongLexicoder(), new StringLexicoder());
    
    assertSortOrder(plexc2, new Pair<Long,String>(0x100l, "a"), new Pair<Long,String>(0x100l, "ab"), new Pair<Long,String>(0xf0l, "a"), new Pair<Long,String>(
        0xf0l, "ab"));
  }
}
