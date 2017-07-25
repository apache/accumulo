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

import java.util.Arrays;

import org.apache.accumulo.core.client.lexicoder.impl.AbstractLexicoderTest;

public class LongLexicoderTest extends AbstractLexicoderTest {
  public void testSortOrder() {

    assertSortOrder(new LongLexicoder(), Arrays.asList(Long.MIN_VALUE, 0xff1234567890abcdl, 0xffff1234567890abl, 0xffffff567890abcdl, 0xffffffff7890abcdl,
        0xffffffffff90abcdl, 0xffffffffffffabcdl, 0xffffffffffffffcdl, -1l, 0l, 0x01l, 0x1234l, 0x123456l, 0x12345678l, 0x1234567890l, 0x1234567890abl,
        0x1234567890abcdl, 0x1234567890abcdefl, Long.MAX_VALUE));
  }

  public void testDecodes() {
    assertDecodes(new LongLexicoder(), Long.MIN_VALUE);
    assertDecodes(new LongLexicoder(), -1l);
    assertDecodes(new LongLexicoder(), 0l);
    assertDecodes(new LongLexicoder(), 1l);
    assertDecodes(new LongLexicoder(), 2l);
    assertDecodes(new LongLexicoder(), Long.MAX_VALUE);
  }
}
