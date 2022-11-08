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
import org.junit.jupiter.api.Test;

public class LongLexicoderTest extends AbstractLexicoderTest {

  @Test
  public void testSortOrder() {

    assertSortOrder(new LongLexicoder(),
        Arrays.asList(Long.MIN_VALUE, 0xff1234567890abcdL, 0xffff1234567890abL, 0xffffff567890abcdL,
            0xffffffff7890abcdL, 0xffffffffff90abcdL, 0xffffffffffffabcdL, 0xffffffffffffffcdL, -1L,
            0L, 0x01L, 0x1234L, 0x123456L, 0x12345678L, 0x1234567890L, 0x1234567890abL,
            0x1234567890abcdL, 0x1234567890abcdefL, Long.MAX_VALUE));
  }

  @Test
  public void testDecodes() {
    assertDecodes(new LongLexicoder(), Long.MIN_VALUE);
    assertDecodes(new LongLexicoder(), -1L);
    assertDecodes(new LongLexicoder(), 0L);
    assertDecodes(new LongLexicoder(), 1L);
    assertDecodes(new LongLexicoder(), 2L);
    assertDecodes(new LongLexicoder(), Long.MAX_VALUE);
  }
}
