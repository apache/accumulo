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

public class IntegerLexicoderTest extends LexicoderTest {
  public void testSortOrder() {
    assertSortOrder(new IntegerLexicoder(), Integer.MIN_VALUE, 0xff123456, 0xffff3456, 0xffffff56, -1, 0, 1, 0x12, 0x1234, 0x123456, 0x1234678,
        Integer.MAX_VALUE);
  }
}
