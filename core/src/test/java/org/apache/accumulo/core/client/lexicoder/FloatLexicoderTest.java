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

/**
 *
 */
public class FloatLexicoderTest extends AbstractLexicoderTest {

  public void testSortOrder() {
    assertSortOrder(
        new FloatLexicoder(),
        Arrays.asList(Float.MIN_VALUE, Float.MAX_VALUE, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, 0.0F, 0.01F, 0.001F, 1.0F, -1.0F, -1.1F, -1.01F,
            Math.nextUp(Float.NEGATIVE_INFINITY), Math.nextAfter(0.0F, Float.NEGATIVE_INFINITY), Math.nextAfter(Float.MAX_VALUE, Float.NEGATIVE_INFINITY)));

  }

  public void testDecode() {
    assertDecodes(new FloatLexicoder(), Float.MIN_VALUE);
    assertDecodes(new FloatLexicoder(), Math.nextUp(Float.NEGATIVE_INFINITY));
    assertDecodes(new FloatLexicoder(), -1.0F);
    assertDecodes(new FloatLexicoder(), 0.0F);
    assertDecodes(new FloatLexicoder(), 1.0F);
    assertDecodes(new FloatLexicoder(), Math.nextAfter(Float.POSITIVE_INFINITY, 0.0F));
    assertDecodes(new FloatLexicoder(), Float.MAX_VALUE);
  }
}
