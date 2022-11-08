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

public class DoubleLexicoderTest extends AbstractLexicoderTest {

  @Test
  public void testSortOrder() {
    assertSortOrder(new DoubleLexicoder(),
        Arrays.asList(Double.MIN_VALUE, Double.MAX_VALUE, Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY, 0.0, 0.01, 0.001, 1.0, -1.0, -1.1, -1.01,
            Math.nextUp(Double.NEGATIVE_INFINITY), Math.nextAfter(0.0, Double.NEGATIVE_INFINITY),
            Math.nextAfter(Double.MAX_VALUE, Double.NEGATIVE_INFINITY), Math.pow(10.0, 30.0) * -1.0,
            Math.pow(10.0, 30.0), Math.pow(10.0, -30.0) * -1.0, Math.pow(10.0, -30.0)));

  }

  @Test
  public void testDecode() {
    assertDecodes(new DoubleLexicoder(), Double.MIN_VALUE);
    assertDecodes(new DoubleLexicoder(), -1.0);
    assertDecodes(new DoubleLexicoder(), -Math.pow(10.0, -30.0));
    assertDecodes(new DoubleLexicoder(), 0.0);
    assertDecodes(new DoubleLexicoder(), Math.pow(10.0, -30.0));
    assertDecodes(new DoubleLexicoder(), 1.0);
    assertDecodes(new DoubleLexicoder(), Double.MAX_VALUE);
  }
}
