/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.client.lexicoder;

import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.accumulo.core.clientImpl.lexicoder.AbstractLexicoderTest;
import org.junit.jupiter.api.Test;

public class BigDecimalLexicoderTest extends AbstractLexicoderTest {

  @Test
  public void testSortOrder() {

    assertSortOrder(new BigDecimalLexicoder(), BigDecimal::compareTo,
        Arrays.asList(new BigDecimal("2.0"), new BigDecimal("2.00"), new BigDecimal("2.000"),
            new BigDecimal("-3.000"), new BigDecimal("-2.00"), new BigDecimal("0.0000"),
            new BigDecimal("0.1"), new BigDecimal("0.10"), new BigDecimal("-65537.000"),
            new BigDecimal("-65537.00"), new BigDecimal("-65537.0")));

  }

  @Test
  public void testDecode() {
    assertDecodes(new BigDecimalLexicoder(), BigDecimal.valueOf(-3.000));
    assertDecodes(new BigDecimalLexicoder(), new BigDecimal("2.00"));
    assertDecodes(new BigDecimalLexicoder(), new BigDecimal("2.000"));

    assertDecodes(new BigDecimalLexicoder(), new BigDecimal("-2.00"));
    assertDecodes(new BigDecimalLexicoder(), BigDecimal.valueOf(0.1));

    assertDecodes(new BigDecimalLexicoder(), BigDecimal.valueOf(2.000));

    assertDecodes(new BigDecimalLexicoder(), new BigDecimal("20.03"));
    assertDecodes(new BigDecimalLexicoder(), new BigDecimal("56.67890"));
    assertDecodes(new BigDecimalLexicoder(), new BigDecimal("1.2345E-12"));
    assertDecodes(new BigDecimalLexicoder(), new BigDecimal("4.9e-324"));
    assertDecodes(new BigDecimalLexicoder(), BigDecimal.valueOf(1.000000D));

  }
}
