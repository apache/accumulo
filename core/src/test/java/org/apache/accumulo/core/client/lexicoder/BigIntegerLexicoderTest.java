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

import java.math.BigInteger;

/**
 *
 */
public class BigIntegerLexicoderTest extends LexicoderTest {
  public void testSortOrder() {
    assertSortOrder(new BigIntegerLexicoder(), new BigInteger("-1"), new BigInteger("0"), new BigInteger("1"), new BigInteger("-257"), new BigInteger("-256"),
        new BigInteger("-255"), new BigInteger("255"), new BigInteger("256"), new BigInteger("257"), new BigInteger("65534"), new BigInteger("65535"),
        new BigInteger("65536"), new BigInteger("65537"), new BigInteger("-65534"), new BigInteger("-65535"), new BigInteger("-65536"),
        new BigInteger("-65537"), new BigInteger("2147483648"), new BigInteger("2147483647"), new BigInteger("2147483649"), new BigInteger("-2147483648"),
        new BigInteger("-2147483647"), new BigInteger("-2147483649"), new BigInteger("32768"), new BigInteger("32769"), new BigInteger("32767"),
        new BigInteger("-32768"), new BigInteger("-32769"), new BigInteger("-32767"), new BigInteger("126"), new BigInteger("127"), new BigInteger("128"),
        new BigInteger("129"), new BigInteger("-126"), new BigInteger("-127"), new BigInteger("-128"), new BigInteger("-129"));

  }
}
