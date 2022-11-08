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

import org.apache.accumulo.core.clientImpl.lexicoder.AbstractLexicoderTest;
import org.junit.jupiter.api.Test;

public class UIntegerLexicoderTest extends AbstractLexicoderTest {

  @Test
  public void testEncoding() {
    UIntegerLexicoder uil = new UIntegerLexicoder();

    assertEqualsB(uil.encode(0), new byte[] {0x00});
    assertEqualsB(uil.encode(0x01), new byte[] {0x01, 0x01});
    assertEqualsB(uil.encode(0x0102), new byte[] {0x02, 0x01, 0x02});
    assertEqualsB(uil.encode(0x010203), new byte[] {0x03, 0x01, 0x02, 0x03});
    assertEqualsB(uil.encode(0x01020304), new byte[] {0x04, 0x01, 0x02, 0x03, 0x04});
    assertEqualsB(uil.encode(0xff020304), new byte[] {0x05, 0x02, 0x03, 0x04});
    assertEqualsB(uil.encode(0xffff0304), new byte[] {0x06, 0x03, 0x04});
    assertEqualsB(uil.encode(0xffffff04), new byte[] {0x07, 0x04});
    assertEqualsB(uil.encode(-1), new byte[] {0x08});
  }

  @Test
  public void testDecode() {
    assertDecodes(new UIntegerLexicoder(), Integer.MIN_VALUE);
    assertDecodes(new UIntegerLexicoder(), -1);
    assertDecodes(new UIntegerLexicoder(), 0);
    assertDecodes(new UIntegerLexicoder(), 1);
    assertDecodes(new UIntegerLexicoder(), Integer.MAX_VALUE);
  }
}
