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

import java.util.ArrayList;
import java.util.UUID;

public class UUIDLexicoderTest extends LexicoderTest {
  public void testSortOrder() {

    assertSortOrder(new UUIDLexicoder(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

    ArrayList<UUID> uuids = new ArrayList<UUID>();

    for (long ms = -260l; ms < 260l; ms++) {
      for (long ls = -2l; ls < 2; ls++) {
        uuids.add(new UUID(ms, ls));
      }
    }

    assertSortOrder(new UUIDLexicoder(), uuids.toArray(new UUID[0]));
  }
}
