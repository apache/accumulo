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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import org.apache.accumulo.core.clientImpl.lexicoder.AbstractLexicoderTest;
import org.junit.jupiter.api.Test;

public class UUIDLexicoderTest extends AbstractLexicoderTest {

  @Test
  public void testSortOrder() {

    assertSortOrder(new UUIDLexicoder(), Arrays.asList(UUID.randomUUID(), UUID.randomUUID(),
        UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()));

    ArrayList<UUID> uuids = new ArrayList<>();

    for (long ms = -260L; ms < 260L; ms++) {
      for (long ls = -2L; ls < 2; ls++) {
        uuids.add(new UUID(ms, ls));
      }
    }

    assertSortOrder(new UUIDLexicoder(), uuids);
  }

  @Test
  public void testDecodes() {
    assertDecodes(new UUIDLexicoder(), UUID.randomUUID());

  }
}
