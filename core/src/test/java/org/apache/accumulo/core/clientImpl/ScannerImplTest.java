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
package org.apache.accumulo.core.clientImpl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.security.Authorizations;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScannerImplTest {

  private ClientContext context;

  @BeforeEach
  public void setup() {
    context = EasyMock.createMock(ClientContext.class);
  }

  @Test
  public void testValidReadaheadValues() {
    try (var s = new ScannerImpl(context, TableId.of("foo"), Authorizations.EMPTY)) {
      s.setReadaheadThreshold(0);
      s.setReadaheadThreshold(10);
      s.setReadaheadThreshold(Long.MAX_VALUE);

      assertEquals(Long.MAX_VALUE, s.getReadaheadThreshold());
    }
  }

  @Test
  public void testInValidReadaheadValues() {
    try (var s = new ScannerImpl(context, TableId.of("foo"), Authorizations.EMPTY)) {
      assertThrows(IllegalArgumentException.class, () -> s.setReadaheadThreshold(-1));
    }
  }

  @Test
  public void testGetAuthorizations() {
    Authorizations expected = new Authorizations("a,b");
    try (var s = new ScannerImpl(context, TableId.of("foo"), expected)) {
      assertEquals(expected, s.getAuthorizations());
    }
  }

  @Test
  public void testNullAuthorizationsFails() {
    assertThrows(IllegalArgumentException.class,
        () -> new ScannerImpl(context, TableId.of("foo"), null));
  }

}
