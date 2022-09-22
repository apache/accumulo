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

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TabletServerBatchReaderTest {

  private ClientContext context;

  @BeforeEach
  public void setup() {
    context = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(context.threadPools()).andReturn(ThreadPools.getServerThreadPools());
    EasyMock.replay(context);
  }

  @Test
  public void testGetAuthorizations() {
    Authorizations expected = new Authorizations("a,b");
    try (BatchScanner s =
        new TabletServerBatchReader(context, TableId.of("foo"), "fooName", expected, 1)) {
      assertEquals(expected, s.getAuthorizations());
    }
  }

  @Test
  public void testNullAuthorizationsFails() {
    assertThrows(IllegalArgumentException.class,
        () -> new TabletServerBatchReader(context, TableId.of("foo"), "fooName", null, 1));
  }
}
