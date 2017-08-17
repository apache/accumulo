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
package org.apache.accumulo.core.client.impl;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.security.Authorizations;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class TabletServerBatchReaderTest {

  private ClientContext context;

  @Before
  public void setup() {
    context = EasyMock.createMock(ClientContext.class);
  }

  @Test
  public void testGetAuthorizations() {
    Authorizations expected = new Authorizations("a,b");
    try (BatchScanner s = new TabletServerBatchReader(context, Table.ID.of("foo"), expected, 1)) {
      assertEquals(expected, s.getAuthorizations());
    }
  }

  @SuppressWarnings("resource")
  @Test(expected = IllegalArgumentException.class)
  public void testNullAuthorizationsFails() {
    new TabletServerBatchReader(context, Table.ID.of("foo"), null, 1);
  }
}
