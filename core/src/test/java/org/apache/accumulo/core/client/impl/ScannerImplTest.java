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

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.security.Authorizations;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class ScannerImplTest {

  private ClientContext context;

  @Before
  public void setup() {
    context = EasyMock.createMock(ClientContext.class);
  }

  @Test
  public void testValidReadaheadValues() {
    Scanner s = new ScannerImpl(context, Table.ID.of("foo"), Authorizations.EMPTY);
    s.setReadaheadThreshold(0);
    s.setReadaheadThreshold(10);
    s.setReadaheadThreshold(Long.MAX_VALUE);

    Assert.assertEquals(Long.MAX_VALUE, s.getReadaheadThreshold());
    s.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInValidReadaheadValues() {
    Scanner s = new ScannerImpl(context, Table.ID.of("foo"), Authorizations.EMPTY);
    s.setReadaheadThreshold(-1);
    s.close();
  }

  @Test
  public void testGetAuthorizations() {
    Authorizations expected = new Authorizations("a,b");
    Scanner s = new ScannerImpl(context, Table.ID.of("foo"), expected);
    assertEquals(expected, s.getAuthorizations());
    s.close();
  }

  @SuppressWarnings("resource")
  @Test(expected = IllegalArgumentException.class)
  public void testNullAuthorizationsFails() {
    new ScannerImpl(context, Table.ID.of("foo"), null);
  }

}
