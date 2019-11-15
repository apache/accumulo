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
package org.apache.accumulo.fate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Collections;
import java.util.EnumSet;

import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.easymock.EasyMock;
import org.junit.Test;

/**
 * Make sure read only decorate passes read methods.
 */
public class ReadOnlyStoreTest {

  @Test
  public void everythingPassesThrough() throws Exception {
    Repo<String> repo = EasyMock.createMock(Repo.class);
    EasyMock.expect(repo.getDescription()).andReturn("description");
    EasyMock.expect(repo.isReady(0xdeadbeefL, null)).andReturn(0x0L);

    TStore<String> mock = EasyMock.createNiceMock(TStore.class);
    EasyMock.expect(mock.reserve()).andReturn(0xdeadbeefL);
    mock.reserve(0xdeadbeefL);
    EasyMock.expect(mock.top(0xdeadbeefL)).andReturn(repo);
    EasyMock.expect(mock.getStatus(0xdeadbeefL)).andReturn(TStatus.UNKNOWN);
    mock.unreserve(0xdeadbeefL, 30);

    EasyMock.expect(mock.waitForStatusChange(0xdeadbeefL, EnumSet.allOf(TStatus.class)))
        .andReturn(TStatus.UNKNOWN);
    EasyMock.expect(mock.getProperty(0xdeadbeefL, "com.example.anyproperty")).andReturn("property");
    EasyMock.expect(mock.list()).andReturn(Collections.emptyList());

    EasyMock.replay(repo);
    EasyMock.replay(mock);

    ReadOnlyTStore<String> store = new ReadOnlyStore<>(mock);
    assertEquals(0xdeadbeefL, store.reserve());
    store.reserve(0xdeadbeefL);
    ReadOnlyRepo<String> top = store.top(0xdeadbeefL);
    assertFalse(top instanceof Repo);
    assertEquals("description", top.getDescription());
    assertEquals(0x0L, top.isReady(0xdeadbeefL, null));
    assertEquals(TStatus.UNKNOWN, store.getStatus(0xdeadbeefL));
    store.unreserve(0xdeadbeefL, 30);

    assertEquals(TStatus.UNKNOWN,
        store.waitForStatusChange(0xdeadbeefL, EnumSet.allOf(TStatus.class)));
    assertEquals("property", store.getProperty(0xdeadbeefL, "com.example.anyproperty"));
    assertEquals(Collections.<Long>emptyList(), store.list());

    EasyMock.verify(repo);
    EasyMock.verify(mock);
  }
}
