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
package org.apache.accumulo.fate;

import java.util.Collections;
import java.util.EnumSet;

import org.apache.accumulo.fate.ReadOnlyTStore.TStatus;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

/**
 * Make sure read only decorate passes read methods.
 */
public class ReadOnlyStoreTest {

  @Test
  public void everythingPassesThrough() throws Exception {
    @SuppressWarnings("unchecked")
    Repo<String> repo = EasyMock.createMock(Repo.class);
    EasyMock.expect(repo.getDescription()).andReturn("description");
    EasyMock.expect(repo.isReady(0xdeadbeefl, null)).andReturn(0x0l);

    @SuppressWarnings("unchecked")
    TStore<String> mock = EasyMock.createNiceMock(TStore.class);
    EasyMock.expect(mock.reserve()).andReturn(0xdeadbeefl);
    mock.reserve(0xdeadbeefl);
    EasyMock.expect(mock.top(0xdeadbeefl)).andReturn(repo);
    EasyMock.expect(mock.getStatus(0xdeadbeefl)).andReturn(TStatus.UNKNOWN);
    mock.unreserve(0xdeadbeefl, 30);

    EasyMock.expect(mock.waitForStatusChange(0xdeadbeefl, EnumSet.allOf(TStatus.class))).andReturn(TStatus.UNKNOWN);
    EasyMock.expect(mock.getProperty(0xdeadbeefl, "com.example.anyproperty")).andReturn("property");
    EasyMock.expect(mock.list()).andReturn(Collections.<Long> emptyList());

    EasyMock.replay(repo);
    EasyMock.replay(mock);

    ReadOnlyTStore<String> store = new ReadOnlyStore<>(mock);
    Assert.assertEquals(0xdeadbeefl, store.reserve());
    store.reserve(0xdeadbeefl);
    ReadOnlyRepo<String> top = store.top(0xdeadbeefl);
    Assert.assertFalse(top instanceof Repo);
    Assert.assertEquals("description", top.getDescription());
    Assert.assertEquals(0x0l, top.isReady(0xdeadbeefl, null));
    Assert.assertEquals(TStatus.UNKNOWN, store.getStatus(0xdeadbeefl));
    store.unreserve(0xdeadbeefl, 30);

    Assert.assertEquals(TStatus.UNKNOWN, store.waitForStatusChange(0xdeadbeefl, EnumSet.allOf(TStatus.class)));
    Assert.assertEquals("property", store.getProperty(0xdeadbeefl, "com.example.anyproperty"));
    Assert.assertEquals(Collections.<Long> emptyList(), store.list());

    EasyMock.verify(repo);
    EasyMock.verify(mock);
  }
}
