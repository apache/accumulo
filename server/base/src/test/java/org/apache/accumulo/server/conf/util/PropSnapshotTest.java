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
package org.apache.accumulo.server.conf.util;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PropSnapshotTest {

  private InstanceId instanceId;
  private PropStore propStore;

  @BeforeEach
  public void init() {
    instanceId = InstanceId.of(UUID.randomUUID());
    propStore = createMock(ZooPropStore.class);
  }

  @Test
  public void getTest() {
    // init props
    expect(propStore.get(eq(PropCacheKey.forSystem(instanceId))))
        .andReturn(new VersionedProperties(123, Instant.now(), Map.of("k1", "v1", "k2", "v2")))
        .once();
    // after update
    expect(propStore.get(eq(PropCacheKey.forSystem(instanceId))))
        .andReturn(new VersionedProperties(124, Instant.now(), Map.of("k3", "v3"))).once();

    replay(propStore);
    PropSnapshot snapshot = new PropSnapshot(PropCacheKey.forSystem(instanceId), propStore);

    assertEquals("v1", snapshot.get().getProperties().get("k1"));
    assertEquals("v2", snapshot.get().getProperties().get("k2"));
    assertNull(snapshot.get().getProperties().get("k3"));

    snapshot.requireUpdate();

    assertEquals("v3", snapshot.get().getProperties().get("k3"));
    assertNull(snapshot.get().getProperties().get("k1"));
    assertNull(snapshot.get().getProperties().get("k2"));

    verify(propStore);
  }
}
