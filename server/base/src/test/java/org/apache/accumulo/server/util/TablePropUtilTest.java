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
package org.apache.accumulo.server.util;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TablePropUtilTest {

  // private static ServerContext context;
  // private static final TableId TID = TableId.of("4");
  // private static final NamespaceId NID = NamespaceId.of("5");
  //
  // private static PropStore propStore;
  // private static InstanceId instanceId;

  // @BeforeAll
  // public static void setup() {
  // instanceId = InstanceId.of(UUID.randomUUID());
  // context = createMock(ServerContext.class);
  // expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
  // expect(context.tablePropUtil()).andReturn(new TablePropUtil(context)).anyTimes();
  // propStore = createMock(ZooPropStore.class);
  // expect(context.getPropStore()).andReturn(propStore).anyTimes();
  // replay(context);
  // }

  @Test
  void testValidProperties() {
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());
    ServerContext context = createMock(ServerContext.class);
    PropStore propStore = createMock(ZooPropStore.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    expect(context.tablePropUtil()).andReturn(new TablePropUtil(context)).anyTimes();
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    replay(context);
    TableId TID = TableId.of("4");
    context.tablePropUtil().setProperties(TID,
        Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));
    verify(context);
  }

  // @Test
  // void testIllegalStateExceptionProperties() {
  // context.tablePropUtil().setProperties(TID,
  // Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));
  // }

  @Test
  void testIllegalArgumentProperties() {
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());
    ServerContext context = createMock(ServerContext.class);
    PropStore propStore = createMock(ZooPropStore.class);
    expect(context.getInstanceID()).andReturn(instanceId).anyTimes();
    expect(context.tablePropUtil()).andReturn(new TablePropUtil(context)).anyTimes();
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    replay(context);
    TableId TID = TableId.of("4");

    IllegalArgumentException thrown =
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
          context.tablePropUtil().setProperties(TID, Map.of("NOT_A_TABLE_SAMPLER_OPTS", "4s"));
        }, "Expected IllegalArgumentException");
    Assertions.assertEquals(
        "Invalid property for table: 4 name: NOT_A_TABLE_SAMPLER_OPTS, value: 4s",
        thrown.getMessage());
    verify(context);
  }

  @Test
  void testRemoveProperties() {
    TableId TID = TableId.of("4");
    ServerContext context = createMock(ServerContext.class);
    PropStore propStore = createMock(ZooPropStore.class);
    expect(context.tablePropUtil()).andReturn(new TablePropUtil(context)).anyTimes();
    expect(context.getPropStore()).andThrow(new IllegalStateException());
    replay(context);

    Assertions.assertThrows(IllegalStateException.class, () -> {
      context.tablePropUtil().removeProperties(TID, List.of("NOT_A_PROPERTY"));
    }, "Expected IllegalStateException");
    verify(context);
  }
}
