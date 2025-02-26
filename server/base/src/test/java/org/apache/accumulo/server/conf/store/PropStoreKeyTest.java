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
package org.apache.accumulo.server.conf.store;

import static org.apache.accumulo.core.Constants.ZCONFIG;
import static org.apache.accumulo.core.Constants.ZNAMESPACES;
import static org.apache.accumulo.core.Constants.ZROOT;
import static org.apache.accumulo.core.Constants.ZTABLES;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropStoreKeyTest {
  private static final Logger log = LoggerFactory.getLogger(PropStoreKeyTest.class);

  private final InstanceId instanceId = InstanceId.of(UUID.randomUUID());

  @Test
  public void systemType() {
    var propKey = SystemPropKey.of();
    log.info("name: {}", propKey);
    assertTrue(propKey.getPath().endsWith(ZCONFIG));
  }

  @Test
  public void systemTypeFromContext() {
    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).once();
    replay(context);

    var propKey = SystemPropKey.of();
    log.info("propKey: {}", propKey);
    assertTrue(propKey.getPath().endsWith(ZCONFIG));
    verify(context);
  }

  @Test
  public void namespaceType() {
    var propKey = NamespacePropKey.of(NamespaceId.of("a"));
    log.info("propKey: {}", propKey);
    assertTrue(propKey.getPath().endsWith(ZCONFIG) && propKey.getPath().contains(ZNAMESPACES));
    log.info("propKey: {}", propKey);
  }

  @Test
  public void namespaceTypeFromContext() {
    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).once();
    replay(context);

    var propKey = NamespacePropKey.of(NamespaceId.of("a"));
    assertTrue(propKey.getPath().endsWith(ZCONFIG) && propKey.getPath().contains(ZNAMESPACES));
    verify(context);
  }

  @Test
  public void tableType() {
    var propKey = TablePropKey.of(TableId.of("a"));
    log.info("propKey: {}", propKey);
    assertTrue(propKey.getPath().endsWith(ZCONFIG) && propKey.getPath().contains(ZTABLES));
    log.info("propKey: {}", propKey);
  }

  @Test
  public void fromPathTest() {
    var t1 = PropStoreKey.fromPath(ZTABLES + "/t1" + ZCONFIG);
    assertTrue(t1 instanceof TablePropKey);
    assertEquals(TableId.of("t1"), ((TablePropKey) t1).getId());

    var n1 = PropStoreKey.fromPath(ZNAMESPACES + "/n1" + ZCONFIG);
    assertTrue(n1 instanceof NamespacePropKey);
    assertEquals(NamespaceId.of("n1"), ((NamespacePropKey) n1).getId());

    var s1 = PropStoreKey.fromPath(ZCONFIG);
    assertFalse(s1 instanceof IdBasedPropStoreKey);
    assertTrue(s1 instanceof SystemPropKey);
  }

  @Test
  public void invalidKeysTest() {
    // too short
    assertNull(PropStoreKey.fromPath(ZROOT));

    // not a system config
    assertTrue(PropStoreKey.fromPath(ZCONFIG) instanceof SystemPropKey);
    assertNull(PropStoreKey.fromPath("/foo"));
    assertNull(PropStoreKey.fromPath(ZCONFIG + "/foo"));

    assertTrue(PropStoreKey.fromPath(ZTABLES + "/a" + ZCONFIG) instanceof TablePropKey);
    assertNull(PropStoreKey.fromPath(ZTABLES + ZCONFIG));
    assertNull(PropStoreKey.fromPath("/invalid/a" + ZCONFIG));
    assertNull(PropStoreKey.fromPath(ZTABLES + "/a" + ZCONFIG + "/foo"));

    assertTrue(PropStoreKey.fromPath(ZNAMESPACES + "/a" + ZCONFIG) instanceof NamespacePropKey);
    assertNull(PropStoreKey.fromPath(ZNAMESPACES + ZCONFIG));
    assertNull(PropStoreKey.fromPath("/invalid/a" + ZCONFIG));
    assertNull(PropStoreKey.fromPath(ZNAMESPACES + "/a" + ZCONFIG + "/foo"));
  }

  @Test
  public void getBasePathTest() {
    assertTrue(SystemPropKey.of().getPath().endsWith("/config"));
    assertTrue(NamespacePropKey.of(NamespaceId.of("123")).getPath().endsWith(ZCONFIG));
    assertTrue(TablePropKey.of(TableId.of("456")).getPath().endsWith(ZCONFIG));
  }
}
