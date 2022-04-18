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
package org.apache.accumulo.server.conf.store;

import static org.apache.accumulo.core.Constants.ZCONFIG;
import static org.apache.accumulo.core.Constants.ZNAMESPACES;
import static org.apache.accumulo.core.Constants.ZTABLES;
import static org.apache.accumulo.server.conf.store.PropCacheKey.PROP_NODE_NAME;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropCacheKeyTest {
  private static final Logger log = LoggerFactory.getLogger(PropCacheKeyTest.class);

  private final InstanceId instanceId = InstanceId.of(UUID.randomUUID());

  @Test
  public void systemType() {
    var propKey = PropCacheKey.forSystem(instanceId);
    log.info("name: {}", propKey);
    assertTrue(propKey.getPath().endsWith(ZCONFIG + "/" + PROP_NODE_NAME));
    assertEquals(PropCacheKey.IdType.SYSTEM, propKey.getIdType());
  }

  @Test
  public void systemTypeFromContext() {
    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).once();
    replay(context);

    var propKey = PropCacheKey.forSystem(context);
    log.info("propKey: {}", propKey);
    assertTrue(propKey.getPath().endsWith(ZCONFIG + "/" + PROP_NODE_NAME));
    assertEquals(PropCacheKey.IdType.SYSTEM, propKey.getIdType());

    verify(context);
  }

  @Test
  public void namespaceType() {
    var propKey = PropCacheKey.forNamespace(instanceId, NamespaceId.of("a"));
    log.info("propKey: {}", propKey);
    assertTrue(
        propKey.getPath().endsWith(PROP_NODE_NAME) && propKey.getPath().contains(ZNAMESPACES));
    assertEquals(PropCacheKey.IdType.NAMESPACE, propKey.getIdType());
    log.info("propKey: {}", propKey);
  }

  @Test
  public void namespaceTypeFromContext() {
    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).once();
    replay(context);

    var propKey = PropCacheKey.forNamespace(context, NamespaceId.of("a"));
    assertTrue(
        propKey.getPath().endsWith(PROP_NODE_NAME) && propKey.getPath().contains(ZNAMESPACES));
    assertEquals(PropCacheKey.IdType.NAMESPACE, propKey.getIdType());

    verify(context);
  }

  @Test
  public void tableType() {
    var propKey = PropCacheKey.forTable(instanceId, TableId.of("a"));
    log.info("propKey: {}", propKey);
    assertTrue(propKey.getPath().endsWith(PROP_NODE_NAME) && propKey.getPath().contains(ZTABLES));
    assertEquals(PropCacheKey.IdType.TABLE, propKey.getIdType());
    log.info("propKey: {}", propKey);
  }

  @Test
  public void sortTest() {
    Set<PropCacheKey> nodes = new TreeSet<>();
    nodes.add(PropCacheKey.forTable(instanceId, TableId.of("z1")));
    nodes.add(PropCacheKey.forTable(instanceId, TableId.of("a1")));
    nodes.add(PropCacheKey.forTable(instanceId, TableId.of("x1")));
    nodes.add(PropCacheKey.forNamespace(instanceId, NamespaceId.of("z2")));
    nodes.add(PropCacheKey.forNamespace(instanceId, NamespaceId.of("a2")));
    nodes.add(PropCacheKey.forNamespace(instanceId, NamespaceId.of("x2")));

    nodes.add(PropCacheKey.forSystem(instanceId));

    Iterator<PropCacheKey> iterator = nodes.iterator();
    assertEquals(PropCacheKey.IdType.SYSTEM, iterator.next().getIdType());
    assertEquals(PropCacheKey.IdType.NAMESPACE, iterator.next().getIdType());
    assertEquals(PropCacheKey.IdType.NAMESPACE, iterator.next().getIdType());
    assertEquals(PropCacheKey.IdType.NAMESPACE, iterator.next().getIdType());
    assertEquals(PropCacheKey.IdType.TABLE, iterator.next().getIdType());
    assertEquals(PropCacheKey.IdType.TABLE, iterator.next().getIdType());
    assertEquals(PropCacheKey.IdType.TABLE, iterator.next().getIdType());

    // rewind.
    iterator = nodes.iterator();
    iterator.next(); // skip system
    assertTrue(iterator.next().getPath().contains("a2"));
    assertTrue(iterator.next().getPath().contains("x2"));
    assertTrue(iterator.next().getPath().contains("z2"));

    assertTrue(iterator.next().getPath().contains("a1"));
    assertTrue(iterator.next().getPath().contains("x1"));
    assertTrue(iterator.next().getPath().contains("z1"));

    log.info("Sorted: {}", nodes);
  }

  @Test
  public void fromPathTest() {

    PropCacheKey t1 = PropCacheKey
        .fromPath("/accumulo/3f9976c6-3bf1-41ab-9751-1b0a9be3551d/tables/t1/conf/encoded_props");
    assertNotNull(t1);
    assertNull(t1.getNamespaceId());
    assertEquals(TableId.of("t1"), t1.getTableId());

    PropCacheKey n1 = PropCacheKey.fromPath(
        "/accumulo/3f9976c6-3bf1-41ab-9751-1b0a9be3551d/namespaces/n1/conf/encoded_props");
    assertNotNull(n1);
    assertEquals(NamespaceId.of("n1"), n1.getNamespaceId());
    assertNull(n1.getTableId());

    PropCacheKey s1 = PropCacheKey
        .fromPath("/accumulo/3f9976c6-3bf1-41ab-9751-1b0a9be3551d/config/encoded_props");
    assertNotNull(s1);
    assertNull(s1.getNamespaceId());
    assertNull(s1.getTableId());
  }

  @Test
  public void getBasePathTest() {
    assertTrue(PropCacheKey.forSystem(instanceId).getBasePath().endsWith("/config"));
    assertTrue(PropCacheKey.forNamespace(instanceId, NamespaceId.of("123")).getBasePath()
        .endsWith("/conf"));
    assertTrue(
        PropCacheKey.forTable(instanceId, TableId.of("456")).getBasePath().endsWith("/conf"));
  }
}
