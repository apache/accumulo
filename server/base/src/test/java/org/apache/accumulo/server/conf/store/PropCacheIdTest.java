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
import static org.apache.accumulo.server.conf.store.PropCacheId.PROP_NODE_NAME;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropCacheIdTest {
  private static final Logger log = LoggerFactory.getLogger(PropCacheIdTest.class);

  private final String instanceId = UUID.randomUUID().toString();

  @Test
  public void systemType() {
    var name = PropCacheId.forSystem(instanceId);
    log.info("name: {}", name);
    assertTrue(name.getPath().endsWith(ZCONFIG + "/" + PROP_NODE_NAME));
    assertEquals(PropCacheId.IdType.SYSTEM, name.getIdType());
  }

  @Test
  public void systemTypeFromContext() {
    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).once();
    replay(context);

    var name = PropCacheId.forSystem(context);
    log.info("name: {}", name);
    assertTrue(name.getPath().endsWith(ZCONFIG + "/" + PROP_NODE_NAME));
    assertEquals(PropCacheId.IdType.SYSTEM, name.getIdType());

    verify(context);
  }

  @Test
  public void namespaceType() {
    var name = PropCacheId.forNamespace(instanceId, NamespaceId.of("a"));
    log.info("name: {}", name);
    assertTrue(name.getPath().endsWith(PROP_NODE_NAME) && name.getPath().contains(ZNAMESPACES));
    assertEquals(PropCacheId.IdType.NAMESPACE, name.getIdType());
    log.info("name: {}", name);
  }

  @Test
  public void namespaceTypeFromContext() {
    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).once();
    replay(context);

    var name = PropCacheId.forNamespace(context, NamespaceId.of("a"));
    assertTrue(name.getPath().endsWith(PROP_NODE_NAME) && name.getPath().contains(ZNAMESPACES));
    assertEquals(PropCacheId.IdType.NAMESPACE, name.getIdType());

    verify(context);
  }

  @Test
  public void tableType() {
    var name = PropCacheId.forTable(instanceId, TableId.of("a"));
    log.info("name: {}", name);
    assertTrue(name.getPath().endsWith(PROP_NODE_NAME) && name.getPath().contains(ZTABLES));
    assertEquals(PropCacheId.IdType.TABLE, name.getIdType());
    log.info("name: {}", name);
  }

  @Test
  public void sortTest() {
    Set<PropCacheId> nodes = new TreeSet<>();
    nodes.add(PropCacheId.forTable(instanceId, TableId.of("z1")));
    nodes.add(PropCacheId.forTable(instanceId, TableId.of("a1")));
    nodes.add(PropCacheId.forTable(instanceId, TableId.of("x1")));
    nodes.add(PropCacheId.forNamespace(instanceId, NamespaceId.of("z2")));
    nodes.add(PropCacheId.forNamespace(instanceId, NamespaceId.of("a2")));
    nodes.add(PropCacheId.forNamespace(instanceId, NamespaceId.of("x2")));

    nodes.add(PropCacheId.forSystem(instanceId));

    Iterator<PropCacheId> iterator = nodes.iterator();
    assertEquals(PropCacheId.IdType.SYSTEM, iterator.next().getIdType());
    assertEquals(PropCacheId.IdType.NAMESPACE, iterator.next().getIdType());
    assertEquals(PropCacheId.IdType.NAMESPACE, iterator.next().getIdType());
    assertEquals(PropCacheId.IdType.NAMESPACE, iterator.next().getIdType());
    assertEquals(PropCacheId.IdType.TABLE, iterator.next().getIdType());
    assertEquals(PropCacheId.IdType.TABLE, iterator.next().getIdType());
    assertEquals(PropCacheId.IdType.TABLE, iterator.next().getIdType());

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
  public void fromPath() {

    Optional<PropCacheId> t1 = PropCacheId
        .fromPath("/accumulo/3f9976c6-3bf1-41ab-9751-1b0a9be3551d/tables/t1/conf/encoded_props");
    assertTrue(t1.isPresent());
    assertTrue(t1.get().getNamespaceId().isEmpty());
    assertEquals(TableId.of("t1"), t1.get().getTableId().orElse(TableId.of("zzzz")));

    Optional<PropCacheId> n1 = PropCacheId.fromPath(
        "/accumulo/3f9976c6-3bf1-41ab-9751-1b0a9be3551d/namespaces/n1/conf/encoded_props");
    assertTrue(n1.isPresent());
    assertEquals(NamespaceId.of("n1"), n1.get().getNamespaceId().orElse(NamespaceId.of("zzzz")));
    assertTrue(n1.get().getTableId().isEmpty());

    Optional<PropCacheId> s1 =
        PropCacheId.fromPath("/accumulo/3f9976c6-3bf1-41ab-9751-1b0a9be3551d/config/encoded_props");
    assertTrue(s1.isPresent());
    assertTrue(s1.get().getNamespaceId().isEmpty());
    assertTrue(s1.get().getTableId().isEmpty());
  }
}
