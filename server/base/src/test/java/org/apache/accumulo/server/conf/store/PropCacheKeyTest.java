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
    var propKey = SystemPropKey.of(instanceId);
    log.info("name: {}", propKey);
    assertTrue(propKey.getPath().endsWith(ZCONFIG + "/" + PROP_NODE_NAME));
  }

  @Test
  public void systemTypeFromContext() {
    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).once();
    replay(context);

    var propKey = SystemPropKey.of(context);
    log.info("propKey: {}", propKey);
    assertTrue(propKey.getPath().endsWith(ZCONFIG + "/" + PROP_NODE_NAME));
    verify(context);
  }

  @Test
  public void namespaceType() {
    var propKey = NamespacePropKey.of(instanceId, NamespaceId.of("a"));
    log.info("propKey: {}", propKey);
    assertTrue(
        propKey.getPath().endsWith(PROP_NODE_NAME) && propKey.getPath().contains(ZNAMESPACES));
    log.info("propKey: {}", propKey);
  }

  @Test
  public void namespaceTypeFromContext() {
    ServerContext context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(instanceId).once();
    replay(context);

    var propKey = NamespacePropKey.of(context, NamespaceId.of("a"));
    assertTrue(
        propKey.getPath().endsWith(PROP_NODE_NAME) && propKey.getPath().contains(ZNAMESPACES));
    verify(context);
  }

  @Test
  public void tableType() {
    var propKey = TablePropKey.of(instanceId, TableId.of("a"));
    log.info("propKey: {}", propKey);
    assertTrue(propKey.getPath().endsWith(PROP_NODE_NAME) && propKey.getPath().contains(ZTABLES));
    log.info("propKey: {}", propKey);
  }

  @Test
  public void fromPathTest() {

    PropCacheKey t1 = PropCacheKey
        .fromPath("/accumulo/3f9976c6-3bf1-41ab-9751-1b0a9be3551d/tables/t1/conf/encoded_props");
    assertNotNull(t1);
    assertEquals(TableId.of("t1"), t1.getId());

    PropCacheKey n1 = PropCacheKey.fromPath(
        "/accumulo/3f9976c6-3bf1-41ab-9751-1b0a9be3551d/namespaces/n1/conf/encoded_props");
    assertNotNull(n1);
    assertEquals(NamespaceId.of("n1"), n1.getId());
    assertNotNull(n1.getId());

    PropCacheKey s1 = PropCacheKey
        .fromPath("/accumulo/3f9976c6-3bf1-41ab-9751-1b0a9be3551d/config/encoded_props");
    assertNotNull(s1);
    assertNull(s1.getId());
  }

  @Test
  public void getBasePathTest() {
    assertTrue(SystemPropKey.of(instanceId).getBasePath().endsWith("/config"));
    assertTrue(
        NamespacePropKey.of(instanceId, NamespaceId.of("123")).getBasePath().endsWith("/conf"));
    assertTrue(TablePropKey.of(instanceId, TableId.of("456")).getBasePath().endsWith("/conf"));
  }
}
