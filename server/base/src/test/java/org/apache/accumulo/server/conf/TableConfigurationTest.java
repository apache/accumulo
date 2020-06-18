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
package org.apache.accumulo.server.conf;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.junit.Before;
import org.junit.Test;

public class TableConfigurationTest {
  private static final TableId TID = TableId.of("table");
  private static final String ZOOKEEPERS = "localhost";
  private static final int ZK_SESSION_TIMEOUT = 120000;

  private String iid;
  private ServerContext context;
  private NamespaceConfiguration parent;
  private ZooCacheFactory zcf;
  private ZooCache zc;
  private TableConfiguration c;

  @Before
  public void setUp() {
    iid = UUID.randomUUID().toString();
    context = createMock(ServerContext.class);

    expect(context.getProperties()).andReturn(new Properties()).anyTimes();
    expect(context.getInstanceID()).andReturn(iid).anyTimes();
    expect(context.getZooKeeperRoot()).andReturn("/accumulo/" + iid).anyTimes();
    expect(context.getZooKeepers()).andReturn(ZOOKEEPERS).anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(ZK_SESSION_TIMEOUT).anyTimes();
    replay(context);

    parent = createMock(NamespaceConfiguration.class);
    c = new TableConfiguration(context, TID, parent);
    zcf = createMock(ZooCacheFactory.class);
    c.setZooCacheFactory(zcf);

    zc = createMock(ZooCache.class);
    expect(zcf.getZooCache(eq(ZOOKEEPERS), eq(ZK_SESSION_TIMEOUT))).andReturn(zc);
    replay(zcf);
  }

  @Test
  public void testGetters() {
    assertEquals(TID, c.getTableId());
    assertEquals(parent, c.getParentConfiguration());
  }

  @Test
  public void testGet_InZK() {
    Property p = Property.INSTANCE_SECRET;
    expect(zc.get(ZooUtil.getRoot(iid) + Constants.ZTABLES + "/" + TID + Constants.ZTABLE_CONF + "/"
        + p.getKey())).andReturn("sekrit".getBytes(UTF_8));
    replay(zc);
    assertEquals("sekrit", c.get(Property.INSTANCE_SECRET));
  }

  @Test
  public void testGet_InParent() {
    Property p = Property.INSTANCE_SECRET;
    expect(zc.get(ZooUtil.getRoot(iid) + Constants.ZTABLES + "/" + TID + Constants.ZTABLE_CONF + "/"
        + p.getKey())).andReturn(null);
    replay(zc);
    expect(parent.get(p)).andReturn("sekrit");
    replay(parent);
    assertEquals("sekrit", c.get(Property.INSTANCE_SECRET));
  }

  @Test
  public void testGetProperties() {
    Predicate<String> all = x -> true;
    Map<String,String> props = new java.util.HashMap<>();
    parent.getProperties(props, all);
    replay(parent);
    List<String> children = new java.util.ArrayList<>();
    children.add("foo");
    children.add("ding");
    expect(zc
        .getChildren(ZooUtil.getRoot(iid) + Constants.ZTABLES + "/" + TID + Constants.ZTABLE_CONF))
            .andReturn(children);
    expect(zc.get(
        ZooUtil.getRoot(iid) + Constants.ZTABLES + "/" + TID + Constants.ZTABLE_CONF + "/" + "foo"))
            .andReturn("bar".getBytes(UTF_8));
    expect(zc.get(ZooUtil.getRoot(iid) + Constants.ZTABLES + "/" + TID + Constants.ZTABLE_CONF + "/"
        + "ding")).andReturn("dong".getBytes(UTF_8));
    replay(zc);
    c.getProperties(props, all);
    assertEquals(2, props.size());
    assertEquals("bar", props.get("foo"));
    assertEquals("dong", props.get("ding"));
  }

  @Test
  public void testInvalidateCache() {
    // need to do a get so the accessor is created
    Property p = Property.INSTANCE_SECRET;
    expect(zc.get(ZooUtil.getRoot(iid) + Constants.ZTABLES + "/" + TID + Constants.ZTABLE_CONF + "/"
        + p.getKey())).andReturn("sekrit".getBytes(UTF_8));
    zc.clear();
    replay(zc);
    c.get(Property.INSTANCE_SECRET);
    c.invalidateCache();
    verify(zc);
  }
}
