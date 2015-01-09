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
package org.apache.accumulo.server.conf;

import static com.google.common.base.Charsets.UTF_8;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration.AllFilter;
import org.apache.accumulo.core.conf.AccumuloConfiguration.PropertyFilter;
import org.apache.accumulo.core.conf.ConfigurationObserver;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.junit.Before;
import org.junit.Test;

public class NamespaceConfigurationTest {
  private static final String NSID = "namespace";
  private static final String ZOOKEEPERS = "localhost";
  private static final int ZK_SESSION_TIMEOUT = 120000;

  private String iid;
  private Instance instance;
  private AccumuloConfiguration parent;
  private ZooCacheFactory zcf;
  private ZooCache zc;
  private NamespaceConfiguration c;

  @Before
  public void setUp() {
    iid = UUID.randomUUID().toString();
    instance = createMock(Instance.class);
    parent = createMock(AccumuloConfiguration.class);
    c = new NamespaceConfiguration(NSID, instance, parent);
    zcf = createMock(ZooCacheFactory.class);
    c.setZooCacheFactory(zcf);

    expect(instance.getInstanceID()).andReturn(iid);
    expectLastCall().anyTimes();
    expect(instance.getZooKeepers()).andReturn(ZOOKEEPERS);
    expect(instance.getZooKeepersSessionTimeOut()).andReturn(ZK_SESSION_TIMEOUT);
    replay(instance);
    zc = createMock(ZooCache.class);
    expect(zcf.getZooCache(eq(ZOOKEEPERS), eq(ZK_SESSION_TIMEOUT), anyObject(NamespaceConfWatcher.class))).andReturn(zc);
    replay(zcf);
  }

  @Test
  public void testGetters() {
    assertEquals(NSID, c.getNamespaceId());
    assertEquals(parent, c.getParentConfiguration());
  }

  @Test
  public void testGet_InZK() {
    Property p = Property.INSTANCE_SECRET;
    expect(zc.get(ZooUtil.getRoot(iid) + Constants.ZNAMESPACES + "/" + NSID + Constants.ZNAMESPACE_CONF + "/" + p.getKey()))
        .andReturn("sekrit".getBytes(UTF_8));
    replay(zc);
    assertEquals("sekrit", c.get(Property.INSTANCE_SECRET));
  }

  @Test
  public void testGet_InParent() {
    Property p = Property.INSTANCE_SECRET;
    expect(zc.get(ZooUtil.getRoot(iid) + Constants.ZNAMESPACES + "/" + NSID + Constants.ZNAMESPACE_CONF + "/" + p.getKey())).andReturn(null);
    replay(zc);
    expect(parent.get(p)).andReturn("sekrit");
    replay(parent);
    assertEquals("sekrit", c.get(Property.INSTANCE_SECRET));
  }

  @Test
  public void testGet_SkipParentIfAccumuloNS() {
    c = new NamespaceConfiguration(Namespaces.ACCUMULO_NAMESPACE_ID, instance, parent);
    c.setZooCacheFactory(zcf);
    Property p = Property.INSTANCE_SECRET;
    expect(zc.get(ZooUtil.getRoot(iid) + Constants.ZNAMESPACES + "/" + Namespaces.ACCUMULO_NAMESPACE_ID + Constants.ZNAMESPACE_CONF + "/" + p.getKey()))
        .andReturn(null);
    replay(zc);
    assertNull(c.get(Property.INSTANCE_SECRET));
  }

  @Test
  public void testGetProperties() {
    PropertyFilter filter = new AllFilter();
    Map<String,String> props = new java.util.HashMap<String,String>();
    parent.getProperties(props, filter);
    replay(parent);
    List<String> children = new java.util.ArrayList<String>();
    children.add("foo");
    children.add("ding");
    expect(zc.getChildren(ZooUtil.getRoot(iid) + Constants.ZNAMESPACES + "/" + NSID + Constants.ZNAMESPACE_CONF)).andReturn(children);
    expect(zc.get(ZooUtil.getRoot(iid) + Constants.ZNAMESPACES + "/" + NSID + Constants.ZNAMESPACE_CONF + "/" + "foo")).andReturn("bar".getBytes(UTF_8));
    expect(zc.get(ZooUtil.getRoot(iid) + Constants.ZNAMESPACES + "/" + NSID + Constants.ZNAMESPACE_CONF + "/" + "ding")).andReturn("dong".getBytes(UTF_8));
    replay(zc);
    c.getProperties(props, filter);
    assertEquals(2, props.size());
    assertEquals("bar", props.get("foo"));
    assertEquals("dong", props.get("ding"));
  }

  @Test
  public void testObserver() {
    ConfigurationObserver o = createMock(ConfigurationObserver.class);
    c.addObserver(o);
    Collection<ConfigurationObserver> os = c.getObservers();
    assertEquals(1, os.size());
    assertTrue(os.contains(o));
    c.removeObserver(o);
    os = c.getObservers();
    assertEquals(0, os.size());
  }

  @Test
  public void testInvalidateCache() {
    // need to do a get so the accessor is created
    Property p = Property.INSTANCE_SECRET;
    expect(zc.get(ZooUtil.getRoot(iid) + Constants.ZNAMESPACES + "/" + NSID + Constants.ZNAMESPACE_CONF + "/" + p.getKey()))
        .andReturn("sekrit".getBytes(UTF_8));
    zc.clear();
    replay(zc);
    c.get(Property.INSTANCE_SECRET);
    c.invalidateCache();
    verify(zc);
  }
}
