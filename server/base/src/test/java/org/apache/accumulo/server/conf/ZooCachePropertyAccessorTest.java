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
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration.PropertyFilter;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.junit.Before;
import org.junit.Test;

public class ZooCachePropertyAccessorTest {
  private static final String PATH = "/root/path/to/props";
  private static final Property PROP = Property.INSTANCE_SECRET;
  private static final String KEY = PROP.getKey();
  private static final String FULL_PATH = PATH + "/" + KEY;
  private static final String VALUE = "value";
  private static final byte[] VALUE_BYTES = VALUE.getBytes(UTF_8);

  private ZooCache zc;
  private ZooCachePropertyAccessor a;

  @Before
  public void setUp() {
    zc = createMock(ZooCache.class);
    a = new ZooCachePropertyAccessor(zc);
  }

  @Test
  public void testGetter() {
    assertSame(zc, a.getZooCache());
  }

  @Test
  public void testGet_Valid() {
    expect(zc.get(FULL_PATH)).andReturn(VALUE_BYTES);
    replay(zc);
    assertEquals(VALUE, a.get(PROP, PATH, null));
  }

  @Test
  public void testGet_Parent() {
    AccumuloConfiguration parent = createMock(AccumuloConfiguration.class);
    expect(parent.get(PROP)).andReturn(VALUE);
    replay(parent);
    expect(zc.get(FULL_PATH)).andReturn(null);
    replay(zc);
    assertEquals(VALUE, a.get(PROP, PATH, parent));
  }

  @Test
  public void testGet_Parent_Null() {
    AccumuloConfiguration parent = createMock(AccumuloConfiguration.class);
    expect(parent.get(PROP)).andReturn(null);
    replay(parent);
    expect(zc.get(FULL_PATH)).andReturn(null);
    replay(zc);
    assertNull(a.get(PROP, PATH, parent));
  }

  @Test
  public void testGet_Null_NoParent() {
    expect(zc.get(FULL_PATH)).andReturn(null);
    replay(zc);
    assertNull(a.get(PROP, PATH, null));
  }

  @Test
  public void testGet_InvalidFormat() {
    Property badProp = Property.MASTER_CLIENTPORT;
    expect(zc.get(PATH + "/" + badProp.getKey())).andReturn(VALUE_BYTES);
    replay(zc);
    AccumuloConfiguration parent = createMock(AccumuloConfiguration.class);
    expect(parent.get(badProp)).andReturn("12345");
    replay(parent);
    assertEquals("12345", a.get(badProp, PATH, parent));
  }

  @Test
  public void testGetProperties() {
    Map<String,String> props = new java.util.HashMap<String,String>();
    AccumuloConfiguration parent = createMock(AccumuloConfiguration.class);
    PropertyFilter filter = createMock(PropertyFilter.class);
    parent.getProperties(props, filter);
    replay(parent);
    String child1 = "child1";
    String child2 = "child2";
    List<String> children = new java.util.ArrayList<String>();
    children.add(child1);
    children.add(child2);
    expect(zc.getChildren(PATH)).andReturn(children);
    expect(zc.get(PATH + "/" + child1)).andReturn(VALUE_BYTES);
    expect(zc.get(PATH + "/" + child2)).andReturn(null);
    replay(zc);
    expect(filter.accept(child1)).andReturn(true);
    expect(filter.accept(child2)).andReturn(true);
    replay(filter);

    a.getProperties(props, PATH, filter, parent, null);
    assertEquals(1, props.size());
    assertEquals(VALUE, props.get(child1));
    verify(parent);
  }

  @Test
  public void testGetProperties_NoChildren() {
    Map<String,String> props = new java.util.HashMap<String,String>();
    AccumuloConfiguration parent = createMock(AccumuloConfiguration.class);
    PropertyFilter filter = createMock(PropertyFilter.class);
    parent.getProperties(props, filter);
    replay(parent);
    expect(zc.getChildren(PATH)).andReturn(null);
    replay(zc);

    a.getProperties(props, PATH, filter, parent, null);
    assertEquals(0, props.size());
  }

  @Test
  public void testGetProperties_Filter() {
    Map<String,String> props = new java.util.HashMap<String,String>();
    AccumuloConfiguration parent = createMock(AccumuloConfiguration.class);
    PropertyFilter filter = createMock(PropertyFilter.class);
    parent.getProperties(props, filter);
    replay(parent);
    String child1 = "child1";
    List<String> children = new java.util.ArrayList<String>();
    children.add(child1);
    expect(zc.getChildren(PATH)).andReturn(children);
    replay(zc);
    expect(filter.accept(child1)).andReturn(false);
    replay(filter);

    a.getProperties(props, PATH, filter, parent, null);
    assertEquals(0, props.size());
  }

  @Test
  public void testGetProperties_ParentFilter() {
    Map<String,String> props = new java.util.HashMap<String,String>();
    AccumuloConfiguration parent = createMock(AccumuloConfiguration.class);
    PropertyFilter filter = createMock(PropertyFilter.class);
    PropertyFilter parentFilter = createMock(PropertyFilter.class);
    parent.getProperties(props, parentFilter);
    replay(parent);
    expect(zc.getChildren(PATH)).andReturn(null);
    replay(zc);

    a.getProperties(props, PATH, filter, parent, parentFilter);
    verify(parent);
  }

  @Test
  public void testInvalidateCache() {
    zc.clear();
    replay(zc);
    a.invalidateCache();
    verify(zc);
  }
}
