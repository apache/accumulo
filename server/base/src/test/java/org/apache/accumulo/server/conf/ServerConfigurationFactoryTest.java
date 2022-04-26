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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.server.MockServerContext;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ServerConfigurationFactoryTest {
  private static final String ZK_HOST = "localhost";
  private static final int ZK_TIMEOUT = 120000;
  private static final InstanceId IID = InstanceId.of("iid");

  // use the same mock ZooCacheFactory and ZooCache for all tests
  private static final SiteConfiguration siteConfig = SiteConfiguration.auto();
  private PropStore propStore;

  @BeforeAll
  public static void setUpClass() {}

  private ServerContext context;
  private ServerConfigurationFactory scf;

  @BeforeEach
  public void setUp() {
    propStore = createMock(ZooPropStore.class);
    // expect(propStore.readFixed()).andReturn(Map.of()).anyTimes();

    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    context = MockServerContext.getWithZK(IID, ZK_HOST, ZK_TIMEOUT);
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();
  }

  private void ready() {
    replay(context, propStore);
    scf = new ServerConfigurationFactory(context, siteConfig);
  }

  @Test
  public void testGetDefaultConfiguration() {
    ready();
    DefaultConfiguration c = scf.getDefaultConfiguration();
    assertNotNull(c);
  }

  @Test
  public void testGetSiteConfiguration() {
    ready();
    var c = scf.getSiteConfiguration();
    assertNotNull(c);
  }

  @Test
  public void testGetConfiguration() {
    expect(propStore.get(eq(PropCacheKey.forSystem(IID))))
        .andReturn(new VersionedProperties(Map.of())).anyTimes();
    ready();
    AccumuloConfiguration c = scf.getSystemConfiguration();
    assertNotNull(c);
  }

  private static final NamespaceId NSID = NamespaceId.of("NAMESPACE");

  @Test
  public void testGetNamespaceConfiguration() {
    expect(propStore.get(eq(PropCacheKey.forSystem(IID))))
        .andReturn(new VersionedProperties(Map.of())).anyTimes();
    ready();
    reset(context);
    PropStore propStore = createMock(ZooPropStore.class);
    expect(propStore.get(eq(PropCacheKey.forSystem(IID))))
        .andReturn(new VersionedProperties(Map.of())).anyTimes();
    expect(propStore.get(eq(PropCacheKey.forNamespace(IID, NSID))))
        .andReturn(new VersionedProperties(Map.of())).anyTimes();

    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    expect(context.getInstanceID()).andReturn(IID).anyTimes();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();

    replay(propStore, context);
    NamespaceConfiguration c = scf.getNamespaceConfiguration(NSID);
    assertEquals(NSID, c.getNamespaceId());
    assertSame(c, scf.getNamespaceConfiguration(NSID));
  }

}
