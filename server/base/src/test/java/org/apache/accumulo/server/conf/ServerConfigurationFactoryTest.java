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
package org.apache.accumulo.server.conf;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ServerConfigurationFactoryTest {
  private static final String ZK_HOST = "localhost";
  private static final int ZK_TIMEOUT = 120_000;
  private static final InstanceId IID = InstanceId.of(UUID.randomUUID());
  private static final TableId TID = TableId.of("TABLE");
  private static final NamespaceId NSID = NamespaceId.of("NAMESPACE");
  private static final SiteConfiguration siteConfig = SiteConfiguration.empty().build();

  private PropStore propStore;
  private ServerContext context;
  private SystemConfiguration sysConfig;
  private ServerConfigurationFactory scf;

  @BeforeEach
  public void setUp() {
    propStore = createMock(ZooPropStore.class);
    expect(propStore.get(eq(SystemPropKey.of(IID)))).andReturn(new VersionedProperties(Map.of()))
        .anyTimes();
    expect(propStore.get(eq(TablePropKey.of(IID, TID))))
        .andReturn(new VersionedProperties(Map.of())).anyTimes();
    expect(propStore.get(eq(NamespacePropKey.of(IID, NSID))))
        .andReturn(new VersionedProperties(Map.of())).anyTimes();

    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    sysConfig = createMock(SystemConfiguration.class);
    sysConfig.getProperties(anyObject(), anyObject());
    expectLastCall().anyTimes();

    context = createMock(ServerContext.class);
    expect(context.getZooKeeperRoot()).andReturn("/accumulo/" + IID).anyTimes();
    expect(context.getInstanceID()).andReturn(IID).anyTimes();
    expect(context.getZooKeepers()).andReturn(ZK_HOST).anyTimes();
    expect(context.getZooKeepersSessionTimeOut()).andReturn(ZK_TIMEOUT).anyTimes();
    expect(context.getSiteConfiguration()).andReturn(siteConfig).anyTimes();
    expect(context.tableNodeExists(TID)).andReturn(true).anyTimes();
    expect(context.getPropStore()).andReturn(propStore).anyTimes();
    expect(context.getConfiguration()).andReturn(sysConfig).anyTimes();
    scf = new ServerConfigurationFactory(context, siteConfig) {
      @Override
      public NamespaceConfiguration getNamespaceConfigurationForTable(TableId tableId) {
        if (tableId.equals(TID)) {
          return getNamespaceConfiguration(NSID);
        }
        throw new UnsupportedOperationException();
      }
    };

    replay(propStore, context, sysConfig);
  }

  @AfterEach
  public void verifyMocks() {
    verify(propStore, context, sysConfig);
  }

  @Test
  public void testGetters() {
    assertSame(DefaultConfiguration.getInstance(), scf.getDefaultConfiguration());
    assertSame(siteConfig, scf.getSiteConfiguration());
    assertNotNull(scf.getSystemConfiguration());
  }

  @Test
  public void testGetNamespaceConfiguration() {
    NamespaceConfiguration namespaceConfigSingleton = scf.getNamespaceConfiguration(NSID);
    assertEquals(NSID, namespaceConfigSingleton.getNamespaceId());
    assertSame(namespaceConfigSingleton, scf.getNamespaceConfiguration(NSID));
  }

  @Test
  public void testGetTableConfiguration() {
    TableConfiguration tableConfigSingleton = scf.getTableConfiguration(TID);
    assertEquals(TID, tableConfigSingleton.getTableId());
    assertSame(tableConfigSingleton, scf.getTableConfiguration(TID));
  }

}
