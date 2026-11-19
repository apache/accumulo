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
package org.apache.accumulo.core.clientImpl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Properties;

import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.metadata.RootTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TabletLocatorTest {

  private static final InstanceId INSTANCE_ID = InstanceId.of("instance");

  @AfterEach
  public void clearLocators() {
    TabletLocator.clearLocators();
  }

  @Test
  public void testExpirationIsPartOfLocatorIdentity() {
    TabletLocator noExpiration = TabletLocator.getLocator(createMockedContext("0"), RootTable.ID);
    TabletLocator tenMinutes = TabletLocator.getLocator(createMockedContext("10m"), RootTable.ID);

    assertSame(noExpiration, TabletLocator.getLocator(createMockedContext("0"), RootTable.ID));
    assertNotSame(noExpiration, tenMinutes);
    assertSame(tenMinutes, TabletLocator.getLocator(createMockedContext("600s"), RootTable.ID));
  }

  private ClientContext createMockedContext(String expiration) {
    Properties properties = new Properties();
    properties.setProperty(ClientProperty.CLIENT_EXTENT_CACHE_EXPIRATION.getKey(), expiration);

    ClientContext context = createMock(ClientContext.class);
    expect(context.getTableState(RootTable.ID)).andReturn(TableState.ONLINE).anyTimes();
    expect(context.getProperties()).andReturn(properties).anyTimes();
    expect(context.getInstanceID()).andReturn(INSTANCE_ID).anyTimes();
    expect(context.getTServerLockChecker()).andReturn(createMock(ZookeeperLockChecker.class))
        .anyTimes();
    replay(context);
    return context;
  }
}
