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
package org.apache.accumulo.core.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MonitorUtilTest {

  private ZooReader zr;
  private ClientContext context;

  @BeforeEach
  public void beforeEachTest() {
    zr = mock(ZooReader.class);
    context = mock(ClientContext.class);
    expect(context.getZooKeeperRoot()).andReturn("/root");
  }

  @AfterEach
  public void afterEachTest() {
    verify(zr, context);
  }

  @Test
  public void testNodeFound() throws Exception {
    expect(zr.getData("/root" + Constants.ZMONITOR_HTTP_ADDR))
        .andReturn("http://example.org/".getBytes(UTF_8));
    replay(zr, context);
    assertEquals("http://example.org/", MonitorUtil.getLocation(zr, context));
  }

  @Test
  public void testNoNodeFound() throws Exception {
    expect(zr.getData("/root" + Constants.ZMONITOR_HTTP_ADDR)).andThrow(new NoNodeException());
    replay(zr, context);
    assertNull(MonitorUtil.getLocation(zr, context));
  }
}
