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
package org.apache.accumulo.core.util;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertNull;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.Test;

public class MonitorUtilTest {

  @Test
  public void testNoNodeFound() throws Exception {

    ZooReader zr = mock(ZooReader.class);
    ClientContext context = mock(ClientContext.class);
    expect(context.getZooKeeperRoot()).andReturn("/root/");
    expect(zr.getData("/root/" + Constants.ZMONITOR_HTTP_ADDR, null))
        .andThrow(new NoNodeException());

    replay(zr, context);
    assertNull(MonitorUtil.getLocation(zr, context));
  }
}
