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
package org.apache.accumulo.tserver;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.spi.ondemand.DefaultOnDemandTabletUnloader;
import org.apache.accumulo.core.spi.ondemand.OnDemandTabletUnloader.UnloaderParams;
import org.apache.accumulo.core.tabletserver.UnloaderParamsImpl;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class DefaultOnDemandTabletUnloaderTest {

  @Test
  public void evaluationTest() {

    long currentTime = System.nanoTime();
    String inactivityTimeSeconds = "60";
    long inactivityTime = MINUTES.toNanos(1);

    TableId tid = TableId.of("42");
    ServerContext context = createMock(ServerContext.class);
    TableConfiguration tconf = createMock(TableConfiguration.class);
    expect(context.getCaches()).andReturn(Caches.getInstance()).anyTimes();
    expect(context.getConfiguration()).andReturn(tconf);
    expect(context.getTableConfiguration(tid)).andReturn(tconf);
    expect(tconf.get(DefaultOnDemandTabletUnloader.INACTIVITY_THRESHOLD))
        .andReturn(inactivityTimeSeconds);
    expect(tconf.newDeriver(anyObject())).andReturn(Map::of).anyTimes();
    Map<KeyExtent,Long> online = new HashMap<>();
    // add an extent whose last access time is less than the currentTime - inactivityTime
    final KeyExtent activeExtent = new KeyExtent(tid, new Text("m"), new Text("a"));
    online.put(activeExtent, currentTime - inactivityTime - 10);
    // add an extent whose last access time is greater than the currentTime - inactivityTime
    final KeyExtent inactiveExtent = new KeyExtent(tid, new Text("z"), new Text("m"));
    online.put(inactiveExtent, currentTime - inactivityTime + 10);
    Set<KeyExtent> onDemandTabletsToUnload = new HashSet<>();

    replay(context, tconf);
    ServiceEnvironmentImpl env = new ServiceEnvironmentImpl(context);
    UnloaderParams params = new UnloaderParamsImpl(tid, env, online, onDemandTabletsToUnload);

    DefaultOnDemandTabletUnloader unloader = new DefaultOnDemandTabletUnloader() {
      @Override
      protected long getCurrentTime() {
        return currentTime;
      }
    };
    unloader.evaluate(params);

    verify(context, tconf);
    assertEquals(1, onDemandTabletsToUnload.size());
    assertTrue(onDemandTabletsToUnload.contains(activeExtent));
  }
}
