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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.tserver.TabletServerResourceManager.AssignmentWatcher;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AssignmentWatcherTest {

  private Map<KeyExtent,RunnableStartedAt> assignments;
  private ServerContext context;
  private AccumuloConfiguration conf;
  private AssignmentWatcher watcher;

  @BeforeEach
  public void setup() {
    assignments = new HashMap<>();
    context = EasyMock.createMock(ServerContext.class);
    conf = EasyMock.createNiceMock(AccumuloConfiguration.class);
    watcher = new AssignmentWatcher(conf, context, assignments);
  }

  @Test
  public void testAssignmentWarning() {
    ActiveAssignmentRunnable task = EasyMock.createMock(ActiveAssignmentRunnable.class);
    RunnableStartedAt run = new RunnableStartedAt(task, System.currentTimeMillis());
    EasyMock.expect(context.getConfiguration()).andReturn(conf).anyTimes();
    EasyMock.expect(conf.getCount(EasyMock.isA(Property.class))).andReturn(1).anyTimes();
    EasyMock.expect(conf.getTimeInMillis(EasyMock.isA(Property.class))).andReturn(0L).anyTimes();
    EasyMock.expect(context.getScheduledExecutor())
        .andReturn((ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1)).anyTimes();
    assignments.put(new KeyExtent(TableId.of("1"), null, null), run);

    EasyMock.expect(task.getException()).andReturn(new Exception("Assignment warning happened"));
    EasyMock.replay(context, task);
    watcher.run();
    EasyMock.verify(context, task);
  }

}
