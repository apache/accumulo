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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.tserver.TabletServerResourceManager.AssignmentWatcher;
import org.junit.jupiter.api.Test;

public class AssignmentWatcherTest {

  @Test
  public void testAssignmentWarning() {
    var e = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);
    var c = new ConfigurationCopy(Map.of(Property.TSERV_ASSIGNMENT_DURATION_WARNING.getKey(), "0"));
    ServerContext context = createMock(ServerContext.class);
    expect(context.getScheduledExecutor()).andReturn(e);
    expect(context.getConfiguration()).andReturn(c);

    ActiveAssignmentRunnable task = createMock(ActiveAssignmentRunnable.class);
    expect(task.getException()).andReturn(new Exception("Assignment warning happened"));

    var assignments = Map.of(new KeyExtent(TableId.of("1"), null, null),
        new RunnableStartedAt(task, System.currentTimeMillis()));
    var watcher = new AssignmentWatcher(context, assignments);

    replay(context, task);
    watcher.run();
    verify(context, task);
  }

}
