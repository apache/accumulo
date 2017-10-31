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
package org.apache.accumulo.tserver;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.tserver.TabletServerResourceManager.AssignmentWatcher;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class AssignmentWatcherTest {

  private Map<KeyExtent,RunnableStartedAt> assignments;
  private SimpleTimer timer;
  private AccumuloConfiguration conf;
  private AssignmentWatcher watcher;

  @Before
  public void setup() {
    assignments = new HashMap<>();
    timer = EasyMock.createMock(SimpleTimer.class);
    conf = EasyMock.createMock(AccumuloConfiguration.class);
    watcher = new AssignmentWatcher(conf, assignments, timer);
  }

  @Test
  public void testAssignmentWarning() {
    ActiveAssignmentRunnable task = EasyMock.createMock(ActiveAssignmentRunnable.class);
    RunnableStartedAt run = new RunnableStartedAt(task, System.currentTimeMillis());
    EasyMock.expect(conf.getTimeInMillis(Property.TSERV_ASSIGNMENT_DURATION_WARNING)).andReturn(0l);

    assignments.put(new KeyExtent(Table.ID.of("1"), null, null), run);

    EasyMock.expect(task.getException()).andReturn(new Exception("Assignment warning happened"));
    EasyMock.expect(timer.schedule(watcher, 5000l)).andReturn(null);

    EasyMock.replay(timer, conf, task);

    watcher.run();

    EasyMock.verify(timer, conf, task);
  }

}
