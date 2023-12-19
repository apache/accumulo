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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

public class WalRemovalOrderTest {

  private ServerContext context;

  @BeforeEach
  private void createMocks() {
    context = createMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();
    replay(context);
  }

  @AfterEach
  private void verifyMocks() {
    verify(context);
  }

  private DfsLogger mockLogger(String filename) {
    var mockLogEntry = LogEntry.fromPath(filename + "+1234/11111111-1111-1111-1111-111111111111");
    return new DfsLogger(context, mockLogEntry);
  }

  private LinkedHashSet<DfsLogger> mockLoggers(String... logs) {
    LinkedHashSet<DfsLogger> logSet = new LinkedHashSet<>();

    for (String log : logs) {
      logSet.add(mockLogger(log));
    }

    return logSet;
  }

  private void runTest(LinkedHashSet<DfsLogger> closedLogs, Set<DfsLogger> inUseLogs,
      Set<DfsLogger> expected) {
    Set<DfsLogger> eligible = TabletServer.findOldestUnreferencedWals(List.copyOf(closedLogs),
        candidates -> candidates.removeAll(inUseLogs));
    assertEquals(expected, eligible);
  }

  @Test
  public void testWalRemoval() {
    runTest(mockLoggers("W1", "W2"), mockLoggers(), mockLoggers("W1", "W2"));
    runTest(mockLoggers("W1", "W2"), mockLoggers("W1"), mockLoggers());
    runTest(mockLoggers("W1", "W2"), mockLoggers("W2"), mockLoggers("W1"));
    runTest(mockLoggers("W1", "W2"), mockLoggers("W1", "W2"), mockLoggers());

    // below W5 represents an open log not in the closed set
    for (Set<DfsLogger> inUse : Sets.powerSet(mockLoggers("W1", "W2", "W3", "W4", "W5"))) {
      Set<DfsLogger> expected;
      if (inUse.contains(mockLogger("W1"))) {
        expected = Collections.emptySet();
      } else if (inUse.contains(mockLogger("W2"))) {
        expected = mockLoggers("W1");
      } else if (inUse.contains(mockLogger("W3"))) {
        expected = mockLoggers("W1", "W2");
      } else if (inUse.contains(mockLogger("W4"))) {
        expected = mockLoggers("W1", "W2", "W3");
      } else {
        expected = mockLoggers("W1", "W2", "W3", "W4");
      }

      runTest(mockLoggers("W1", "W2", "W3", "W4"), inUse, expected);
    }
  }
}
