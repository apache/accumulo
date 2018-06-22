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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.tserver.TabletServer.ReferencedRemover;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.log.DfsLogger.ServerResources;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TabletServerTest {

  static DfsLogger logger(String log) {
    ServerResources conf = new ServerResources() {
      @Override
      public AccumuloConfiguration getConfiguration() {
        return DefaultConfiguration.getInstance();
      }

      @Override
      public VolumeManager getFileSystem() {
        throw new UnsupportedOperationException();
      }
    };

    try {
      return new DfsLogger(conf, log, null);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static LinkedHashSet<DfsLogger> logs(String... logs) {
    LinkedHashSet<DfsLogger> logSet = new LinkedHashSet<>();

    for (String log : logs) {
      logSet.add(logger(log));
    }

    return logSet;
  }

  private static class TestRefRemover implements ReferencedRemover {
    Set<DfsLogger> inUseLogs;

    TestRefRemover(Set<DfsLogger> inUseLogs) {
      this.inUseLogs = inUseLogs;
    }

    @Override
    public void removeInUse(Set<DfsLogger> candidates) {
      candidates.removeAll(inUseLogs);
    }
  }

  private static void runTest(LinkedHashSet<DfsLogger> closedLogs, Set<DfsLogger> inUseLogs,
      Set<DfsLogger> expected) {
    List<DfsLogger> copy = TabletServer.copyClosedLogs(closedLogs);
    Set<DfsLogger> eligible = TabletServer.findOldestUnreferencedWals(copy,
        new TestRefRemover(inUseLogs));
    Assert.assertEquals(expected, eligible);
  }

  @Test
  public void testWalRemoval() {
    runTest(logs("W1", "W2"), logs(), logs("W1", "W2"));
    runTest(logs("W1", "W2"), logs("W1"), logs());
    runTest(logs("W1", "W2"), logs("W2"), logs("W1"));
    runTest(logs("W1", "W2"), logs("W1", "W2"), logs());

    // below W5 represents an open log not in the closed set
    for (Set<DfsLogger> inUse : Sets.powerSet(logs("W1", "W2", "W3", "W4", "W5"))) {
      Set<DfsLogger> expected;
      if (inUse.contains(logger("W1"))) {
        expected = Collections.emptySet();
      } else if (inUse.contains(logger("W2"))) {
        expected = logs("W1");
      } else if (inUse.contains(logger("W3"))) {
        expected = logs("W1", "W2");
      } else if (inUse.contains(logger("W4"))) {
        expected = logs("W1", "W2", "W3");
      } else {
        expected = logs("W1", "W2", "W3", "W4");
      }

      runTest(logs("W1", "W2", "W3", "W4"), inUse, expected);
    }
  }
}
