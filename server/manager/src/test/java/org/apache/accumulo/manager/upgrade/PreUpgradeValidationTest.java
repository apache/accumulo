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
package org.apache.accumulo.manager.upgrade;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.server.AccumuloDataVersion.REMOVE_DEPRECATIONS_FOR_VERSION_3;
import static org.apache.accumulo.server.AccumuloDataVersion.ROOT_TABLET_META_CHANGES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerDirs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.AclParser;
import org.apache.zookeeper.data.ACL;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreUpgradeValidationTest {

  private static final Logger log = LoggerFactory.getLogger(PreUpgradeValidationTest.class);

  @Test
  public void noUpdateNeededTest() throws Exception {
    ServerContext context = Mockito.mock(ServerContext.class);

    Mockito.mockStatic(AccumuloDataVersion.class);
    Mockito.when(AccumuloDataVersion.getCurrentVersion(context))
        .thenReturn(REMOVE_DEPRECATIONS_FOR_VERSION_3);
    Mockito.when(AccumuloDataVersion.get()).thenReturn(REMOVE_DEPRECATIONS_FOR_VERSION_3);

    PreUpgradeValidation pcheck = new PreUpgradeValidation();
    pcheck.validate(context, null);

    Mockito.verify(context);
  }

  @Test
  public void needsUpdateNoLocksTest() throws Exception {
    final String uuid = UUID.randomUUID().toString();
    final String zkRootPath = "/accumulo/" + uuid;

    ServerContext context = Mockito.mock(ServerContext.class);
    ServerDirs serverDirs = Mockito.mock(ServerDirs.class);

    ZooReaderWriter zrw = Mockito.mock(ZooReaderWriter.class);
    ZooKeeper zk = Mockito.mock(ZooKeeper.class);

    Mockito.mockStatic(AccumuloDataVersion.class);
    Mockito.when(AccumuloDataVersion.getCurrentVersion(context))
        .thenReturn(ROOT_TABLET_META_CHANGES);
    Mockito.when(AccumuloDataVersion.get()).thenReturn(REMOVE_DEPRECATIONS_FOR_VERSION_3);

    Mockito.when(context.getZooReaderWriter()).thenReturn(zrw);
    Mockito.when(context.getZooKeeperRoot()).thenReturn(zkRootPath);

    Mockito.when(zrw.getZooKeeper()).thenReturn(zk);

    List<ACL> pub = AclParser.parse("digest:accumulo:abcde123:cdrwa");
    Mockito.when(zk.getACL(Mockito.eq(zkRootPath), any())).thenReturn(pub);

    // expect(context.getServerDirs()).andReturn(null).anyTimes();
    // expect(context.getVolumeManager()).andReturn(null).anyTimes();
    // expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    // expect(context.getZooKeeperRoot()).andReturn("/accumulo/" + uuid).anyTimes();
    //
    // expect(zrw.getZooKeeper()).andReturn(zk).anyTimes();

    // expect(zk.getData(eq("/accumulo/" + uuid), anyBoolean(), eq(null))).andReturn(new byte[0])
    // .anyTimes();
    // List<ACL> pub = AclParser.parse("digest:accumulo:abcde123:cdrwa");
    // log.info("USING: {}", pub);
    // expect(zk.getACL(eq("/accumulo/" + uuid), anyObject())).andReturn(pub).anyTimes();
    // expect(zk.getChildren(eq("/accumulo/" + uuid), anyBoolean(), anyObject()))
    // .andReturn(new ArrayList<>()).anyTimes();
    //
    // replay(context, serverDirs, zrw, zk);

    PreUpgradeValidation pcheck = new PreUpgradeValidation();
    pcheck.validate(context, null);

    Mockito.verify(context, times(2)).getZooReaderWriter();
    Mockito.verify(context, times(2)).getZooKeeperRoot();
    verifyNoMoreInteractions(context);

    Mockito.verify(zrw, times(2)).getZooKeeper();
    verifyNoMoreInteractions(zrw);

    Mockito.verify(zk).getACL(any(), any());
    // verifyNoMoreInteractions(zk);

    // verify(context, serverDirs, zrw, zk);
  }

  @Test
  public void retryExample() {
    int attempts = 0;

    long maxWaitSec = 5;

    Retry retry =
        Retry.builder().maxRetries(10).retryAfter(50, MILLISECONDS).incrementBy(100, MILLISECONDS)
            .maxWait(maxWaitSec, SECONDS).backOffFactor(2.0).logInterval(1, SECONDS).createRetry();
    try {
      boolean haveServers = false;
      long startNanos = System.nanoTime();
      while (!haveServers && retry.canRetry()) {
        haveServers = checkTserverCount(10, startNanos, MINUTES.toNanos(5));
        attempts++;
        retry.logRetry(log, "current counts " + attempts);
        retry.waitForNextAttempt(log, "pause for next attempt");
        retry.useRetry();
      }
    } catch (InterruptedException ex) {
      log.info("Interrupted", ex);
    }
    retry.logCompletion(log, "finished after " + attempts + " attempts");
  }

  boolean checkTserverCount(int numNeeded, long startTimeNanos, long maxWaitNanos) {
    int numServers = getServerCount();
    if (numServers > -numNeeded || (System.nanoTime() - startTimeNanos) > maxWaitNanos) {
      return true;
    }
    return false;
  }

  int getServerCount() {
    return 1;
  }
}
