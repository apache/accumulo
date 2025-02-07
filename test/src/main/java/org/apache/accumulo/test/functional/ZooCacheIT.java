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
package org.apache.accumulo.test.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.core.zookeeper.ZooCache.ZooCacheWatcher;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.test.util.Wait;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ZooCacheIT extends ConfigurableMacBase {

  private static final Logger LOG = LoggerFactory.getLogger(ZooCacheIT.class);

  private static String pathName = "/zcTest-42";
  private static File testDir;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "path provided by test")
  @BeforeAll
  public static void createTestDirectory() {
    testDir = new File(createTestDir(ZooCacheIT.class.getName()), pathName);
    FileUtils.deleteQuietly(testDir);
    assertTrue(testDir.mkdir());
  }

  @Test
  public void test() throws Exception {
    assertEquals(0, exec(CacheTestClean.class, pathName, testDir.getAbsolutePath()).waitFor());
    final AtomicReference<Exception> ref = new AtomicReference<>();
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Thread reader = new Thread(() -> {
        try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
          CacheTestReader.main(new String[] {pathName, testDir.getAbsolutePath(),
              ClientInfo.from(client.properties()).getZooKeepers()});
        } catch (Exception ex) {
          ref.set(ex);
        }
      });
      reader.start();
      threads.add(reader);
    }
    assertEquals(0,
        exec(CacheTestWriter.class, pathName, testDir.getAbsolutePath(), "3", "50").waitFor());
    for (Thread t : threads) {
      t.join();
      if (ref.get() != null) {
        throw ref.get();
      }
    }
  }

  @Test
  public void testZooKeeperClientChanges() throws Exception {

    try (final AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build();
        final ClientContext ctx = ((ClientContext) client)) {

      final InstanceId iid = ctx.getInstanceID();
      final String zooRoot = ZooUtil.getRoot(iid);
      final String tableName = "test_Table";

      final AtomicBoolean tableCreatedEvent = new AtomicBoolean(false);
      final AtomicBoolean tableDeletedEvent = new AtomicBoolean(false);
      final AtomicBoolean connectionOpenEvent = new AtomicBoolean(false);
      final AtomicBoolean connectionClosedEvent = new AtomicBoolean(false);
      final AtomicBoolean sessionExpiredEvent = new AtomicBoolean(false);
      final AtomicReference<String> tableId = new AtomicReference<>();
      final Supplier<AtomicReference<String>> tableIdSup = () -> tableId;

      final ZooCache cache = ctx.getZooCache();
      cache.addZooCacheWatcher(new ZooCacheWatcher() {

        @Override
        @SuppressWarnings("deprecation")
        public void accept(WatchedEvent event) {

          final String eventPath = event.getPath();

          if (event.getType() != EventType.None
              && !eventPath.startsWith(zooRoot + Constants.ZTABLES)) {
            return;
          }
          // LOG.info("received event {}", event);

          String tableId = tableIdSup.get().get();

          switch (event.getType()) {
            case ChildWatchRemoved:
            case DataWatchRemoved:
            case NodeChildrenChanged:
            case NodeDataChanged:
            case PersistentWatchRemoved:
              break;
            case NodeCreated:
              if (eventPath.equals(zooRoot + Constants.ZTABLES + "/" + tableId)) {
                LOG.info("Setting tableCreatedEvent");
                tableCreatedEvent.set(true);
              }
              break;
            case NodeDeleted:
              if (eventPath.equals(zooRoot + Constants.ZTABLES + "/" + tableId)) {
                LOG.info("Setting tableDeletedEvent");
                tableDeletedEvent.set(true);
              }
              break;
            case None:
              switch (event.getState()) {
                case AuthFailed:
                case ConnectedReadOnly:
                case NoSyncConnected:
                case SaslAuthenticated:
                case Unknown:
                  break;
                case Closed:
                case Disconnected:
                  LOG.info("Setting connectionClosedEvent");
                  connectionClosedEvent.set(true);
                  break;
                case Expired:
                  LOG.info("Setting sessionExpiredEvent");
                  sessionExpiredEvent.set(true);
                  break;
                case SyncConnected:
                  LOG.info("Setting connectionOpenEvent");
                  connectionOpenEvent.set(true);
                  break;
                default:
                  break;

              }
              break;
            default:
              break;

          }
        }

      });

      assertFalse(connectionOpenEvent.get()); // missed the event, connection already established

      client.tableOperations().create(tableName);
      final String tid = ctx.tableOperations().tableIdMap().get(tableName);
      tableId.set(tid);
      // we might miss the table created event, don't check for it
      final String tableZPath = zooRoot + Constants.ZTABLES + "/" + tid;
      assertFalse(cache.childrenCached(tableZPath));
      assertNotNull(cache.getChildren(tableZPath));
      assertTrue(cache.childrenCached(tableZPath));

      client.tableOperations().delete(tableName);
      Wait.waitFor(() -> tableDeletedEvent.get(), 60_000);
      assertFalse(cache.childrenCached(tableZPath)); // ZooCache watcher fired and deleted the data
      assertNull(cache.getChildren(tableZPath));

      assertFalse(connectionOpenEvent.get());
      assertFalse(connectionClosedEvent.get());
      tableDeletedEvent.set(false);

      getCluster().getClusterControl().stop(ServerType.ZOOKEEPER);

      Wait.waitFor(() -> connectionClosedEvent.get(), 30_000);
      connectionClosedEvent.set(false);
      // Cache should be cleared
      assertFalse(cache.childrenCached(zooRoot + Constants.ZTABLES));

      getCluster().getClusterControl().start(ServerType.ZOOKEEPER);

      Wait.waitFor(() -> connectionOpenEvent.get(), 60_000);
      connectionOpenEvent.set(false);

      // Cache should be cleared
      assertFalse(cache.childrenCached(zooRoot + Constants.ZTABLES));

      // let's assume that are tableId will be one more than the previous (safe assumption)
      String newTableId = Integer.parseInt(tid) + 1 + "";
      tableId.set(newTableId);
      client.tableOperations().create(tableName);
      Wait.waitFor(() -> tableCreatedEvent.get(), 60_000);

      final String newTableZPath = zooRoot + Constants.ZTABLES + "/" + newTableId;
      assertFalse(cache.childrenCached(newTableZPath));
      assertNotNull(cache.getChildren(newTableZPath));
      assertTrue(cache.childrenCached(newTableZPath));

      client.tableOperations().delete(tableName);
      Wait.waitFor(() -> tableDeletedEvent.get(), 60_000);
      assertFalse(cache.childrenCached(newTableZPath)); // ZooCache watcher fired and deleted the
                                                        // data
      assertNull(cache.getChildren(newTableZPath));

      assertFalse(connectionOpenEvent.get());
      assertFalse(connectionClosedEvent.get());

    }
  }
}
