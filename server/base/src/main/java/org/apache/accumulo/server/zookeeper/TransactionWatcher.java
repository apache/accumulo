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
package org.apache.accumulo.server.zookeeper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionWatcher {

  private static final Logger log = LoggerFactory.getLogger(TransactionWatcher.class);
  final private Map<Long,AtomicInteger> counts = new HashMap<>();
  final private Arbitrator arbitrator;

  public TransactionWatcher(Arbitrator arbitrator) {
    this.arbitrator = arbitrator;
  }

  public TransactionWatcher(ServerContext context) {
    this(new ZooArbitrator(context));
  }

  public interface Arbitrator {
    boolean transactionAlive(String type, long tid) throws Exception;

    boolean transactionComplete(String type, long tid) throws Exception;
  }

  public static class ZooArbitrator implements Arbitrator {

    private ServerContext context;
    private ZooReader rdr;

    public ZooArbitrator(ServerContext context) {
      this.context = context;
      rdr = context.getZooReader();
    }

    @Override
    public boolean transactionAlive(String type, long tid) throws Exception {
      String path = context.getZooKeeperRoot() + "/" + type + "/" + tid;
      rdr.sync(path);
      return rdr.exists(path);
    }

    public static void start(ServerContext context, String type, long tid)
        throws KeeperException, InterruptedException {
      ZooReaderWriter writer = context.getZooReaderWriter();
      writer.putPersistentData(context.getZooKeeperRoot() + "/" + type, new byte[] {},
          NodeExistsPolicy.OVERWRITE);
      writer.putPersistentData(context.getZooKeeperRoot() + "/" + type + "/" + tid, new byte[] {},
          NodeExistsPolicy.OVERWRITE);
      writer.putPersistentData(context.getZooKeeperRoot() + "/" + type + "/" + tid + "-running",
          new byte[] {}, NodeExistsPolicy.OVERWRITE);
    }

    public static void stop(ServerContext context, String type, long tid)
        throws KeeperException, InterruptedException {
      ZooReaderWriter writer = context.getZooReaderWriter();
      writer.recursiveDelete(context.getZooKeeperRoot() + "/" + type + "/" + tid,
          NodeMissingPolicy.SKIP);
    }

    public static void cleanup(ServerContext context, String type, long tid)
        throws KeeperException, InterruptedException {
      ZooReaderWriter writer = context.getZooReaderWriter();
      writer.recursiveDelete(context.getZooKeeperRoot() + "/" + type + "/" + tid,
          NodeMissingPolicy.SKIP);
      writer.recursiveDelete(context.getZooKeeperRoot() + "/" + type + "/" + tid + "-running",
          NodeMissingPolicy.SKIP);
    }

    public static Set<Long> allTransactionsAlive(ServerContext context, String type)
        throws KeeperException, InterruptedException {
      final ZooReader reader = context.getZooReaderWriter();
      final Set<Long> result = new HashSet<>();
      final String parent = context.getZooKeeperRoot() + "/" + type;
      reader.sync(parent);
      if (reader.exists(parent)) {
        List<String> children = reader.getChildren(parent);
        for (String child : children) {
          if (child.endsWith("-running")) {
            continue;
          }
          result.add(Long.parseLong(child));
        }
      }
      return result;
    }

    @Override
    public boolean transactionComplete(String type, long tid) throws Exception {
      String path = context.getZooKeeperRoot() + "/" + type + "/" + tid + "-running";
      rdr.sync(path);
      return !rdr.exists(path);
    }
  }

  /**
   * Run task only if transaction is still active in zookeeper. If the tx is no longer active then
   * that task is not run and a debug message is logged indicating the task was ignored.
   */
  public void runQuietly(String ztxBulk, long tid, Runnable task) {
    synchronized (counts) {
      try {
        if (!arbitrator.transactionAlive(ztxBulk, tid)) {
          log.debug("Transaction " + tid + " of type " + ztxBulk + " is no longer active.");
          return;
        }
      } catch (Exception e) {
        log.warn("Unable to check if transaction " + tid + " of type " + ztxBulk + " is alive ", e);
        return;
      }
      increment(tid);
    }
    try {
      task.run();
    } finally {
      decrement(tid);
    }
  }

  public <T> T run(String ztxBulk, long tid, Callable<T> callable) throws Exception {
    synchronized (counts) {
      if (!arbitrator.transactionAlive(ztxBulk, tid)) {
        throw new Exception("Transaction " + tid + " of type " + ztxBulk + " is no longer active");
      }
      increment(tid);
    }
    try {
      return callable.call();
    } finally {
      decrement(tid);
    }
  }

  public boolean isActive(long tid) {
    synchronized (counts) {
      log.debug("Transactions in progress {}", counts);
      AtomicInteger count = counts.get(tid);
      return count != null && count.get() > 0;
    }
  }

  private void increment(long tid) {
    AtomicInteger count = counts.get(tid);
    if (count == null) {
      counts.put(tid, count = new AtomicInteger());
    }
    count.incrementAndGet();
  }

  private void decrement(long tid) {
    synchronized (counts) {
      AtomicInteger count = counts.get(tid);
      if (count == null) {
        log.error("unexpected missing count for transaction {}", tid);
      } else {
        if (count.decrementAndGet() == 0) {
          counts.remove(tid);
        }
      }
    }
  }
}
