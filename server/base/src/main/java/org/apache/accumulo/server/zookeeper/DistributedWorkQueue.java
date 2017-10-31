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
package org.apache.accumulo.server.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a way to push work out to tablet servers via zookeeper and wait for that work to be done. Any tablet server can pick up a work item and process it.
 *
 * Worker processes watch a zookeeper node for tasks to be performed. After getting an exclusive lock on the node, the worker will perform the task.
 */
public class DistributedWorkQueue {

  private static final String LOCKS_NODE = "locks";

  private static final Logger log = LoggerFactory.getLogger(DistributedWorkQueue.class);

  private ThreadPoolExecutor threadPool;
  private ZooReaderWriter zoo = ZooReaderWriter.getInstance();
  private String path;
  private AccumuloConfiguration config;
  private long timerInitialDelay, timerPeriod;

  private AtomicInteger numTask = new AtomicInteger(0);

  private void lookForWork(final Processor processor, List<String> children) {
    if (children.size() == 0)
      return;

    if (numTask.get() >= threadPool.getCorePoolSize())
      return;

    Random random = new Random();
    Collections.shuffle(children, random);
    try {
      for (final String child : children) {

        if (child.equals(LOCKS_NODE))
          continue;

        final String lockPath = path + "/locks/" + child;

        try {
          // no need to use zoolock, because a queue (ephemeral sequential) is not needed
          // if can not get the lock right now then do not want to wait
          zoo.putEphemeralData(lockPath, new byte[0]);
        } catch (NodeExistsException nee) {
          // someone else has reserved it
          continue;
        }

        final String childPath = path + "/" + child;

        // check to see if another node processed it already
        if (!zoo.exists(childPath)) {
          zoo.recursiveDelete(lockPath, NodeMissingPolicy.SKIP);
          continue;
        }

        // Great... we got the lock, but maybe we're too busy
        if (numTask.get() >= threadPool.getCorePoolSize()) {
          zoo.recursiveDelete(lockPath, NodeMissingPolicy.SKIP);
          break;
        }

        log.debug("got lock for {}", child);

        Runnable task = new Runnable() {

          @Override
          public void run() {
            try {
              try {
                processor.newProcessor().process(child, zoo.getData(childPath, null));

                // if the task fails, then its entry in the Q is not deleted... so it will be retried
                try {
                  zoo.recursiveDelete(childPath, NodeMissingPolicy.SKIP);
                } catch (Exception e) {
                  log.error("Error received when trying to delete entry in zookeeper " + childPath, e);
                }

              } catch (Exception e) {
                log.warn("Failed to process work " + child, e);
              }

              try {
                zoo.recursiveDelete(lockPath, NodeMissingPolicy.SKIP);
              } catch (Exception e) {
                log.error("Error received when trying to delete entry in zookeeper " + childPath, e);
              }

            } finally {
              numTask.decrementAndGet();
            }

            try {
              // its important that this is called after numTask is decremented
              lookForWork(processor, zoo.getChildren(path));
            } catch (KeeperException e) {
              log.error("Failed to look for work", e);
            } catch (InterruptedException e) {
              log.info("Interrupted looking for work", e);
            }
          }
        };

        numTask.incrementAndGet();
        threadPool.execute(task);

      }
    } catch (Throwable t) {
      log.error("Unexpected error", t);
    }
  }

  public interface Processor {
    Processor newProcessor();

    void process(String workID, byte[] data);
  }

  public DistributedWorkQueue(String path, AccumuloConfiguration config) {
    // Preserve the old delay and period
    this(path, config, new Random().nextInt(60 * 1000), 60 * 1000);
  }

  public DistributedWorkQueue(String path, AccumuloConfiguration config, long timerInitialDelay, long timerPeriod) {
    this.path = path;
    this.config = config;
    this.timerInitialDelay = timerInitialDelay;
    this.timerPeriod = timerPeriod;
  }

  public void startProcessing(final Processor processor, ThreadPoolExecutor executorService) throws KeeperException, InterruptedException {

    threadPool = executorService;

    zoo.mkdirs(path);
    zoo.mkdirs(path + "/" + LOCKS_NODE);

    List<String> children = zoo.getChildren(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        switch (event.getType()) {
          case NodeChildrenChanged:
            if (event.getPath().equals(path))
              try {
                lookForWork(processor, zoo.getChildren(path, this));
              } catch (KeeperException e) {
                log.error("Failed to look for work", e);
              } catch (InterruptedException e) {
                log.info("Interrupted looking for work", e);
              }
            else
              log.info("Unexpected path for NodeChildrenChanged event {}", event.getPath());
            break;
          case NodeCreated:
          case NodeDataChanged:
          case NodeDeleted:
          case None:
            log.info("Got unexpected zookeeper event: {} for {}", event.getType(), path);
            break;

        }
      }
    });

    lookForWork(processor, children);

    // Add a little jitter to avoid all the tservers slamming zookeeper at once
    SimpleTimer.getInstance(config).schedule(new Runnable() {
      @Override
      public void run() {
        log.debug("Looking for work in {}", path);
        try {
          lookForWork(processor, zoo.getChildren(path));
        } catch (KeeperException e) {
          log.error("Failed to look for work", e);
        } catch (InterruptedException e) {
          log.info("Interrupted looking for work", e);
        }
      }
    }, timerInitialDelay, timerPeriod);
  }

  /**
   * Adds work to the queue, automatically converting the String to bytes using UTF-8
   */
  public void addWork(String workId, String data) throws KeeperException, InterruptedException {
    addWork(workId, data.getBytes(UTF_8));
  }

  public void addWork(String workId, byte[] data) throws KeeperException, InterruptedException {
    if (workId.equalsIgnoreCase(LOCKS_NODE))
      throw new IllegalArgumentException("locks is reserved work id");

    zoo.mkdirs(path);
    zoo.putPersistentData(path + "/" + workId, data, NodeExistsPolicy.SKIP);
  }

  public List<String> getWorkQueued() throws KeeperException, InterruptedException {
    ArrayList<String> children = new ArrayList<>(zoo.getChildren(path));
    children.remove(LOCKS_NODE);
    return children;
  }

  public void waitUntilDone(Set<String> workIDs) throws KeeperException, InterruptedException {

    final Object condVar = new Object();

    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        switch (event.getType()) {
          case NodeChildrenChanged:
            synchronized (condVar) {
              condVar.notify();
            }
            break;
          case NodeCreated:
          case NodeDataChanged:
          case NodeDeleted:
          case None:
            log.info("Got unexpected zookeeper event: {} for {}", event.getType(), path);
            break;

        }
      }
    };

    List<String> children = zoo.getChildren(path, watcher);

    while (!Collections.disjoint(children, workIDs)) {
      synchronized (condVar) {
        condVar.wait(10000);
      }
      children = zoo.getChildren(path, watcher);
    }
  }
}
