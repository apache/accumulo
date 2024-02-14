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

import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a way to push work out to tablet servers via zookeeper and wait for that work to be
 * done. Any tablet server can pick up a work item and process it.
 *
 * Worker processes watch a zookeeper node for tasks to be performed. After getting an exclusive
 * lock on the node, the worker will perform the task.
 */
public class DistributedWorkQueue {

  private static final String LOCKS_NODE = "locks";

  private static final Logger log = LoggerFactory.getLogger(DistributedWorkQueue.class);

  private ZooReaderWriter zoo;
  private String path;
  private ServerContext context;
  private long timerInitialDelay, timerPeriod;

  private AtomicInteger numTask = new AtomicInteger(0);

  /**
   * Finds a child in {@code children} that is not currently being processed and adds a Runnable to
   * the {@code executor} that invokes the {@code processor}. The Runnable will recursively call
   * {@code lookForWork} after it invokes the {@code processor} such that it will continue to look
   * for children that need work until that condition is exhausted. This method will return early if
   * the number of currently running tasks is larger than {@code maxThreads}.
   */
  private void lookForWork(final Processor processor, final List<String> children,
      final ExecutorService executor, final int maxThreads) {
    if (children.isEmpty()) {
      return;
    }

    if (numTask.get() >= maxThreads) {
      return;
    }

    Collections.shuffle(children, RANDOM.get());
    try {
      for (final String child : children) {

        if (child.equals(LOCKS_NODE)) {
          continue;
        }

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
        if (numTask.get() >= maxThreads) {
          zoo.recursiveDelete(lockPath, NodeMissingPolicy.SKIP);
          break;
        }

        log.debug("got lock for {}", child);

        Runnable task = new Runnable() {

          @Override
          public void run() {
            try {
              try {
                processor.newProcessor().process(child, zoo.getData(childPath));

                // if the task fails, then its entry in the Q is not deleted... so it will be
                // retried
                try {
                  zoo.recursiveDelete(childPath, NodeMissingPolicy.SKIP);
                } catch (Exception e) {
                  log.error("Error received when trying to delete entry in zookeeper " + childPath,
                      e);
                }

              } catch (Exception e) {
                log.warn("Failed to process work " + child, e);
              }

              try {
                zoo.recursiveDelete(lockPath, NodeMissingPolicy.SKIP);
              } catch (Exception e) {
                log.error("Error received when trying to delete entry in zookeeper " + childPath,
                    e);
              }

            } finally {
              numTask.decrementAndGet();
            }

            try {
              // its important that this is called after numTask is decremented
              lookForWork(processor, zoo.getChildren(path), executor, maxThreads);
            } catch (KeeperException e) {
              log.error("Failed to look for work", e);
            } catch (InterruptedException e) {
              log.info("Interrupted looking for work", e);
            }
          }
        };

        numTask.incrementAndGet();
        executor.execute(task);

      }
    } catch (Exception t) {
      log.error("Unexpected error", t);
    }
  }

  public interface Processor {
    Processor newProcessor();

    void process(String workID, byte[] data);
  }

  public DistributedWorkQueue(String path, AccumuloConfiguration config, ServerContext context) {
    // Preserve the old delay and period
    this(path, config, context, RANDOM.get().nextInt(toIntExact(MINUTES.toMillis(1))),
        MINUTES.toMillis(1));
  }

  public DistributedWorkQueue(String path, AccumuloConfiguration config, ServerContext context,
      long timerInitialDelay, long timerPeriod) {
    this.path = path;
    this.context = context;
    this.timerInitialDelay = timerInitialDelay;
    this.timerPeriod = timerPeriod;
    zoo = context.getZooReaderWriter();
  }

  public ServerContext getContext() {
    return context;
  }

  public long getCheckInterval() {
    return this.timerPeriod;
  }

  /**
   * Finds the children at the path passed in the constructor and calls {@code lookForWork} which
   * will attempt to process all of the currently available work
   */
  public void processExistingWork(final Processor processor, ExecutorService executor,
      final int maxThreads, boolean setWatch) throws KeeperException, InterruptedException {

    zoo.mkdirs(path);
    zoo.mkdirs(path + "/" + LOCKS_NODE);

    List<String> children = null;
    if (setWatch) {
      children = zoo.getChildren(path, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          switch (event.getType()) {
            case NodeChildrenChanged:
              if (event.getPath().equals(path)) {
                try {
                  lookForWork(processor, zoo.getChildren(path, this), executor, maxThreads);
                } catch (KeeperException e) {
                  log.error("Failed to look for work at path {}; {}", path, event, e);
                } catch (InterruptedException e) {
                  log.info("Interrupted looking for work at path {}; {}", path, event, e);
                }
              } else {
                log.info("Unexpected path for NodeChildrenChanged event watching path {}; {}", path,
                    event);
              }
              break;
            default:
              log.info("Unexpected event watching path {}; {}", path, event);
              break;
          }
        }
      });
    } else {
      children = zoo.getChildren(path);
    }

    lookForWork(processor, children, executor, maxThreads);

  }

  /**
   * Calls {@code runOne} to attempt to process all currently available work, then adds a background
   * thread that looks for work in the future.
   */
  public void processExistingAndFuture(final Processor processor,
      ThreadPoolExecutor executorService) throws KeeperException, InterruptedException {

    processExistingWork(processor, executorService, executorService.getCorePoolSize(), true);

    // Add a little jitter to avoid all the tservers slamming zookeeper at once
    ThreadPools.watchCriticalScheduledTask(
        context.getScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
          @Override
          public void run() {
            log.debug("Looking for work in {}", path);
            try {
              lookForWork(processor, zoo.getChildren(path), executorService,
                  executorService.getCorePoolSize());
            } catch (KeeperException e) {
              log.error("Failed to look for work", e);
            } catch (InterruptedException e) {
              log.info("Interrupted looking for work", e);
            }
          }
        }, timerInitialDelay, timerPeriod, TimeUnit.MILLISECONDS));
  }

  /**
   * Adds work to the queue, automatically converting the String to bytes using UTF-8
   */
  public void addWork(String workId, String data) throws KeeperException, InterruptedException {
    addWork(workId, data.getBytes(UTF_8));
  }

  public void addWork(String workId, byte[] data) throws KeeperException, InterruptedException {

    if (workId.equalsIgnoreCase(LOCKS_NODE)) {
      throw new IllegalArgumentException("locks is reserved work id");
    }

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
          default:
            log.info("Got unexpected zookeeper event for path {}: {}", path, event);
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
