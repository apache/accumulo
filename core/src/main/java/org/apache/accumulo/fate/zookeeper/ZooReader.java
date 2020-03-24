/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.fate.zookeeper;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.fate.util.Retry;
import org.apache.accumulo.fate.util.Retry.RetryFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooReader {
  private static final Logger log = LoggerFactory.getLogger(ZooReader.class);

  protected static final RetryFactory RETRY_FACTORY = Retry.builder().maxRetries(10)
      .retryAfter(250, MILLISECONDS).incrementBy(250, MILLISECONDS).maxWait(5, TimeUnit.SECONDS)
      .backOffFactor(1.5).logInterval(3, TimeUnit.MINUTES).createFactory();

  protected final String keepers;
  protected final int timeout;

  public ZooReader(String keepers, int timeout) {
    this.keepers = keepers;
    this.timeout = timeout;
  }

  protected ZooKeeper getZooKeeper() {
    return ZooSession.getAnonymousSession(keepers, timeout);
  }

  protected RetryFactory getRetryFactory() {
    return RETRY_FACTORY;
  }

  protected static void retryOrThrow(Retry retry, KeeperException e) throws KeeperException {
    log.warn("Saw (possibly) transient exception communicating with ZooKeeper", e);
    if (retry.canRetry()) {
      retry.useRetry();
      return;
    }

    log.error("Retry attempts ({}) exceeded trying to communicate with ZooKeeper",
        retry.retriesCompleted());
    throw e;
  }

  public byte[] getData(String zPath, Stat stat) throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        return getZooKeeper().getData(zPath, false, stat);
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT
            || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public byte[] getData(String zPath, Watcher watcher, Stat stat)
      throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        return getZooKeeper().getData(zPath, watcher, stat);
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT
            || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public Stat getStatus(String zPath) throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        return getZooKeeper().exists(zPath, false);
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT
            || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public Stat getStatus(String zPath, Watcher watcher)
      throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        return getZooKeeper().exists(zPath, watcher);
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT
            || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public List<String> getChildren(String zPath) throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        return getZooKeeper().getChildren(zPath, false);
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT
            || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public List<String> getChildren(String zPath, Watcher watcher)
      throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        return getZooKeeper().getChildren(zPath, watcher);
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT
            || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public boolean exists(String zPath) throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        return getZooKeeper().exists(zPath, false) != null;
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT
            || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public boolean exists(String zPath, Watcher watcher)
      throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().createRetry();
    while (true) {
      try {
        return getZooKeeper().exists(zPath, watcher) != null;
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT
            || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  public void sync(final String path) throws KeeperException, InterruptedException {
    final AtomicInteger rc = new AtomicInteger();
    final CountDownLatch waiter = new CountDownLatch(1);
    getZooKeeper().sync(path, (code, arg1, arg2) -> {
      rc.set(code);
      waiter.countDown();
    }, null);
    waiter.await();
    Code code = Code.get(rc.get());
    if (code != KeeperException.Code.OK) {
      throw KeeperException.create(code);
    }
  }

}
