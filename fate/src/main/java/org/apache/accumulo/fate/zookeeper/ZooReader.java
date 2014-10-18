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
package org.apache.accumulo.fate.zookeeper;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZooReader implements IZooReader {
  private static final Logger log = Logger.getLogger(ZooReader.class);

  protected String keepers;
  protected int timeout;
  private final RetryFactory retryFactory;

  protected ZooKeeper getSession(String keepers, int timeout, String scheme, byte[] auth) {
    return ZooSession.getSession(keepers, timeout, scheme, auth);
  }

  protected ZooKeeper getZooKeeper() {
    return getSession(keepers, timeout, null, null);
  }

  protected RetryFactory getRetryFactory() {
    return retryFactory;
  }

  protected void retryOrThrow(Retry retry, KeeperException e) throws KeeperException {
    log.warn("Saw (possibly) transient exception communicating with ZooKeeper", e);
    if (retry.canRetry()) {
      retry.useRetry();
      return;
    }

    log.error("Retry attempts (" + retry.retriesCompleted() + ") exceeded trying to communicate with ZooKeeper");
    throw e;
  }

  @Override
  public byte[] getData(String zPath, Stat stat) throws KeeperException, InterruptedException {
    return getData(zPath, false, stat);
  }

  @Override
  public byte[] getData(String zPath, boolean watch, Stat stat) throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().create();
    while (true) {
      try {
        return getZooKeeper().getData(zPath, watch, stat);
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  @Override
  public Stat getStatus(String zPath) throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().create();
    while (true) {
      try {
        return getZooKeeper().exists(zPath, false);
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  @Override
  public Stat getStatus(String zPath, Watcher watcher) throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().create();
    while (true) {
      try {
        return getZooKeeper().exists(zPath, watcher);
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  @Override
  public List<String> getChildren(String zPath) throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().create();
    while (true) {
      try {
        return getZooKeeper().getChildren(zPath, false);
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  @Override
  public List<String> getChildren(String zPath, Watcher watcher) throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().create();
    while (true) {
      try {
        return getZooKeeper().getChildren(zPath, watcher);
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  @Override
  public boolean exists(String zPath) throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().create();
    while (true) {
      try {
        return getZooKeeper().exists(zPath, false) != null;
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  @Override
  public boolean exists(String zPath, Watcher watcher) throws KeeperException, InterruptedException {
    final Retry retry = getRetryFactory().create();
    while (true) {
      try {
        return getZooKeeper().exists(zPath, watcher) != null;
      } catch (KeeperException e) {
        final Code code = e.code();
        if (code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT || code == Code.SESSIONEXPIRED) {
          retryOrThrow(retry, e);
        } else {
          throw e;
        }
      }

      retry.waitForNextAttempt();
    }
  }

  @Override
  public void sync(final String path) throws KeeperException, InterruptedException {
    final AtomicInteger rc = new AtomicInteger();
    final AtomicBoolean waiter = new AtomicBoolean(false);
    getZooKeeper().sync(path, new VoidCallback() {
      @Override
      public void processResult(int code, String arg1, Object arg2) {
        rc.set(code);
        synchronized (waiter) {
          waiter.set(true);
          waiter.notifyAll();
        }
      }
    }, null);
    synchronized (waiter) {
      while (!waiter.get())
        waiter.wait();
    }
    Code code = Code.get(rc.get());
    if (code != KeeperException.Code.OK) {
      throw KeeperException.create(code);
    }
  }

  public ZooReader(String keepers, int timeout) {
    this.keepers = keepers;
    this.timeout = timeout;
    this.retryFactory = RetryFactory.DEFAULT_INSTANCE;
  }
}
