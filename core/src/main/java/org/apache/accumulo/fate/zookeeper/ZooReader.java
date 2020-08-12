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

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
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
    this.keepers = requireNonNull(keepers);
    this.timeout = timeout;
  }

  protected ZooKeeper getZooKeeper() {
    return ZooSession.getAnonymousSession(keepers, timeout);
  }

  protected RetryFactory getRetryFactory() {
    return RETRY_FACTORY;
  }

  /**
   * @throws KeeperException
   *           e, if the the issue is transient and no more retries are available
   * @throws InterruptedException
   *           if the retry is interrupted while waiting
   */
  protected static void retryTransientOrThrow(Retry retry, KeeperException e)
      throws KeeperException, InterruptedException {
    final Code c = e.code();
    if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
      log.warn("Saw (possibly) transient exception communicating with ZooKeeper", e);
      if (retry.canRetry()) {
        retry.useRetry();
        retry.waitForNextAttempt();
        return;
      }

      log.error("Retry attempts ({}) exceeded trying to communicate with ZooKeeper",
          retry.retriesCompleted());
      throw e;
    }
    // non-transient issue should always be thrown and handled by the caller
    throw e;
  }

  public byte[] getData(String zPath) throws KeeperException, InterruptedException {
    return retryLoop(zk -> zk.getData(zPath, null, null));
  }

  public byte[] getData(String zPath, Stat stat) throws KeeperException, InterruptedException {
    return retryLoop(zk -> zk.getData(zPath, null, requireNonNull(stat)));
  }

  public byte[] getData(String zPath, Watcher watcher)
      throws KeeperException, InterruptedException {
    return retryLoop(zk -> zk.getData(zPath, requireNonNull(watcher), null));
  }

  public Stat getStatus(String zPath) throws KeeperException, InterruptedException {
    return retryLoop(zk -> zk.exists(zPath, null));
  }

  public Stat getStatus(String zPath, Watcher watcher)
      throws KeeperException, InterruptedException {
    return retryLoop(zk -> zk.exists(zPath, requireNonNull(watcher)));
  }

  public List<String> getChildren(String zPath) throws KeeperException, InterruptedException {
    return retryLoop(zk -> zk.getChildren(zPath, null));
  }

  public List<String> getChildren(String zPath, Watcher watcher)
      throws KeeperException, InterruptedException {
    return retryLoop(zk -> zk.getChildren(zPath, requireNonNull(watcher)));
  }

  public boolean exists(String zPath) throws KeeperException, InterruptedException {
    return getStatus(zPath) != null;
  }

  public boolean exists(String zPath, Watcher watcher)
      throws KeeperException, InterruptedException {
    return getStatus(zPath, watcher) != null;
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
    if (code != Code.OK) {
      throw KeeperException.create(code);
    }
  }

  protected interface ZKFunction<R> extends ZKFunctionMutator<R> {
    @Override
    R apply(ZooKeeper zk) throws KeeperException, InterruptedException;
  }

  protected interface ZKFunctionMutator<R> {
    R apply(ZooKeeper zk)
        throws KeeperException, InterruptedException, AcceptableThriftTableOperationException;
  }

  protected <R> R retryLoop(ZKFunction<R> f) throws KeeperException, InterruptedException {
    return retryLoopPutData(f, null);
  }

  protected <R> R retryLoopPutData(ZKFunction<R> f,
      UnaryOperator<KeeperException> skipRetryIncrement)
      throws KeeperException, InterruptedException {
    try {
      return retryLoopMutator(f, skipRetryIncrement);
    } catch (AcceptableThriftTableOperationException e) {
      throw new AssertionError("Don't use this method for whatever you're trying to do.");
    }
  }

  // if skipRetryIncrement returns null, the exception was handled, and the loop should
  // repeat without incrementing the retry; otherwise, retry using the new exception
  protected <R> R retryLoopMutator(ZKFunctionMutator<R> f,
      UnaryOperator<KeeperException> skipRetryIncrement)
      throws KeeperException, InterruptedException, AcceptableThriftTableOperationException {
    var retry = getRetryFactory().createRetry();
    while (true) {
      try {
        return f.apply(getZooKeeper());
      } catch (KeeperException e1) {
        KeeperException e2 = skipRetryIncrement == null ? e1 : skipRetryIncrement.apply(e1);
        if (e2 != null) {
          retryTransientOrThrow(retry, e2);
        }
      }
    }
  }
}
