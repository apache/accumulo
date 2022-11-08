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
package org.apache.accumulo.core.fate.zookeeper;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.Retry.RetryFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooReader {
  private static final Logger log = LoggerFactory.getLogger(ZooReader.class);

  protected static final RetryFactory RETRY_FACTORY =
      Retry.builder().maxRetries(10).retryAfter(250, MILLISECONDS).incrementBy(250, MILLISECONDS)
          .maxWait(5, SECONDS).backOffFactor(1.5).logInterval(3, MINUTES).createFactory();

  protected final String keepers;
  protected final int timeout;

  public ZooReader(String keepers, int timeout) {
    this.keepers = requireNonNull(keepers);
    this.timeout = timeout;
  }

  public ZooReaderWriter asWriter(String secret) {
    return new ZooReaderWriter(keepers, timeout, secret);
  }

  protected ZooKeeper getZooKeeper() {
    return ZooSession.getAnonymousSession(keepers, timeout);
  }

  protected RetryFactory getRetryFactory() {
    return RETRY_FACTORY;
  }

  /**
   * Returns the requested ZooKeeper client session timeout. The client may negotiate a different
   * value and the actual negotiated value may change after a re-connect.
   *
   * @return the timeout in milliseconds
   */
  public int getSessionTimeout() {
    return timeout;
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

  public byte[] getData(String zPath, Watcher watcher, Stat stat)
      throws KeeperException, InterruptedException {
    return retryLoop(zk -> zk.getData(zPath, requireNonNull(watcher), requireNonNull(stat)));
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

  protected interface ZKFunction<R> {
    R apply(ZooKeeper zk) throws KeeperException, InterruptedException;
  }

  protected interface ZKFunctionMutator<R> {
    R apply(ZooKeeper zk)
        throws KeeperException, InterruptedException, AcceptableThriftTableOperationException;
  }

  /**
   * This method executes the provided function, retrying several times for transient issues.
   */
  protected <R> R retryLoop(ZKFunction<R> f) throws KeeperException, InterruptedException {
    return retryLoop(f, e -> false);
  }

  /**
   * This method executes the provided function, retrying several times for transient issues, and
   * retrying indefinitely for special cases. Use {@link #retryLoop(ZKFunction)} if there is no such
   * special case.
   */
  protected <R> R retryLoop(ZKFunction<R> zkf, Predicate<KeeperException> alwaysRetryCondition)
      throws KeeperException, InterruptedException {
    try {
      // reuse the code from retryLoopMutator, but suppress the exception
      // because ZKFunction can't throw it
      return retryLoopMutator(zkf::apply, alwaysRetryCondition);
    } catch (AcceptableThriftTableOperationException e) {
      throw new AssertionError("Not possible; " + ZKFunction.class.getName() + " can't throw "
          + AcceptableThriftTableOperationException.class.getName());
    }
  }

  /**
   * This method is a special case of {@link #retryLoop(ZKFunction, Predicate)}, intended to handle
   * {@link ZooReaderWriter#mutateExisting(String, ZooReaderWriter.Mutator)}'s additional thrown
   * exception type. Other callers should use {@link #retryLoop(ZKFunction)} or
   * {@link #retryLoop(ZKFunction, Predicate)} instead.
   */
  protected <R> R retryLoopMutator(ZKFunctionMutator<R> zkf,
      Predicate<KeeperException> alwaysRetryCondition)
      throws KeeperException, InterruptedException, AcceptableThriftTableOperationException {
    requireNonNull(zkf);
    requireNonNull(alwaysRetryCondition);
    var retries = getRetryFactory().createRetry();
    while (true) {
      try {
        return zkf.apply(getZooKeeper());
      } catch (KeeperException e) {
        if (alwaysRetryCondition.test(e) || useRetryForTransient(retries, e)) {
          continue;
        }
        throw e;
      }
    }
  }

  // should use an available retry if there are retries left and
  // the issue is one that is likely to be transient
  private static boolean useRetryForTransient(Retry retries, KeeperException e)
      throws KeeperException, InterruptedException {
    final Code c = e.code();
    if (c == Code.CONNECTIONLOSS || c == Code.OPERATIONTIMEOUT || c == Code.SESSIONEXPIRED) {
      log.warn("Saw (possibly) transient exception communicating with ZooKeeper", e);
      if (retries.canRetry()) {
        retries.useRetry();
        retries.waitForNextAttempt(log, "attempting to communicate with zookeeper after exception");
        return true;
      }
      log.error("Retry attempts ({}) exceeded trying to communicate with ZooKeeper",
          retries.retriesCompleted());
    }
    // non-transient issue should always be thrown and handled by the caller
    return false;
  }

}
