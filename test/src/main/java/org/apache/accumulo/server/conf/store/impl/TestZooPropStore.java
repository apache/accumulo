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
package org.apache.accumulo.server.conf.store.impl;

import java.util.concurrent.CountDownLatch;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Ticker;

public class TestZooPropStore extends ZooPropStore {

  private static final Logger LOG = LoggerFactory.getLogger(TestZooPropStore.class);

  private final Runnable closeConnectionTask;
  private final CountDownLatch latch = new CountDownLatch(1);

  public TestZooPropStore(InstanceId instanceId, ZooReaderWriter zrw, ReadyMonitor monitor,
      PropStoreWatcher watcher, Ticker ticker, Runnable closeConnectionTask) {
    super(instanceId, zrw, monitor, watcher, ticker);
    this.closeConnectionTask = closeConnectionTask;
  }

  private void runCloseConnectionTask() {
    Thread t = Threads.createThread("close-connection-task", closeConnectionTask);
    t.start();
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      LOG.error("Sleep interrupted", e);
    } // allow the thread to fire
  }

  @Override
  public @NonNull VersionedProperties get(PropStoreKey<?> propStoreKey) {
    try {
      return super.get(propStoreKey);
    } finally {
      // try {
      // latch.await();
      // } catch (InterruptedException e) {
      // LOG.error("Error waiting for latch", e);
      // }
    }
  }

  @Override
  public void checkZkConnection() {
    super.checkZkConnection();
  }

  @Override
  public void connectionEvent() {
    super.connectionEvent();
    latch.countDown();
  }

}
