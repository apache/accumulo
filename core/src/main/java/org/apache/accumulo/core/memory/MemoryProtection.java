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
package org.apache.accumulo.core.memory;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;

import org.apache.accumulo.core.util.threads.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryProtection implements NotificationListener {

  private static class FreeMemoryUpdater implements Runnable {

    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicReference<CountDownLatch> latchRef =
        new AtomicReference<>(new CountDownLatch(1));

    public void update() {
      lock.lock();
      try {
        latchRef.get().countDown();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void run() {
      try {
        while (true) {
          CountDownLatch latch = latchRef.get();
          // TODO: Could use latch.await(long, TimeUnit) to update free memory
          // when GC does not occur. It's probable that memory allocations
          // will occur without GC happening, depending on the memory pool
          // sizes.
          latch.await();
          // acquiring a lock here so that we don't miss a call to update() while
          // we are getting the current free memory
          lock.lock();
          try {
            FREE_MEMORY.set(calculateFreeMemory());
            LOG.info("Free Memory set to: {}", FREE_MEMORY.get());
            latchRef.set(new CountDownLatch(1));
          } finally {
            lock.unlock();
          }
        }
      } catch (InterruptedException e) {
        throw new RuntimeException("RFileMemoryProtection thread interrupted");
      }
    }

  }

  public interface ProtectedAction {
    void execute() throws IOException;
  }

  public static final String ENABLED_PROPERTY = "EnableRFileMemoryProtection";
  public static final String SIZE_THRESHOLD_PROPERTY = "RFileMemoryProtectionSizeThreshold";

  private static final Logger LOG = LoggerFactory.getLogger(MemoryProtection.class);
  private static final Runtime RUNTIME = Runtime.getRuntime();
  private static final AtomicLong FREE_MEMORY = new AtomicLong(RUNTIME.freeMemory());
  private static final FreeMemoryUpdater UPDATER = new FreeMemoryUpdater();

  private static final int MB = 1048576;
  private static final int MB10 = 10 * MB;
  private static final int MB50 = 50 * MB;
  private static final int MB100 = 100 * MB;
  private static final int MB512 = 512 * MB;
  private static final int GB1 = 1024 * MB;
  private static final int FREE_MEMORY_BUFFER = 24 * MB;

  private static boolean IS_ENABLED = false;
  private static int SIZE_THRESHOLD = (int) (RUNTIME.maxMemory() * 0.05);

  static {
    IS_ENABLED = Boolean.parseBoolean(System.getProperty(ENABLED_PROPERTY, "false"));
    if (IS_ENABLED) {
      try {
        SIZE_THRESHOLD = Integer.parseInt(
            System.getProperty(SIZE_THRESHOLD_PROPERTY, Integer.toString(SIZE_THRESHOLD)));
      } catch (NumberFormatException e) {
        LOG.warn("{} system property value is not a valid Integer: {}", SIZE_THRESHOLD_PROPERTY,
            SIZE_THRESHOLD);
      }
      LOG.info("Enabled for Value sizes over {}", SIZE_THRESHOLD);
      List<GarbageCollectorMXBean> gcMBeans = ManagementFactory.getGarbageCollectorMXBeans();
      NotificationListener listener = new MemoryProtection();
      gcMBeans
          .forEach(mb -> ((NotificationEmitter) mb).addNotificationListener(listener, null, null));
      Threads.createThread("MemoryProtectionThread", UPDATER).start();
    }
  }

  private MemoryProtection() {}

  // private static double getBufferSize(int size) {
  // if (size < MB10) {
  // return 2.0;
  // } else if (size < MB50) {
  // return 1.5;
  // } else if (size < MB100) {
  // return 1.25;
  // } else if (size < MB512) {
  // return 1.10;
  // } else if (size < GB1) {
  // return 1.02;
  // } else {
  // return 1.01;
  // }
  // }

  private static long calculateFreeMemory() {
    long maxConfiguredMemory = RUNTIME.maxMemory();
    long allocatedMemory = RUNTIME.totalMemory();
    long allocatedFreeMemory = RUNTIME.freeMemory();
    return (maxConfiguredMemory - (allocatedMemory - allocatedFreeMemory));
  }

  public static <R> void protect(int size, ProtectedAction action) throws IOException {
    if (IS_ENABLED) {
      final long threshold = (long) ((size /* * getBufferSize(size) */) + FREE_MEMORY_BUFFER);
      while (FREE_MEMORY.get() < threshold /* && calculateFreeMemory() < threshold */) {
        LOG.info("Spinning, current: {}, threshold: {}", FREE_MEMORY.get(), threshold);
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          Thread.interrupted();
          throw new IOException("thread interrupted", e);
        }
      }
      LOG.info("Free Memory larger than {}", threshold);
    }
    action.execute();
  }

  @Override
  public void handleNotification(Notification n, Object callbackObject) {
    // TODO: Replace "com.sun.management.gc.notification" with reference to
    // GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION. Need to figure
    // out how to get jdk.management module enabled in IDE and for compilation
    if (n.getType().equals("com.sun.management.gc.notification")) {
      // We just got a notification that a GC completed in one of the GC memory pools. Queue up
      // an action to determine amount of free memory
      UPDATER.update();
    }
  }

}
