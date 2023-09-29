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
package org.apache.accumulo.tserver.tablet;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.NamespaceConfiguration;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

public class TabletTransactionLogTest {

  private static TestTable FOO = new TestTable("Foo", TableId.of("1"));
  private static KeyExtent FOO_EXTENT = new KeyExtent(FOO.getId(), new Text("2"), new Text("1"));

  private ServerConfigurationFactory factory;

  @BeforeEach
  public void init() {
    ServerContext context = createMockContext();
    replay(context);
    factory = new TestServerConfigurationFactory(context);
    initFactory(factory);
  }

  private TabletTransactionLog createLog(Set<StoredTabletFile> initialFiles) {
    return createLog(initialFiles, 0);
  }

  private TabletTransactionLog createLog(final Set<StoredTabletFile> initialFiles,
      final int maxSize) {
    enableLog(true);
    setMaxSize(maxSize);
    TabletTransactionLog log = new TabletTransactionLog(FOO_EXTENT, initialFiles,
        factory.getTableConfiguration(FOO.getId()));
    assertEquals(initialFiles, log.getExpectedFiles());
    return log;
  }

  private void enableLog(boolean enabled) {
    ((ConfigurationCopy) (factory.getSystemConfiguration()))
        .set(Property.TABLE_TRANSACTION_LOG_ENABLED, Boolean.toString(enabled));
  }

  private void setMaxSize(int maxSize) {
    ((ConfigurationCopy) (factory.getSystemConfiguration()))
        .set(Property.TABLE_TRANSACTION_LOG_MAX_SIZE, Integer.toString(maxSize));
  }

  @Test
  public void testFlushed() throws InterruptedException {
    StoredTabletFile initialFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile1.rf");
    Set<StoredTabletFile> initialFiles = Sets.newHashSet(initialFile);
    TabletTransactionLog log = createLog(initialFiles);
    String dump = log.dumpLog();
    long logDate = log.getInitialDate().getTime();

    Thread.sleep(2); // to insure the time stamp is different
    log.flushed(Optional.empty(), initialFiles);
    assertEquals(initialFiles, log.getInitialFiles());
    assertEquals(initialFiles, log.getExpectedFiles());
    assertEquals(0, log.getTransactions().size());
    assertTrue(log.getInitialDate().getTime() > logDate);
    logDate = log.getInitialDate().getTime();
    assertNotEquals(dump, log.dumpLog());
    dump = log.dumpLog();

    Thread.sleep(2); // to insure the time stamp is different
    StoredTabletFile flushedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ffile1.rf");
    Set<StoredTabletFile> expectedFiles = Sets.newHashSet(initialFile, flushedFile);
    log.flushed(Optional.of(flushedFile), expectedFiles);
    assertEquals(expectedFiles, log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());
    assertEquals(0, log.getTransactions().size());
    assertTrue(log.getInitialDate().getTime() > logDate);
    assertNotEquals(dump, log.dumpLog());
  }

  @Test
  public void testBulkImport() throws InterruptedException {
    StoredTabletFile initialFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile1.rf");
    Set<StoredTabletFile> initialFiles = Sets.newHashSet(initialFile);
    TabletTransactionLog log = createLog(initialFiles);
    String dump = log.dumpLog();
    long logDate = log.getInitialDate().getTime();

    Thread.sleep(2); // to insure the time stamp is different
    StoredTabletFile importedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ifile1.rf");
    Set<StoredTabletFile> expectedFiles = Sets.newHashSet(initialFile, importedFile);
    log.bulkImported(importedFile, expectedFiles);
    assertEquals(expectedFiles, log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());
    assertEquals(0, log.getTransactions().size());
    assertTrue(log.getInitialDate().getTime() > logDate);
    assertNotEquals(dump, log.dumpLog());
  }

  @Test
  public void testCompacted() throws InterruptedException {
    StoredTabletFile initialFile1 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile1.rf");
    StoredTabletFile initialFile2 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile2.rf");
    StoredTabletFile initialFile3 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile3.rf");
    Set<StoredTabletFile> initialFiles = Sets.newHashSet(initialFile1, initialFile2, initialFile3);
    TabletTransactionLog log = createLog(initialFiles);
    String dump = log.dumpLog();
    long logDate = log.getInitialDate().getTime();

    Thread.sleep(2); // to insure the time stamp is different
    StoredTabletFile compactedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Cfile1.rf");
    Set<StoredTabletFile> expectedFiles = Sets.newHashSet(initialFile3, compactedFile);
    log.compacted(Sets.newHashSet(initialFile1, initialFile2), Optional.of(compactedFile),
        expectedFiles);
    assertEquals(expectedFiles, log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());
    assertEquals(0, log.getTransactions().size());
    assertTrue(log.getInitialDate().getTime() > logDate);
    assertNotEquals(dump, log.dumpLog());
    dump = log.dumpLog();
    logDate = log.getInitialDate().getTime();

    Thread.sleep(2);
    expectedFiles = Sets.newHashSet(initialFile3);
    log.compacted(Sets.newHashSet(compactedFile), Optional.empty(), expectedFiles);
    assertEquals(expectedFiles, log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());
    assertEquals(0, log.getTransactions().size());
    assertTrue(log.getInitialDate().getTime() > logDate);
    assertNotEquals(dump, log.dumpLog());
  }

  @Test
  public void testNonEmptyLog() throws InterruptedException {
    StoredTabletFile initialFile1 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile1.rf");
    StoredTabletFile initialFile2 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile2.rf");
    StoredTabletFile initialFile3 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile3.rf");
    Set<StoredTabletFile> initialFiles = Sets.newHashSet(initialFile1, initialFile2, initialFile3);
    TabletTransactionLog log = createLog(initialFiles, 3);
    String dump = log.dumpLog();
    long logDate = log.getInitialDate().getTime();

    Thread.sleep(2); // to insure the time stamp is different
    StoredTabletFile importedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ifile1.rf");
    Set<StoredTabletFile> expectedFiles =
        Sets.newHashSet(initialFile1, initialFile2, initialFile3, importedFile);
    log.bulkImported(importedFile, expectedFiles);
    assertEquals(initialFiles, log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());
    List<TabletTransaction> logs = log.getTransactions();
    assertEquals(1, logs.size());
    assertEquals(logDate, log.getInitialDate().getTime());
    assertNotEquals(dump, log.dumpLog());
    dump = log.dumpLog();

    Thread.sleep(2); // to insure the time stamp is different
    StoredTabletFile compactedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Cfile1.rf");
    expectedFiles = Sets.newHashSet(initialFile1, initialFile2, compactedFile);
    log.compacted(Sets.newHashSet(initialFile3, importedFile), Optional.of(compactedFile),
        expectedFiles);
    assertEquals(initialFiles, log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());
    assertEquals(logDate, log.getInitialDate().getTime());
    logs = log.getTransactions();
    assertEquals(2, logs.size());
    assertNotEquals(dump, log.dumpLog());
    dump = log.dumpLog();

    Thread.sleep(2); // to insure the time stamp is different
    expectedFiles = Sets.newHashSet(initialFile1, initialFile2);
    log.compacted(Sets.newHashSet(compactedFile), Optional.empty(), expectedFiles);
    assertEquals(initialFiles, log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());
    assertEquals(logDate, log.getInitialDate().getTime());
    logs = log.getTransactions();
    assertEquals(3, logs.size());
    assertNotEquals(dump, log.dumpLog());
    dump = log.dumpLog();

    assertTrue(logs.get(0) instanceof TabletTransaction.BulkImported);
    assertEquals(importedFile, ((TabletTransaction.BulkImported) logs.get(0)).getImportFile());
    assertTrue(logs.get(1) instanceof TabletTransaction.Compacted);
    assertEquals(Sets.newHashSet(initialFile3, importedFile),
        ((TabletTransaction.Compacted) logs.get(1)).getCompactedFiles());
    assertEquals(Optional.of(compactedFile),
        ((TabletTransaction.Compacted) logs.get(1)).getDestination());
    assertTrue(logs.get(2) instanceof TabletTransaction.Compacted);
    assertEquals(Sets.newHashSet(compactedFile),
        ((TabletTransaction.Compacted) logs.get(2)).getCompactedFiles());
    assertEquals(Optional.empty(), ((TabletTransaction.Compacted) logs.get(2)).getDestination());

    Thread.sleep(2); // to insure the time stamp is different
    StoredTabletFile flushedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ffile1.rf");
    expectedFiles = Sets.newHashSet(initialFile1, initialFile2, flushedFile);
    log.flushed(Optional.of(flushedFile), expectedFiles);
    assertEquals(Sets.newHashSet(initialFile1, initialFile2, initialFile3, importedFile),
        log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());
    assertEquals(logs.get(0).ts, log.getInitialDate().getTime());
    logs = log.getTransactions();
    assertEquals(3, logs.size());
    assertNotEquals(dump, log.dumpLog());
    dump = log.dumpLog();

    assertTrue(logs.get(0) instanceof TabletTransaction.Compacted);
    assertEquals(Sets.newHashSet(initialFile3, importedFile),
        ((TabletTransaction.Compacted) logs.get(0)).getCompactedFiles());
    assertEquals(Optional.of(compactedFile),
        ((TabletTransaction.Compacted) logs.get(0)).getDestination());
    assertTrue(logs.get(1) instanceof TabletTransaction.Compacted);
    assertEquals(Sets.newHashSet(compactedFile),
        ((TabletTransaction.Compacted) logs.get(1)).getCompactedFiles());
    assertEquals(Optional.empty(), ((TabletTransaction.Compacted) logs.get(1)).getDestination());
    assertTrue(logs.get(2) instanceof TabletTransaction.Flushed);
    assertEquals(Optional.of(flushedFile),
        ((TabletTransaction.Flushed) logs.get(2)).getFlushFile());

    Thread.sleep(2); // to insure the time stamp is different
    setMaxSize(1);
    expectedFiles = Sets.newHashSet(initialFile1, initialFile2, flushedFile);
    log.flushed(Optional.empty(), expectedFiles);
    assertEquals(Sets.newHashSet(initialFile1, initialFile2, flushedFile), log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());
    // we went from 3 to 1 logs, so the first one should not be the last of the original logs
    assertEquals(logs.get(2).ts, log.getInitialDate().getTime());
    logs = log.getTransactions();
    assertEquals(1, logs.size());
    assertNotEquals(dump, log.dumpLog());

    assertTrue(logs.get(0) instanceof TabletTransaction.Flushed);
    assertEquals(Optional.empty(), ((TabletTransaction.Flushed) logs.get(0)).getFlushFile());
  }

  @Test
  public void testEnableDisable() throws InterruptedException {
    StoredTabletFile initialFile1 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile1.rf");
    StoredTabletFile initialFile2 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile2.rf");
    StoredTabletFile initialFile3 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile3.rf");
    Set<StoredTabletFile> initialFiles = Sets.newHashSet(initialFile1, initialFile2, initialFile3);
    TabletTransactionLog log = createLog(initialFiles, 3);

    StoredTabletFile importedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ifile1.rf");
    Set<StoredTabletFile> expectedFiles =
        Sets.newHashSet(initialFile1, initialFile2, initialFile3, importedFile);
    log.bulkImported(importedFile, expectedFiles);
    StoredTabletFile compactedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Cfile1.rf");
    expectedFiles = Sets.newHashSet(initialFile1, initialFile2, compactedFile);
    log.compacted(Sets.newHashSet(initialFile3, importedFile), Optional.of(compactedFile),
        expectedFiles);
    expectedFiles = Sets.newHashSet(initialFile1, initialFile2);
    log.compacted(Sets.newHashSet(compactedFile), Optional.empty(), expectedFiles);

    List<TabletTransaction> logs = log.getTransactions();
    assertEquals(3, logs.size());

    // now disable the log
    enableLog(false);

    // still has transactions until a new one is added
    logs = log.getTransactions();
    assertEquals(3, logs.size());

    // another transaction
    StoredTabletFile flushedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ffile1.rf");
    expectedFiles = Sets.newHashSet(initialFile1, initialFile2, flushedFile);
    log.flushed(Optional.of(flushedFile), expectedFiles);

    // no more transactions
    logs = log.getTransactions();
    assertEquals(0, logs.size());

    // add a transaction
    expectedFiles = Sets.newHashSet(initialFile1, initialFile2, flushedFile);
    log.flushed(Optional.empty(), expectedFiles);

    // still no more transactions
    logs = log.getTransactions();
    assertEquals(0, logs.size());

    // reenable the log
    enableLog(true);

    // add a transaction
    expectedFiles = Sets.newHashSet(initialFile1, initialFile2, flushedFile);
    log.flushed(Optional.empty(), expectedFiles);

    // Now we have a transaction
    logs = log.getTransactions();
    assertEquals(1, logs.size());
  }

  @Test
  public void testCapacityChange() {
    StoredTabletFile initialFile1 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile1.rf");
    StoredTabletFile initialFile2 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile2.rf");
    StoredTabletFile initialFile3 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile3.rf");
    Set<StoredTabletFile> initialFiles = Sets.newHashSet(initialFile1, initialFile2, initialFile3);
    TabletTransactionLog log = createLog(initialFiles, 3);
    StoredTabletFile importedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ifile1.rf");
    Set<StoredTabletFile> expectedFiles =
        Sets.newHashSet(initialFile1, initialFile2, initialFile3, importedFile);
    log.bulkImported(importedFile, expectedFiles);
    StoredTabletFile compactedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Cfile1.rf");
    expectedFiles = Sets.newHashSet(initialFile1, initialFile2, compactedFile);
    log.compacted(Sets.newHashSet(initialFile3, importedFile), Optional.of(compactedFile),
        expectedFiles);
    expectedFiles = Sets.newHashSet(initialFile1, initialFile2);
    log.compacted(Sets.newHashSet(compactedFile), Optional.empty(), expectedFiles);
    StoredTabletFile flushedFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ffile1.rf");
    expectedFiles = Sets.newHashSet(initialFile1, initialFile2, flushedFile);
    log.flushed(Optional.of(flushedFile), expectedFiles);

    List<TabletTransaction> logs = log.getTransactions();
    assertEquals(3, logs.size());
    assertTrue(logs.get(0) instanceof TabletTransaction.Compacted);
    assertEquals(Sets.newHashSet(initialFile3, importedFile),
        ((TabletTransaction.Compacted) logs.get(0)).getCompactedFiles());
    assertEquals(Optional.of(compactedFile),
        ((TabletTransaction.Compacted) logs.get(0)).getDestination());
    assertTrue(logs.get(1) instanceof TabletTransaction.Compacted);
    assertEquals(Sets.newHashSet(compactedFile),
        ((TabletTransaction.Compacted) logs.get(1)).getCompactedFiles());
    assertEquals(Optional.empty(), ((TabletTransaction.Compacted) logs.get(1)).getDestination());
    assertTrue(logs.get(2) instanceof TabletTransaction.Flushed);
    assertEquals(Optional.of(flushedFile),
        ((TabletTransaction.Flushed) logs.get(2)).getFlushFile());
    assertEquals(Sets.newHashSet(initialFile1, initialFile2, initialFile3, importedFile),
        log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());

    // change the capacity
    setMaxSize(5);
    logs = log.getTransactions();
    assertEquals(3, logs.size());
    assertTrue(logs.get(0) instanceof TabletTransaction.Compacted);
    assertEquals(Sets.newHashSet(initialFile3, importedFile),
        ((TabletTransaction.Compacted) logs.get(0)).getCompactedFiles());
    assertEquals(Optional.of(compactedFile),
        ((TabletTransaction.Compacted) logs.get(0)).getDestination());
    assertTrue(logs.get(1) instanceof TabletTransaction.Compacted);
    assertEquals(Sets.newHashSet(compactedFile),
        ((TabletTransaction.Compacted) logs.get(1)).getCompactedFiles());
    assertEquals(Optional.empty(), ((TabletTransaction.Compacted) logs.get(1)).getDestination());
    assertTrue(logs.get(2) instanceof TabletTransaction.Flushed);
    assertEquals(Optional.of(flushedFile),
        ((TabletTransaction.Flushed) logs.get(2)).getFlushFile());
    assertEquals(Sets.newHashSet(initialFile1, initialFile2, initialFile3, importedFile),
        log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());

    // add another transaction
    StoredTabletFile flushedFile2 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ffile2.rf");
    expectedFiles = Sets.newHashSet(initialFile1, initialFile2, flushedFile, flushedFile2);
    log.flushed(Optional.of(flushedFile2), expectedFiles);

    logs = log.getTransactions();
    assertEquals(4, logs.size());
    assertTrue(logs.get(0) instanceof TabletTransaction.Compacted);
    assertEquals(Sets.newHashSet(initialFile3, importedFile),
        ((TabletTransaction.Compacted) logs.get(0)).getCompactedFiles());
    assertEquals(Optional.of(compactedFile),
        ((TabletTransaction.Compacted) logs.get(0)).getDestination());
    assertTrue(logs.get(1) instanceof TabletTransaction.Compacted);
    assertEquals(Sets.newHashSet(compactedFile),
        ((TabletTransaction.Compacted) logs.get(1)).getCompactedFiles());
    assertEquals(Optional.empty(), ((TabletTransaction.Compacted) logs.get(1)).getDestination());
    assertTrue(logs.get(2) instanceof TabletTransaction.Flushed);
    assertEquals(Optional.of(flushedFile),
        ((TabletTransaction.Flushed) logs.get(2)).getFlushFile());
    assertTrue(logs.get(3) instanceof TabletTransaction.Flushed);
    assertEquals(Optional.of(flushedFile2),
        ((TabletTransaction.Flushed) logs.get(3)).getFlushFile());
    assertEquals(Sets.newHashSet(initialFile1, initialFile2, initialFile3, importedFile),
        log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());

    // change the capacity to be smaller
    setMaxSize(2);

    // size won't actually change until we add another transaction
    logs = log.getTransactions();
    assertEquals(4, logs.size());
    assertEquals(Sets.newHashSet(initialFile1, initialFile2, initialFile3, importedFile),
        log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());

    // add another transaction
    StoredTabletFile flushedFile3 =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ffile3.rf");
    expectedFiles =
        Sets.newHashSet(initialFile1, initialFile2, flushedFile, flushedFile2, flushedFile3);
    log.flushed(Optional.of(flushedFile3), expectedFiles);

    // ensure we are now a reduced size
    logs = log.getTransactions();
    assertEquals(2, logs.size());
    assertTrue(logs.get(0) instanceof TabletTransaction.Flushed);
    assertEquals(Optional.of(flushedFile2),
        ((TabletTransaction.Flushed) logs.get(0)).getFlushFile());
    assertTrue(logs.get(1) instanceof TabletTransaction.Flushed);
    assertEquals(Optional.of(flushedFile3),
        ((TabletTransaction.Flushed) logs.get(1)).getFlushFile());
    assertEquals(Sets.newHashSet(initialFile1, initialFile2, flushedFile), log.getInitialFiles());
    assertEquals(expectedFiles, log.getExpectedFiles());
  }

  private static class ExceptionHandler implements Thread.UncaughtExceptionHandler {
    public Throwable thrown = null;

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      synchronized (this) {
        thrown = e;
      }
    }
  }

  @Test
  public void testThreadSafety() throws InterruptedException {
    StoredTabletFile initialFile =
        new StoredTabletFile("file://accumulo/tables/1/default_tablet/Afile1.rf");
    Set<StoredTabletFile> initialFiles = Sets.newHashSet(initialFile);
    final TabletTransactionLog log = createLog(initialFiles, 5);
    final ExceptionHandler handler = new ExceptionHandler();
    final Object startLock = new Object();
    final AtomicInteger ready = new AtomicInteger(0);
    final AtomicInteger writes = new AtomicInteger(0);
    final AtomicInteger reads = new AtomicInteger(0);
    final SecureRandom random = new SecureRandom();
    final int STARTUP_WAIT = 15000;
    final int EXECUTION_TIME = 2000;

    // create writer threads
    // Note that the write methods are being done randomly hence the
    // expected new file list will most certainly be different each time
    // resulting in much logging. This is ok.
    Runnable writable = new Runnable() {
      @Override
      public void run() {
        synchronized (startLock) {
          try {
            ready.incrementAndGet();
            startLock.wait(STARTUP_WAIT);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        long start = System.currentTimeMillis();
        StoredTabletFile importedFile =
            new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ifile1.rf");
        StoredTabletFile flushedFile =
            new StoredTabletFile("file://accumulo/tables/1/default_tablet/Ffile1.rf");
        StoredTabletFile compactedFile =
            new StoredTabletFile("file://accumulo/tables/1/default_tablet/Cfile1.rf");
        while (System.currentTimeMillis() - start < EXECUTION_TIME) {
          enableLog(random.nextBoolean());
          setMaxSize(random.nextInt(10));
          int choice = random.nextInt(4);
          switch (choice) {
            case 0:
              log.bulkImported(importedFile, Sets.newHashSet(importedFile));
              break;
            case 1:
              log.flushed(Optional.of(flushedFile), Sets.newHashSet(flushedFile));
              break;
            case 2:
              log.compacted(Sets.newHashSet(importedFile, flushedFile), Optional.of(compactedFile),
                  Sets.newHashSet(compactedFile));
              break;
            default:
              log.clearLog();
          }
          writes.incrementAndGet();
        }
      }
    };

    // create a writer threads
    Runnable readable = new Runnable() {
      @Override
      public void run() {
        synchronized (startLock) {
          try {
            ready.incrementAndGet();
            startLock.wait(STARTUP_WAIT);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < EXECUTION_TIME) {
          log.getTransactions();
          reads.incrementAndGet();
        }
      }
    };

    // create the threads
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      Thread readerThread = new Thread(readable);
      readerThread.setUncaughtExceptionHandler(handler);
      threads.add(readerThread);
      Thread writerThread = new Thread(writable);
      writerThread.setUncaughtExceptionHandler(handler);
      threads.add(writerThread);
    }

    // start the threads
    threads.stream().forEach(t -> t.start());

    // wait until they are both waiting on the start lock
    while (ready.get() < 10) {
      Thread.sleep(100);
    }

    // unlock the threads
    synchronized (startLock) {
      ready.incrementAndGet();
      startLock.notifyAll();
    }

    // wait for the threads to complete
    boolean alive = threads.stream().anyMatch(t -> t.isAlive());
    while (alive) {
      Thread.sleep(100);
      alive = threads.stream().anyMatch(t -> t.isAlive());
    }

    // ensure no exceptions were thrown
    assertNull(handler.thrown, "Exception was thrown: " + handler.thrown);

    // ensure we got some writes and some reads in there
    assertTrue(writes.get() > 0);
    assertTrue(reads.get() > 0);
  }

  protected ServerContext createMockContext() {
    InstanceId instanceId = InstanceId.of(UUID.randomUUID());
    ServerContext mockContext = createMock(ServerContext.class);
    PropStore propStore = createMock(ZooPropStore.class);
    expect(mockContext.getProperties()).andReturn(new Properties()).anyTimes();
    expect(mockContext.getZooKeepers()).andReturn("").anyTimes();
    expect(mockContext.getInstanceName()).andReturn("test").anyTimes();
    expect(mockContext.getZooKeepersSessionTimeOut()).andReturn(30).anyTimes();
    expect(mockContext.getInstanceID()).andReturn(instanceId).anyTimes();
    expect(mockContext.getZooKeeperRoot()).andReturn(Constants.ZROOT + "/1111").anyTimes();

    expect(mockContext.getPropStore()).andReturn(propStore).anyTimes();
    propStore.registerAsListener(anyObject(), anyObject());
    expectLastCall().anyTimes();

    expect(propStore.get(eq(NamespacePropKey.of(instanceId, NamespaceId.of("+default")))))
        .andReturn(new VersionedProperties()).anyTimes();

    expect(propStore.get(eq(TablePropKey.of(instanceId, TableId.of("1")))))
        .andReturn(new VersionedProperties()).anyTimes();

    replay(propStore);
    return mockContext;
  }

  private void initFactory(ServerConfigurationFactory factory) {
    ServerContext context = createMockContext();
    expect(context.getConfiguration()).andReturn(factory.getSystemConfiguration()).anyTimes();
    expect(context.getTableConfiguration(FOO.getId()))
        .andReturn(factory.getTableConfiguration(FOO.getId())).anyTimes();
    replay(context);
  }

  protected static class TestTable {
    private String tableName;
    private TableId id;

    TestTable(String tableName, TableId id) {
      this.tableName = tableName;
      this.id = id;
    }

    public String getTableName() {
      return tableName;
    }

    public TableId getId() {
      return id;
    }
  }

  protected static final HashMap<String,String> DEFAULT_TABLE_PROPERTIES = new HashMap<>();
  {
    DEFAULT_TABLE_PROPERTIES.put(Property.TABLE_TRANSACTION_LOG_MAX_SIZE.getKey(), "0");
  }

  private static SiteConfiguration siteConfg = SiteConfiguration.empty().build();

  protected static class TestServerConfigurationFactory extends ServerConfigurationFactory {

    private final ServerContext context;
    protected final ConfigurationCopy config;

    public TestServerConfigurationFactory(ServerContext context) {
      super(context, siteConfg);
      this.context = context;
      this.config = new ConfigurationCopy(DEFAULT_TABLE_PROPERTIES);
    }

    @Override
    public synchronized AccumuloConfiguration getSystemConfiguration() {
      return config;
    }

    @Override
    public TableConfiguration getTableConfiguration(final TableId tableId) {
      // create a dummy namespaceConfiguration to satisfy requireNonNull in TableConfiguration
      // constructor
      NamespaceConfiguration dummyConf = new NamespaceConfiguration(context, Namespace.DEFAULT.id(),
          DefaultConfiguration.getInstance());
      return new TableConfiguration(context, tableId, dummyConf) {
        @Override
        public String get(Property property) {
          return getSystemConfiguration().get(property.getKey());
        }

        @Override
        public void getProperties(Map<String,String> props, Predicate<String> filter) {
          getSystemConfiguration().getProperties(props, filter);
        }

        @Override
        public long getUpdateCount() {
          return getSystemConfiguration().getUpdateCount();
        }
      };
    }
  }

}
