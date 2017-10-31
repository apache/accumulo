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
package org.apache.accumulo.tserver.log;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationConfigurationUtil;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.fate.util.LoggingRunnable;
import org.apache.accumulo.fate.zookeeper.Retry;
import org.apache.accumulo.fate.zookeeper.RetryFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.replication.StatusUtil;
import org.apache.accumulo.server.replication.proto.Replication.Status;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.ReplicationTableUtil;
import org.apache.accumulo.tserver.Mutations;
import org.apache.accumulo.tserver.TabletMutations;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.log.DfsLogger.LoggerOperation;
import org.apache.accumulo.tserver.log.DfsLogger.ServerResources;
import org.apache.accumulo.tserver.tablet.CommitSession;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Central logging facility for the TServerInfo.
 *
 * Forwards in-memory updates to remote logs, carefully writing the same data to every log, while maintaining the maximum thread parallelism for greater
 * performance. As new logs are used and minor compactions are performed, the metadata table is kept up-to-date.
 *
 */
public class TabletServerLogger {

  private static final Logger log = LoggerFactory.getLogger(TabletServerLogger.class);

  private final AtomicLong logSizeEstimate = new AtomicLong();
  private final long maxSize;
  private final long maxAge;

  private final TabletServer tserver;

  // The current logger
  private DfsLogger currentLog = null;
  private final SynchronousQueue<Object> nextLog = new SynchronousQueue<>();
  private ThreadPoolExecutor nextLogMaker;

  // The current generation of logs.
  // Because multiple threads can be using a log at one time, a log
  // failure is likely to affect multiple threads, who will all attempt to
  // create a new log. This will cause many unnecessary updates to the
  // metadata table.
  // We'll use this generational counter to determine if another thread has
  // already fetched a new log.
  private final AtomicInteger logId = new AtomicInteger();

  // Use a ReadWriteLock to allow multiple threads to use the log set, but obtain a write lock to change them
  private final ReentrantReadWriteLock logIdLock = new ReentrantReadWriteLock();

  private final AtomicInteger seqGen = new AtomicInteger();

  private final AtomicLong syncCounter;
  private final AtomicLong flushCounter;

  private long createTime = 0;

  private final RetryFactory retryFactory;
  private Retry retry = null;

  static private abstract class TestCallWithWriteLock {
    abstract boolean test();

    abstract void withWriteLock() throws IOException;
  }

  /**
   * Pattern taken from the documentation for ReentrantReadWriteLock
   *
   * @param rwlock
   *          lock to use
   * @param code
   *          a test/work pair
   */
  private static void testLockAndRun(final ReadWriteLock rwlock, TestCallWithWriteLock code) throws IOException {
    // Get a read lock
    rwlock.readLock().lock();
    try {
      // does some condition exist that needs the write lock?
      if (code.test()) {
        // Yes, let go of the readlock
        rwlock.readLock().unlock();
        // Grab the write lock
        rwlock.writeLock().lock();
        try {
          // double-check the condition, since we let go of the lock
          if (code.test()) {
            // perform the work with with write lock held
            code.withWriteLock();
          }
        } finally {
          // regain the readlock
          rwlock.readLock().lock();
          // unlock the write lock
          rwlock.writeLock().unlock();
        }
      }
    } finally {
      // always let go of the lock
      rwlock.readLock().unlock();
    }
  }

  public TabletServerLogger(TabletServer tserver, long maxSize, AtomicLong syncCounter, AtomicLong flushCounter, RetryFactory retryFactory, long maxAge) {
    this.tserver = tserver;
    this.maxSize = maxSize;
    this.syncCounter = syncCounter;
    this.flushCounter = flushCounter;
    this.retryFactory = retryFactory;
    this.retry = null;
    this.maxAge = maxAge;
  }

  private DfsLogger initializeLoggers(final AtomicInteger logIdOut) throws IOException {
    final AtomicReference<DfsLogger> result = new AtomicReference<>();
    testLockAndRun(logIdLock, new TestCallWithWriteLock() {
      @Override
      boolean test() {
        result.set(currentLog);
        if (currentLog != null)
          logIdOut.set(logId.get());
        return currentLog == null;
      }

      @Override
      void withWriteLock() throws IOException {
        try {
          createLogger();
          result.set(currentLog);
          if (currentLog != null)
            logIdOut.set(logId.get());
          else
            logIdOut.set(-1);
        } catch (IOException e) {
          log.error("Unable to create loggers", e);
        }
      }
    });
    return result.get();
  }

  /**
   * Get the current WAL file
   *
   * @return The name of the current log, or null if there is no current log.
   */
  public String getLogFile() {
    logIdLock.readLock().lock();
    try {
      if (null == currentLog) {
        return null;
      }
      return currentLog.getFileName();
    } finally {
      logIdLock.readLock().unlock();
    }
  }

  synchronized private void createLogger() throws IOException {
    if (!logIdLock.isWriteLockedByCurrentThread()) {
      throw new IllegalStateException("createLoggers should be called with write lock held!");
    }

    if (currentLog != null) {
      throw new IllegalStateException("createLoggers should not be called when current log is set");
    }

    try {
      startLogMaker();
      Object next = nextLog.take();
      if (next instanceof Exception) {
        throw (Exception) next;
      }
      if (next instanceof DfsLogger) {
        currentLog = (DfsLogger) next;
        logId.incrementAndGet();
        log.info("Using next log {}", currentLog.getFileName());

        // When we successfully create a WAL, make sure to reset the Retry.
        if (null != retry) {
          retry = null;
        }

        this.createTime = System.currentTimeMillis();
        return;
      } else {
        throw new RuntimeException("Error: unexpected type seen: " + next);
      }
    } catch (Exception t) {
      if (null == retry) {
        retry = retryFactory.create();
      }

      // We have more retries or we exceeded the maximum number of accepted failures
      if (retry.canRetry()) {
        // Use the retry and record the time in which we did so
        retry.useRetry();

        try {
          // Backoff
          retry.waitForNextAttempt();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      } else {
        log.error("Repeatedly failed to create WAL. Going to exit tabletserver.", t);
        // We didn't have retries or we failed too many times.
        Halt.halt("Experienced too many errors creating WALs, giving up", 1);
      }

      // The exception will trigger the log creation to be re-attempted.
      throw new RuntimeException(t);
    }
  }

  private synchronized void startLogMaker() {
    if (nextLogMaker != null) {
      return;
    }
    nextLogMaker = new SimpleThreadPool(1, "WALog creator");
    nextLogMaker.submit(new LoggingRunnable(log, new Runnable() {
      @Override
      public void run() {
        final ServerResources conf = tserver.getServerConfig();
        final VolumeManager fs = conf.getFileSystem();
        while (!nextLogMaker.isShutdown()) {
          DfsLogger alog = null;
          try {
            log.debug("Creating next WAL");
            alog = new DfsLogger(conf, syncCounter, flushCounter);
            alog.open(tserver.getClientAddressString());
            String fileName = alog.getFileName();
            log.debug("Created next WAL " + fileName);
            tserver.addNewLogMarker(alog);
            while (!nextLog.offer(alog, 12, TimeUnit.HOURS)) {
              log.info("Our WAL was not used for 12 hours: {}", fileName);
            }
          } catch (Exception t) {
            log.error("Failed to open WAL", t);
            if (null != alog) {
              // It's possible that the sync of the header and OPEN record to the WAL failed
              // We want to make sure that clean up the resources/thread inside the DfsLogger
              // object before trying to create a new one.
              try {
                alog.close();
              } catch (Exception e) {
                log.error("Failed to close WAL after it failed to open", e);
              }
              // Try to avoid leaving a bunch of empty WALs lying around
              try {
                Path path = alog.getPath();
                if (fs.exists(path)) {
                  fs.delete(path);
                }
              } catch (Exception e) {
                log.warn("Failed to delete a WAL that failed to open", e);
              }
            }
            try {
              nextLog.offer(t, 12, TimeUnit.HOURS);
            } catch (InterruptedException ex) {
              // ignore
            }
          }
        }
      }
    }));
  }

  public void resetLoggers() throws IOException {
    logIdLock.writeLock().lock();
    try {
      close();
    } finally {
      logIdLock.writeLock().unlock();
    }
  }

  synchronized private void close() throws IOException {
    if (!logIdLock.isWriteLockedByCurrentThread()) {
      throw new IllegalStateException("close should be called with write lock held!");
    }
    try {
      if (null != currentLog) {
        try {
          currentLog.close();
        } catch (DfsLogger.LogClosedException ex) {
          // ignore
        } catch (Throwable ex) {
          log.error("Unable to cleanly close log " + currentLog.getFileName() + ": " + ex, ex);
        } finally {
          this.tserver.walogClosed(currentLog);
        }
        currentLog = null;
        logSizeEstimate.set(0);
      }
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  interface Writer {
    LoggerOperation write(DfsLogger logger, int seq) throws Exception;
  }

  private int write(CommitSession commitSession, boolean mincFinish, Writer writer) throws IOException {
    List<CommitSession> sessions = Collections.singletonList(commitSession);
    return write(sessions, mincFinish, writer);
  }

  private int write(final Collection<CommitSession> sessions, boolean mincFinish, Writer writer) throws IOException {
    // Work very hard not to lock this during calls to the outside world
    int currentLogId = logId.get();

    int seq = -1;
    int attempt = 1;
    boolean success = false;
    while (!success) {
      try {
        // get a reference to the loggers that no other thread can touch
        DfsLogger copy = null;
        AtomicInteger currentId = new AtomicInteger(-1);
        copy = initializeLoggers(currentId);
        currentLogId = currentId.get();

        // add the logger to the log set for the memory in the tablet,
        // update the metadata table if we've never used this tablet

        if (currentLogId == logId.get()) {
          for (CommitSession commitSession : sessions) {
            if (commitSession.beginUpdatingLogsUsed(copy, mincFinish)) {
              try {
                // Scribble out a tablet definition and then write to the metadata table
                defineTablet(commitSession);
              } finally {
                commitSession.finishUpdatingLogsUsed();
              }

              // Need to release
              KeyExtent extent = commitSession.getExtent();
              if (ReplicationConfigurationUtil.isEnabled(extent, tserver.getTableConfiguration(extent))) {
                Status status = StatusUtil.openWithUnknownLength(System.currentTimeMillis());
                log.debug("Writing " + ProtobufUtil.toString(status) + " to metadata table for " + copy.getFileName());
                // Got some new WALs, note this in the metadata table
                ReplicationTableUtil.updateFiles(tserver, commitSession.getExtent(), copy.getFileName(), status);
              }
            }
          }
        }

        // Make sure that the logs haven't changed out from underneath our copy
        if (currentLogId == logId.get()) {

          // write the mutation to the logs
          seq = seqGen.incrementAndGet();
          if (seq < 0)
            throw new RuntimeException("Logger sequence generator wrapped!  Onos!!!11!eleven");
          LoggerOperation lop = writer.write(copy, seq);
          lop.await();

          // double-check: did the log set change?
          success = (currentLogId == logId.get());
        }
      } catch (DfsLogger.LogClosedException ex) {
        log.debug("Logs closed while writing, retrying {}", attempt);
      } catch (Exception t) {
        if (attempt != 1) {
          log.error("Unexpected error writing to log, retrying attempt " + attempt, t);
        }
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } finally {
        attempt++;
      }
      // Some sort of write failure occurred. Grab the write lock and reset the logs.
      // But since multiple threads will attempt it, only attempt the reset when
      // the logs haven't changed.
      final int finalCurrent = currentLogId;
      if (!success) {
        testLockAndRun(logIdLock, new TestCallWithWriteLock() {

          @Override
          boolean test() {
            return finalCurrent == logId.get();
          }

          @Override
          void withWriteLock() throws IOException {
            close();
            closeForReplication(sessions);
          }
        });
      }
    }
    // if the log gets too big or too old, reset it .. grab the write lock first
    logSizeEstimate.addAndGet(4 * 3); // event, tid, seq overhead
    testLockAndRun(logIdLock, new TestCallWithWriteLock() {
      @Override
      boolean test() {
        return (logSizeEstimate.get() > maxSize) || ((System.currentTimeMillis() - createTime) > maxAge);
      }

      @Override
      void withWriteLock() throws IOException {
        close();
        closeForReplication(sessions);
      }
    });
    return seq;
  }

  protected void closeForReplication(Collection<CommitSession> sessions) {
    // TODO We can close the WAL here for replication purposes
  }

  public int defineTablet(final CommitSession commitSession) throws IOException {
    // scribble this into the metadata tablet, too.
    return write(commitSession, false, new Writer() {
      @Override
      public LoggerOperation write(DfsLogger logger, int ignored) throws Exception {
        logger.defineTablet(commitSession.getWALogSeq(), commitSession.getLogId(), commitSession.getExtent());
        return DfsLogger.NO_WAIT_LOGGER_OP;
      }
    });
  }

  public int log(final CommitSession commitSession, final int tabletSeq, final Mutation m, final Durability durability) throws IOException {
    if (durability == Durability.NONE) {
      return -1;
    }
    if (durability == Durability.DEFAULT) {
      throw new IllegalArgumentException("Unexpected durability " + durability);
    }
    int seq = write(commitSession, false, new Writer() {
      @Override
      public LoggerOperation write(DfsLogger logger, int ignored) throws Exception {
        return logger.log(tabletSeq, commitSession.getLogId(), m, durability);
      }
    });
    logSizeEstimate.addAndGet(m.numBytes());
    return seq;
  }

  public int logManyTablets(Map<CommitSession,Mutations> mutations) throws IOException {

    final Map<CommitSession,Mutations> loggables = new HashMap<>(mutations);
    for (Entry<CommitSession,Mutations> entry : mutations.entrySet()) {
      if (entry.getValue().getDurability() == Durability.NONE) {
        loggables.remove(entry.getKey());
      }
    }
    if (loggables.size() == 0)
      return -1;

    int seq = write(loggables.keySet(), false, new Writer() {
      @Override
      public LoggerOperation write(DfsLogger logger, int ignored) throws Exception {
        List<TabletMutations> copy = new ArrayList<>(loggables.size());
        for (Entry<CommitSession,Mutations> entry : loggables.entrySet()) {
          CommitSession cs = entry.getKey();
          Durability durability = entry.getValue().getDurability();
          copy.add(new TabletMutations(cs.getLogId(), cs.getWALogSeq(), entry.getValue().getMutations(), durability));
        }
        return logger.logManyTablets(copy);
      }
    });
    for (Mutations entry : loggables.values()) {
      if (entry.getMutations().size() < 1) {
        throw new IllegalArgumentException("logManyTablets: logging empty mutation list");
      }
      for (Mutation m : entry.getMutations()) {
        logSizeEstimate.addAndGet(m.numBytes());
      }
    }
    return seq;
  }

  public void minorCompactionFinished(final CommitSession commitSession, final String fullyQualifiedFileName, final int walogSeq, final Durability durability)
      throws IOException {

    long t1 = System.currentTimeMillis();

    int seq = write(commitSession, true, new Writer() {
      @Override
      public LoggerOperation write(DfsLogger logger, int ignored) throws Exception {
        return logger.minorCompactionFinished(walogSeq, commitSession.getLogId(), fullyQualifiedFileName, durability);
      }
    });

    long t2 = System.currentTimeMillis();

    log.debug(" wrote MinC finish  {}: writeTime:{}ms  durability:{}", seq, (t2 - t1), durability);
  }

  public int minorCompactionStarted(final CommitSession commitSession, final int seq, final String fullyQualifiedFileName, final Durability durability)
      throws IOException {
    write(commitSession, false, new Writer() {
      @Override
      public LoggerOperation write(DfsLogger logger, int ignored) throws Exception {
        return logger.minorCompactionStarted(seq, commitSession.getLogId(), fullyQualifiedFileName, durability);
      }
    });
    return seq;
  }

  public void recover(VolumeManager fs, KeyExtent extent, TableConfiguration tconf, List<Path> logs, Set<String> tabletFiles, MutationReceiver mr)
      throws IOException {
    try {
      SortedLogRecovery recovery = new SortedLogRecovery(fs);
      recovery.recover(extent, logs, tabletFiles, mr);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

}
