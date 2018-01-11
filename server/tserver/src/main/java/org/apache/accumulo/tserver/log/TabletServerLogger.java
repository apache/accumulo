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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationConfigurationUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
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

  // The current log set: always updated to a new set with every change of loggers
  private final List<DfsLogger> loggers = new ArrayList<>();

  // The current generation of logSet.
  // Because multiple threads can be using a log set at one time, a log
  // failure is likely to affect multiple threads, who will all attempt to
  // create a new logSet. This will cause many unnecessary updates to the
  // metadata table.
  // We'll use this generational counter to determine if another thread has
  // already fetched a new logSet.
  private AtomicInteger logSetId = new AtomicInteger();

  // Use a ReadWriteLock to allow multiple threads to use the log set, but obtain a write lock to change them
  private final ReentrantReadWriteLock logSetLock = new ReentrantReadWriteLock();

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

  private int initializeLoggers(final List<DfsLogger> copy) throws IOException {
    final int[] result = {-1};
    testLockAndRun(logSetLock, new TestCallWithWriteLock() {
      @Override
      boolean test() {
        copy.clear();
        copy.addAll(loggers);
        if (!loggers.isEmpty())
          result[0] = logSetId.get();
        return loggers.isEmpty();
      }

      @Override
      void withWriteLock() throws IOException {
        try {
          createLoggers();
          copy.clear();
          copy.addAll(loggers);
          if (copy.size() > 0)
            result[0] = logSetId.get();
          else
            result[0] = -1;
        } catch (IOException e) {
          log.error("Unable to create loggers", e);
        }
      }
    });
    return result[0];
  }

  public void getLogFiles(Set<String> loggersOut) {
    logSetLock.readLock().lock();
    try {
      for (DfsLogger logger : loggers) {
        loggersOut.add(logger.getFileName());
      }
    } finally {
      logSetLock.readLock().unlock();
    }
  }

  synchronized private void createLoggers() throws IOException {
    if (!logSetLock.isWriteLockedByCurrentThread()) {
      throw new IllegalStateException("createLoggers should be called with write lock held!");
    }

    if (loggers.size() != 0) {
      throw new IllegalStateException("createLoggers should not be called when loggers.size() is " + loggers.size());
    }

    try {
      DfsLogger alog = new DfsLogger(tserver.getServerConfig(), syncCounter, flushCounter);
      alog.open(tserver.getClientAddressString());
      loggers.add(alog);
      logSetId.incrementAndGet();

      // When we successfully create a WAL, make sure to reset the Retry.
      if (null != retry) {
        retry = null;
      }

      this.createTime = System.currentTimeMillis();
      return;
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

  synchronized private void close() throws IOException {
    if (!logSetLock.isWriteLockedByCurrentThread()) {
      throw new IllegalStateException("close should be called with write lock held!");
    }
    try {
      for (DfsLogger logger : loggers) {
        try {
          logger.close();
        } catch (DfsLogger.LogClosedException ex) {
          // ignore
        } catch (Throwable ex) {
          log.error("Unable to cleanly close log " + logger.getFileName() + ": " + ex, ex);
        }
      }
      loggers.clear();
      logSizeEstimate.set(0);
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  interface Writer {
    LoggerOperation write(DfsLogger logger) throws Exception;
  }

  private void write(CommitSession commitSession, boolean mincFinish, Writer writer) throws IOException {
    List<CommitSession> sessions = Collections.singletonList(commitSession);
    write(sessions, mincFinish, writer);
  }

  private void write(final Collection<CommitSession> sessions, boolean mincFinish, Writer writer) throws IOException {
    // Work very hard not to lock this during calls to the outside world
    int currentLogSet = logSetId.get();

    int attempt = 1;
    boolean success = false;
    while (!success) {
      try {
        // get a reference to the loggers that no other thread can touch
        ArrayList<DfsLogger> copy = new ArrayList<>();
        currentLogSet = initializeLoggers(copy);

        // add the logger to the log set for the memory in the tablet,
        // update the metadata table if we've never used this tablet

        if (currentLogSet == logSetId.get()) {
          for (CommitSession commitSession : sessions) {
            if (commitSession.beginUpdatingLogsUsed(copy, mincFinish)) {
              try {
                // Scribble out a tablet definition and then write to the metadata table
                defineTablet(commitSession);
                if (currentLogSet == logSetId.get())
                  tserver.addLoggersToMetadata(copy, commitSession.getExtent(), commitSession.getLogId());
              } finally {
                commitSession.finishUpdatingLogsUsed();
              }

              // Need to release
              KeyExtent extent = commitSession.getExtent();
              if (ReplicationConfigurationUtil.isEnabled(extent, tserver.getTableConfiguration(extent))) {
                Set<String> logs = new HashSet<>();
                for (DfsLogger logger : copy) {
                  logs.add(logger.getFileName());
                }
                Status status = StatusUtil.fileCreated(System.currentTimeMillis());
                log.debug("Writing " + ProtobufUtil.toString(status) + " to metadata table for " + logs);
                // Got some new WALs, note this in the metadata table
                ReplicationTableUtil.updateFiles(tserver, commitSession.getExtent(), logs, status);
              }
            }
          }
        }

        // Make sure that the logs haven't changed out from underneath our copy
        if (currentLogSet == logSetId.get()) {

          // write the mutation to the logs
          ArrayList<LoggerOperation> queuedOperations = new ArrayList<>(copy.size());
          for (DfsLogger wal : copy) {
            queuedOperations.add(writer.write(wal));
          }

          for (LoggerOperation lop : queuedOperations) {
            lop.await();
          }

          // double-check: did the log set change?
          success = (currentLogSet == logSetId.get());
        }
      } catch (DfsLogger.LogClosedException ex) {
        log.debug("Logs closed while writing, retrying " + attempt);
      } catch (Exception t) {
        if (attempt != 1) {
          log.error("Unexpected error writing to log, retrying attempt " + attempt, t);
        }
        UtilWaitThread.sleep(100);
      } finally {
        attempt++;
      }
      // Some sort of write failure occurred. Grab the write lock and reset the logs.
      // But since multiple threads will attempt it, only attempt the reset when
      // the logs haven't changed.
      final int finalCurrent = currentLogSet;
      if (!success) {
        testLockAndRun(logSetLock, new TestCallWithWriteLock() {

          @Override
          boolean test() {
            return finalCurrent == logSetId.get();
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
    testLockAndRun(logSetLock, new TestCallWithWriteLock() {
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
  }

  protected void closeForReplication(Collection<CommitSession> sessions) {
    // TODO We can close the WAL here for replication purposes
  }

  public void defineTablet(final CommitSession commitSession) throws IOException {
    // scribble this into the metadata tablet, too.
    write(commitSession, false, new Writer() {
      @Override
      public LoggerOperation write(DfsLogger logger) throws Exception {
        logger.defineTablet(commitSession.getWALogSeq(), commitSession.getLogId(), commitSession.getExtent());
        return DfsLogger.NO_WAIT_LOGGER_OP;
      }
    });
  }

  public void log(final CommitSession commitSession, final long tabletSeq, final Mutation m, final Durability durability) throws IOException {
    if (durability == Durability.NONE) {
      return;
    }
    if (durability == Durability.DEFAULT) {
      throw new IllegalArgumentException("Unexpected durability " + durability);
    }
    write(commitSession, false, new Writer() {
      @Override
      public LoggerOperation write(DfsLogger logger) throws Exception {
        return logger.log(tabletSeq, commitSession.getLogId(), m, durability);
      }
    });
    logSizeEstimate.addAndGet(m.numBytes());
  }

  public void logManyTablets(Map<CommitSession,Mutations> mutations) throws IOException {

    final Map<CommitSession,Mutations> loggables = new HashMap<>(mutations);
    for (Entry<CommitSession,Mutations> entry : mutations.entrySet()) {
      if (entry.getValue().getDurability() == Durability.NONE) {
        loggables.remove(entry.getKey());
      }
    }
    if (loggables.size() == 0)
      return;

    write(loggables.keySet(), false, new Writer() {
      @Override
      public LoggerOperation write(DfsLogger logger) throws Exception {
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
  }

  public void minorCompactionFinished(final CommitSession commitSession, final String fullyQualifiedFileName, final long walogSeq, final Durability durability)
      throws IOException {

    long t1 = System.currentTimeMillis();

    write(commitSession, true, new Writer() {
      @Override
      public LoggerOperation write(DfsLogger logger) throws Exception {
        logger.minorCompactionFinished(walogSeq, commitSession.getLogId(), fullyQualifiedFileName, durability).await();
        return DfsLogger.NO_WAIT_LOGGER_OP;
      }
    });

    long t2 = System.currentTimeMillis();

    log.debug(" wrote MinC finish: writeTime:{}ms  durability:{}", (t2 - t1), durability);
  }

  public long minorCompactionStarted(final CommitSession commitSession, final long seq, final String fullyQualifiedFileName, final Durability durability)
      throws IOException {
    write(commitSession, false, new Writer() {
      @Override
      public LoggerOperation write(DfsLogger logger) throws Exception {
        logger.minorCompactionStarted(seq, commitSession.getLogId(), fullyQualifiedFileName, durability).await();
        return DfsLogger.NO_WAIT_LOGGER_OP;
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
