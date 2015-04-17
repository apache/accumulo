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
import java.util.concurrent.TimeUnit;
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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

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

  private final TabletServer tserver;

  // The current log set: always updated to a new set with every change of loggers
  private final List<DfsLogger> loggers = new ArrayList<DfsLogger>();

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

  private final AtomicInteger seqGen = new AtomicInteger();

  private final AtomicLong syncCounter;
  private final AtomicLong flushCounter;

  private final static int HALT_AFTER_ERROR_COUNT = 5;
  // Die if we get 5 WAL creation errors in 10 seconds
  private final Cache<Long,Object> walErrors = CacheBuilder.newBuilder().maximumSize(HALT_AFTER_ERROR_COUNT).expireAfterWrite(10, TimeUnit.SECONDS).build();

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

  public TabletServerLogger(TabletServer tserver, long maxSize, AtomicLong syncCounter, AtomicLong flushCounter) {
    this.tserver = tserver;
    this.maxSize = maxSize;
    this.syncCounter = syncCounter;
    this.flushCounter = flushCounter;
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
      return;
    } catch (Exception t) {
      walErrors.put(System.currentTimeMillis(), "");
      if (walErrors.size() >= HALT_AFTER_ERROR_COUNT) {
        Halt.halt("Experienced too many errors creating WALs, giving up");
      }
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
    LoggerOperation write(DfsLogger logger, int seq) throws Exception;
  }

  private int write(CommitSession commitSession, boolean mincFinish, Writer writer) throws IOException {
    List<CommitSession> sessions = Collections.singletonList(commitSession);
    return write(sessions, mincFinish, writer);
  }

  private int write(final Collection<CommitSession> sessions, boolean mincFinish, Writer writer) throws IOException {
    // Work very hard not to lock this during calls to the outside world
    int currentLogSet = logSetId.get();

    int seq = -1;
    int attempt = 1;
    boolean success = false;
    while (!success) {
      try {
        // get a reference to the loggers that no other thread can touch
        ArrayList<DfsLogger> copy = new ArrayList<DfsLogger>();
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
                Set<String> logs = new HashSet<String>();
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
          seq = seqGen.incrementAndGet();
          if (seq < 0)
            throw new RuntimeException("Logger sequence generator wrapped!  Onos!!!11!eleven");
          ArrayList<LoggerOperation> queuedOperations = new ArrayList<LoggerOperation>(copy.size());
          for (DfsLogger wal : copy) {
            LoggerOperation lop = writer.write(wal, seq);
            if (lop != null)
              queuedOperations.add(lop);
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
    // if the log gets too big, reset it .. grab the write lock first
    logSizeEstimate.addAndGet(4 * 3); // event, tid, seq overhead
    testLockAndRun(logSetLock, new TestCallWithWriteLock() {
      @Override
      boolean test() {
        return logSizeEstimate.get() > maxSize;
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
        return null;
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

    final Map<CommitSession,Mutations> loggables = new HashMap<CommitSession,Mutations>(mutations);
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
        List<TabletMutations> copy = new ArrayList<TabletMutations>(loggables.size());
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

  public void minorCompactionFinished(final CommitSession commitSession, final String fullyQualifiedFileName, final int walogSeq) throws IOException {

    long t1 = System.currentTimeMillis();

    int seq = write(commitSession, true, new Writer() {
      @Override
      public LoggerOperation write(DfsLogger logger, int ignored) throws Exception {
        logger.minorCompactionFinished(walogSeq, commitSession.getLogId(), fullyQualifiedFileName).await();
        return null;
      }
    });

    long t2 = System.currentTimeMillis();

    log.debug(" wrote MinC finish  " + seq + ": writeTime:" + (t2 - t1) + "ms ");
  }

  public int minorCompactionStarted(final CommitSession commitSession, final int seq, final String fullyQualifiedFileName) throws IOException {
    write(commitSession, false, new Writer() {
      @Override
      public LoggerOperation write(DfsLogger logger, int ignored) throws Exception {
        logger.minorCompactionStarted(seq, commitSession.getLogId(), fullyQualifiedFileName).await();
        return null;
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
