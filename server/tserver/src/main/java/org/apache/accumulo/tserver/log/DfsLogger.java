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
package org.apache.accumulo.tserver.log;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_FINISH;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_START;
import static org.apache.accumulo.tserver.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.tserver.logger.LogEvents.MANY_MUTATIONS;
import static org.apache.accumulo.tserver.logger.LogEvents.MUTATION;
import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoEnvironmentImpl;
import org.apache.accumulo.core.crypto.CryptoUtils;
import org.apache.accumulo.core.crypto.streams.NoFlushOutputStream;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment.Scope;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.FileDecrypter;
import org.apache.accumulo.core.spi.crypto.FileEncrypter;
import org.apache.accumulo.core.spi.crypto.NoCryptoService;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.tserver.TabletMutations;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.accumulo.tserver.tablet.CommitSession;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Wrap a connection to a logger.
 *
 */
public class DfsLogger implements Comparable<DfsLogger> {
  // older version supported for upgrade
  public static final String LOG_FILE_HEADER_V3 = "--- Log File Header (v3) ---";

  /**
   * Simplified encryption technique supported in V4.
   *
   * @since 2.0
   */
  public static final String LOG_FILE_HEADER_V4 = "--- Log File Header (v4) ---";

  private static final Logger log = LoggerFactory.getLogger(DfsLogger.class);
  private static final DatanodeInfo[] EMPTY_PIPELINE = new DatanodeInfo[0];

  public static class LogClosedException extends IOException {
    private static final long serialVersionUID = 1L;

    public LogClosedException() {
      super("LogClosed");
    }
  }

  /**
   * A well-timed tabletserver failure could result in an incomplete header written to a write-ahead
   * log. This exception is thrown when the header cannot be read from a WAL which should only
   * happen when the tserver dies as described.
   */
  public static class LogHeaderIncompleteException extends Exception {
    private static final long serialVersionUID = 1L;

    public LogHeaderIncompleteException(EOFException cause) {
      super(cause);
    }
  }

  public interface ServerResources {
    AccumuloConfiguration getConfiguration();

    VolumeManager getVolumeManager();
  }

  private final LinkedBlockingQueue<DfsLogger.LogWork> workQueue = new LinkedBlockingQueue<>();

  private final Object closeLock = new Object();

  private static final DfsLogger.LogWork CLOSED_MARKER =
      new DfsLogger.LogWork(null, Durability.FLUSH);

  private static final LogFileValue EMPTY = new LogFileValue();

  private boolean closed = false;

  private class LogSyncingTask implements Runnable {
    private int expectedReplication = 0;

    @Override
    public void run() {
      ArrayList<DfsLogger.LogWork> work = new ArrayList<>();
      boolean sawClosedMarker = false;
      while (!sawClosedMarker) {
        work.clear();

        try {
          work.add(workQueue.take());
        } catch (InterruptedException ex) {
          continue;
        }
        workQueue.drainTo(work);

        Optional<Boolean> shouldHSync = Optional.empty();
        loop: for (LogWork logWork : work) {
          switch (logWork.durability) {
            case DEFAULT:
            case NONE:
            case LOG:
              // shouldn't make it to the work queue
              throw new IllegalArgumentException("unexpected durability " + logWork.durability);
            case SYNC:
              shouldHSync = Optional.of(Boolean.TRUE);
              break loop;
            case FLUSH:
              if (shouldHSync.isEmpty()) {
                shouldHSync = Optional.of(Boolean.FALSE);
              }
              break;
          }
        }

        long start = System.currentTimeMillis();
        try {
          if (shouldHSync.isPresent()) {
            if (shouldHSync.get()) {
              logFile.hsync();
              syncCounter.incrementAndGet();
            } else {
              logFile.hflush();
              flushCounter.incrementAndGet();
            }
          }
        } catch (IOException | RuntimeException ex) {
          fail(work, ex, "synching");
        }
        long duration = System.currentTimeMillis() - start;
        if (duration > slowFlushMillis) {
          String msg = new StringBuilder(128).append("Slow sync cost: ").append(duration)
              .append(" ms, current pipeline: ").append(Arrays.toString(getPipeLine())).toString();
          log.info(msg);
          if (expectedReplication > 0) {
            int current = expectedReplication;
            try {
              current = ((DFSOutputStream) logFile.getWrappedStream()).getCurrentBlockReplication();
            } catch (IOException e) {
              fail(work, e, "getting replication level");
            }
            if (current < expectedReplication) {
              fail(work,
                  new IOException(
                      "replication of " + current + " is less than " + expectedReplication),
                  "replication check");
            }
          }
        }
        if (expectedReplication == 0 && logFile.getWrappedStream() instanceof DFSOutputStream) {
          try {
            expectedReplication =
                ((DFSOutputStream) logFile.getWrappedStream()).getCurrentBlockReplication();
          } catch (IOException e) {
            fail(work, e, "getting replication level");
          }
        }

        for (DfsLogger.LogWork logWork : work) {
          if (logWork == CLOSED_MARKER) {
            sawClosedMarker = true;
          } else {
            logWork.latch.countDown();
          }
        }
      }
    }

    private void fail(ArrayList<DfsLogger.LogWork> work, Exception ex, String why) {
      log.warn("Exception {} {}", why, ex, ex);
      for (DfsLogger.LogWork logWork : work) {
        logWork.exception = ex;
      }
    }
  }

  private static class LogWork {
    final CountDownLatch latch;
    final Durability durability;
    volatile Exception exception;

    public LogWork(CountDownLatch latch, Durability durability) {
      this.latch = latch;
      this.durability = durability;
    }
  }

  static class LoggerOperation {
    private final LogWork work;

    public LoggerOperation(LogWork work) {
      this.work = work;
    }

    public void await() throws IOException {
      try {
        work.latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      if (work.exception != null) {
        if (work.exception instanceof IOException) {
          throw (IOException) work.exception;
        } else if (work.exception instanceof RuntimeException) {
          throw (RuntimeException) work.exception;
        } else {
          throw new RuntimeException(work.exception);
        }
      }
    }
  }

  private static class NoWaitLoggerOperation extends LoggerOperation {

    public NoWaitLoggerOperation() {
      super(null);
    }

    @Override
    public void await() {}
  }

  static final LoggerOperation NO_WAIT_LOGGER_OP = new NoWaitLoggerOperation();

  @Override
  public boolean equals(Object obj) {
    // filename is unique
    if (obj == null) {
      return false;
    }
    if (obj instanceof DfsLogger) {
      return getFileName().equals(((DfsLogger) obj).getFileName());
    }
    return false;
  }

  @Override
  public int hashCode() {
    // filename is unique
    return getFileName().hashCode();
  }

  private final ServerContext context;
  private final ServerResources conf;
  private FSDataOutputStream logFile;
  private DataOutputStream encryptingLogFile = null;
  private String logPath;
  private Thread syncThread;

  /* Track what's actually in +r/!0 for this logger ref */
  private String metaReference;
  private AtomicLong syncCounter;
  private AtomicLong flushCounter;
  private final long slowFlushMillis;
  private long writes = 0;

  private DfsLogger(ServerContext context, ServerResources conf) {
    this.context = context;
    this.conf = conf;
    this.slowFlushMillis =
        conf.getConfiguration().getTimeInMillis(Property.TSERV_SLOW_FLUSH_MILLIS);
  }

  public DfsLogger(ServerContext context, ServerResources conf, AtomicLong syncCounter,
      AtomicLong flushCounter) {
    this(context, conf);
    this.syncCounter = syncCounter;
    this.flushCounter = flushCounter;
  }

  /**
   * Reference a pre-existing log file.
   *
   * @param meta the cq for the "log" entry in +r/!0
   */
  public DfsLogger(ServerContext context, ServerResources conf, String filename, String meta) {
    this(context, conf);
    this.logPath = filename;
    metaReference = meta;
  }

  /**
   * Reads the WAL file header, and returns a decrypting stream which wraps the original stream. If
   * the file is not encrypted, the original stream is returned.
   *
   * @throws LogHeaderIncompleteException if the header cannot be fully read (can happen if the
   *         tserver died before finishing)
   */
  public static DataInputStream getDecryptingStream(FSDataInputStream input,
      CryptoService cryptoService) throws LogHeaderIncompleteException, IOException {
    DataInputStream decryptingInput;

    byte[] magic4 = DfsLogger.LOG_FILE_HEADER_V4.getBytes(UTF_8);
    byte[] magic3 = DfsLogger.LOG_FILE_HEADER_V3.getBytes(UTF_8);

    if (magic4.length != magic3.length) {
      throw new AssertionError("Always expect log file headers to be same length : " + magic4.length
          + " != " + magic3.length);
    }

    byte[] magicBuffer = new byte[magic4.length];
    try {
      input.readFully(magicBuffer);
      if (Arrays.equals(magicBuffer, magic4)) {
        FileDecrypter decrypter =
            CryptoUtils.getFileDecrypter(cryptoService, Scope.WAL, null, input);
        log.debug("Using {} for decrypting WAL", cryptoService.getClass().getSimpleName());
        decryptingInput = cryptoService instanceof NoCryptoService ? input
            : new DataInputStream(decrypter.decryptStream(input));
      } else if (Arrays.equals(magicBuffer, magic3)) {
        // Read logs files from Accumulo 1.9 and throw an error if they are encrypted
        String cryptoModuleClassname = input.readUTF();
        if (!cryptoModuleClassname.equals("NullCryptoModule")) {
          throw new IllegalArgumentException(
              "Old encryption modules not supported at this time.  Unsupported module : "
                  + cryptoModuleClassname);
        }

        decryptingInput = input;
      } else {
        throw new IllegalArgumentException(
            "Unsupported write ahead log version " + new String(magicBuffer));
      }
    } catch (EOFException e) {
      // Explicitly catch any exceptions that should be converted to LogHeaderIncompleteException
      // A TabletServer might have died before the (complete) header was written
      throw new LogHeaderIncompleteException(e);
    }

    return decryptingInput;
  }

  /**
   * Opens a Write-Ahead Log file and writes the necessary header information and OPEN entry to the
   * file. The file is ready to be used for ingest if this method returns successfully. If an
   * exception is thrown from this method, it is the callers responsibility to ensure that
   * {@link #close()} is called to prevent leaking the file handle and/or syncing thread.
   *
   * @param address The address of the host using this WAL
   */
  public synchronized void open(String address) throws IOException {
    String filename = UUID.randomUUID().toString();
    log.debug("Address is {}", address);
    String logger = Joiner.on("+").join(address.split(":"));

    log.debug("DfsLogger.open() begin");
    VolumeManager fs = conf.getVolumeManager();

    var chooserEnv = new VolumeChooserEnvironmentImpl(
        org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment.Scope.LOGGER, context);
    logPath = fs.choose(chooserEnv, context.getBaseUris()) + Path.SEPARATOR + Constants.WAL_DIR
        + Path.SEPARATOR + logger + Path.SEPARATOR + filename;

    metaReference = toString();
    LoggerOperation op = null;
    try {
      Path logfilePath = new Path(logPath);
      short replication = (short) conf.getConfiguration().getCount(Property.TSERV_WAL_REPLICATION);
      if (replication == 0) {
        replication = fs.getDefaultReplication(logfilePath);
      }
      long blockSize = getWalBlockSize(conf.getConfiguration());
      if (conf.getConfiguration().getBoolean(Property.TSERV_WAL_SYNC)) {
        logFile = fs.createSyncable(logfilePath, 0, replication, blockSize);
      } else {
        logFile = fs.create(logfilePath, true, 0, replication, blockSize);
      }

      // Tell the DataNode that the write ahead log does not need to be cached in the OS page cache
      try {
        logFile.setDropBehind(Boolean.TRUE);
      } catch (UnsupportedOperationException e) {
        log.debug("setDropBehind writes not enabled for wal file: {}", logFile);
      } catch (IOException e) {
        log.debug("IOException setting drop behind for file: {}, msg: {}", logFile, e.getMessage());
      }

      // check again that logfile can be sync'd
      if (!fs.canSyncAndFlush(logfilePath)) {
        log.warn("sync not supported for log file {}. Data loss may occur.", logPath);
      }

      // Initialize the log file with a header and its encryption
      CryptoEnvironment env = new CryptoEnvironmentImpl(Scope.WAL);
      CryptoService cryptoService = context.getCryptoFactory().getService(env,
          conf.getConfiguration().getAllCryptoProperties());
      logFile.write(LOG_FILE_HEADER_V4.getBytes(UTF_8));

      log.debug("Using {} for encrypting WAL {}", cryptoService.getClass().getSimpleName(),
          filename);
      FileEncrypter encrypter = cryptoService.getFileEncrypter(env);
      byte[] cryptoParams = encrypter.getDecryptionParameters();
      CryptoUtils.writeParams(cryptoParams, logFile);

      /**
       * Always wrap the WAL in a NoFlushOutputStream to prevent extra flushing to HDFS. The
       * {@link #write(LogFileKey, LogFileValue)} method will flush crypto data or do nothing when
       * crypto is not enabled.
       **/
      OutputStream encryptedStream = encrypter.encryptStream(new NoFlushOutputStream(logFile));
      if (encryptedStream instanceof NoFlushOutputStream) {
        encryptingLogFile = (NoFlushOutputStream) encryptedStream;
      } else {
        encryptingLogFile = new DataOutputStream(encryptedStream);
      }

      LogFileKey key = new LogFileKey();
      key.event = OPEN;
      key.tserverSession = filename;
      key.filename = filename;
      op = logKeyData(key, Durability.SYNC);
    } catch (Exception ex) {
      if (logFile != null) {
        logFile.close();
      }
      logFile = null;
      encryptingLogFile = null;
      throw new IOException(ex);
    }

    syncThread = Threads.createThread("Accumulo WALog thread " + this, new LogSyncingTask());
    syncThread.start();
    op.await();
    log.debug("Got new write-ahead log: {}", this);
  }

  static long getWalBlockSize(AccumuloConfiguration conf) {
    long blockSize = conf.getAsBytes(Property.TSERV_WAL_BLOCKSIZE);
    if (blockSize == 0) {
      @SuppressWarnings("deprecation")
      Property prop = conf.resolve(Property.TSERV_WAL_MAX_SIZE, Property.TSERV_WALOG_MAX_SIZE);
      blockSize = (long) (conf.getAsBytes(prop) * 1.1);
    }
    return blockSize;
  }

  @Override
  public String toString() {
    String fileName = getFileName();
    if (fileName.contains(":")) {
      return getLogger() + "/" + getFileName();
    }
    return fileName;
  }

  /**
   * get the cq needed to reference this logger's entry in +r/!0
   */
  public String getMeta() {
    if (metaReference == null) {
      throw new IllegalStateException("logger doesn't have meta reference. " + this);
    }
    return metaReference;
  }

  public String getFileName() {
    return logPath;
  }

  public Path getPath() {
    return new Path(logPath);
  }

  public void close() throws IOException {

    synchronized (closeLock) {
      if (closed) {
        return;
      }
      // after closed is set to true, nothing else should be added to the queue
      // CLOSED_MARKER should be the last thing on the queue, therefore when the
      // background thread sees the marker and exits there should be nothing else
      // to process... so nothing should be left waiting for the background
      // thread to do work
      closed = true;
      workQueue.add(CLOSED_MARKER);
    }

    // wait for background thread to finish before closing log file
    if (syncThread != null) {
      try {
        syncThread.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    // expect workq should be empty at this point
    if (!workQueue.isEmpty()) {
      log.error("WAL work queue not empty after sync thread exited");
      throw new IllegalStateException("WAL work queue not empty after sync thread exited");
    }

    if (encryptingLogFile != null) {
      try {
        logFile.close();
      } catch (IOException ex) {
        log.error("Failed to close log file", ex);
        throw new LogClosedException();
      }
    }
  }

  public synchronized long getWrites() {
    Preconditions.checkState(writes >= 0);
    return writes;
  }

  public LoggerOperation defineTablet(CommitSession cs) throws IOException {
    // write this log to the METADATA table
    final LogFileKey key = new LogFileKey();
    key.event = DEFINE_TABLET;
    key.seq = cs.getWALogSeq();
    key.tabletId = cs.getLogId();
    key.tablet = cs.getExtent();
    return logKeyData(key, Durability.LOG);
  }

  private synchronized void write(LogFileKey key, LogFileValue value) throws IOException {
    key.write(encryptingLogFile);
    value.write(encryptingLogFile);
    encryptingLogFile.flush();
    writes++;
  }

  private LoggerOperation logKeyData(LogFileKey key, Durability d) throws IOException {
    return logFileData(singletonList(new Pair<>(key, EMPTY)), d);
  }

  private LoggerOperation logFileData(List<Pair<LogFileKey,LogFileValue>> keys,
      Durability durability) throws IOException {
    DfsLogger.LogWork work = new DfsLogger.LogWork(new CountDownLatch(1), durability);
    try {
      for (Pair<LogFileKey,LogFileValue> pair : keys) {
        write(pair.getFirst(), pair.getSecond());
      }
    } catch (ClosedChannelException ex) {
      throw new LogClosedException();
    } catch (Exception e) {
      log.error("Failed to write log entries", e);
      work.exception = e;
    }

    synchronized (closeLock) {
      // use a different lock for close check so that adding to work queue does not need
      // to wait on walog I/O operations

      if (closed) {
        throw new LogClosedException();
      }

      if (durability == Durability.LOG) {
        return NO_WAIT_LOGGER_OP;
      }

      workQueue.add(work);
    }

    return new LoggerOperation(work);
  }

  public LoggerOperation logManyTablets(Collection<TabletMutations> mutations) throws IOException {
    Durability durability = Durability.NONE;
    List<Pair<LogFileKey,LogFileValue>> data = new ArrayList<>();
    for (TabletMutations tabletMutations : mutations) {
      LogFileKey key = new LogFileKey();
      key.event = MANY_MUTATIONS;
      key.seq = tabletMutations.getSeq();
      key.tabletId = tabletMutations.getTid();
      LogFileValue value = new LogFileValue();
      value.mutations = tabletMutations.getMutations();
      data.add(new Pair<>(key, value));
      durability = maxDurability(tabletMutations.getDurability(), durability);
    }
    return logFileData(data, durability);
  }

  public LoggerOperation log(CommitSession cs, Mutation m, Durability d) throws IOException {
    LogFileKey key = new LogFileKey();
    key.event = MUTATION;
    key.seq = cs.getWALogSeq();
    key.tabletId = cs.getLogId();
    LogFileValue value = new LogFileValue();
    value.mutations = singletonList(m);
    return logFileData(singletonList(new Pair<>(key, value)), d);
  }

  /**
   * Return the Durability with the highest precedence
   */
  static Durability maxDurability(Durability dur1, Durability dur2) {
    if (dur1.ordinal() > dur2.ordinal()) {
      return dur1;
    } else {
      return dur2;
    }
  }

  public LoggerOperation minorCompactionFinished(long seq, int tid, Durability durability)
      throws IOException {
    LogFileKey key = new LogFileKey();
    key.event = COMPACTION_FINISH;
    key.seq = seq;
    key.tabletId = tid;
    return logKeyData(key, durability);
  }

  public LoggerOperation minorCompactionStarted(long seq, int tid, String fqfn,
      Durability durability) throws IOException {
    LogFileKey key = new LogFileKey();
    key.event = COMPACTION_START;
    key.seq = seq;
    key.tabletId = tid;
    key.filename = fqfn;
    return logKeyData(key, durability);
  }

  private String getLogger() {
    String[] parts = logPath.split("/");
    return Joiner.on(":").join(parts[parts.length - 2].split("[+]"));
  }

  @Override
  public int compareTo(DfsLogger o) {
    return getFileName().compareTo(o.getFileName());
  }

  /*
   * The following method was shamelessly lifted from HBASE-11240 (sans reflection). Thanks HBase!
   */

  /**
   * This method gets the pipeline for the current walog.
   *
   * @return non-null array of DatanodeInfo
   */
  DatanodeInfo[] getPipeLine() {
    if (logFile != null) {
      OutputStream os = logFile.getWrappedStream();
      if (os instanceof DFSOutputStream) {
        return ((DFSOutputStream) os).getPipeline();
      }
    }

    // Don't have a pipeline or can't figure it out.
    return EMPTY_PIPELINE;
  }

}
