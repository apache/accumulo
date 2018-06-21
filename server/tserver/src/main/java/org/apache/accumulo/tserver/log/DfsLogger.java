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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.security.crypto.CryptoEnvironment.Scope;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_FINISH;
import static org.apache.accumulo.tserver.logger.LogEvents.COMPACTION_START;
import static org.apache.accumulo.tserver.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.tserver.logger.LogEvents.MANY_MUTATIONS;
import static org.apache.accumulo.tserver.logger.LogEvents.OPEN;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.security.crypto.CryptoEnvironment;
import org.apache.accumulo.core.security.crypto.CryptoService;
import org.apache.accumulo.core.security.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.security.crypto.FileDecrypter;
import org.apache.accumulo.core.security.crypto.FileEncrypter;
import org.apache.accumulo.core.security.crypto.NoFlushOutputStream;
import org.apache.accumulo.core.security.crypto.impl.NoCryptoService;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.fate.util.LoggingRunnable;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment.ChooserScope;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.tserver.TabletMutations;
import org.apache.accumulo.tserver.logger.LogFileKey;
import org.apache.accumulo.tserver.logger.LogFileValue;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * Wrap a connection to a logger.
 *
 */
public class DfsLogger implements Comparable<DfsLogger> {
  public static final String LOG_FILE_HEADER_V2 = "--- Log File Header (v2) ---";
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
  public static class LogHeaderIncompleteException extends IOException {
    private static final long serialVersionUID = 1L;

    public LogHeaderIncompleteException(Throwable cause) {
      super(cause);
    }
  }

  public static class DFSLoggerInputStreams {

    private FSDataInputStream originalInput;
    private DataInputStream decryptingInputStream;

    public DFSLoggerInputStreams(FSDataInputStream originalInput,
        DataInputStream decryptingInputStream) {
      this.originalInput = originalInput;
      this.decryptingInputStream = decryptingInputStream;
    }

    public FSDataInputStream getOriginalInput() {
      return originalInput;
    }

    public void setOriginalInput(FSDataInputStream originalInput) {
      this.originalInput = originalInput;
    }

    public DataInputStream getDecryptingInputStream() {
      return decryptingInputStream;
    }

    public void setDecryptingInputStream(DataInputStream decryptingInputStream) {
      this.decryptingInputStream = decryptingInputStream;
    }
  }

  public interface ServerResources {
    AccumuloConfiguration getConfiguration();

    VolumeManager getFileSystem();
  }

  private final LinkedBlockingQueue<DfsLogger.LogWork> workQueue = new LinkedBlockingQueue<>();

  private final Object closeLock = new Object();

  private static final DfsLogger.LogWork CLOSED_MARKER = new DfsLogger.LogWork(null,
      Durability.FLUSH);

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

        Method durabilityMethod = null;
        loop: for (LogWork logWork : work) {
          switch (logWork.durability) {
            case DEFAULT:
            case NONE:
            case LOG:
              // shouldn't make it to the work queue
              throw new IllegalArgumentException("unexpected durability " + logWork.durability);
            case SYNC:
              durabilityMethod = sync;
              break loop;
            case FLUSH:
              if (durabilityMethod == null) {
                durabilityMethod = flush;
              }
              break;
          }
        }

        long start = System.currentTimeMillis();
        try {
          if (durabilityMethod != null) {
            durabilityMethod.invoke(logFile);
            if (durabilityMethod == sync) {
              syncCounter.incrementAndGet();
            } else {
              flushCounter.incrementAndGet();
            }
          }
        } catch (Exception ex) {
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
            expectedReplication = ((DFSOutputStream) logFile.getWrappedStream())
                .getCurrentBlockReplication();
          } catch (IOException e) {
            fail(work, e, "getting replication level");
          }
        }

        for (DfsLogger.LogWork logWork : work)
          if (logWork == CLOSED_MARKER)
            sawClosedMarker = true;
          else
            logWork.latch.countDown();
      }
    }

    private void fail(ArrayList<DfsLogger.LogWork> work, Exception ex, String why) {
      log.warn("Exception " + why + " " + ex);
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
        if (work.exception instanceof IOException)
          throw (IOException) work.exception;
        else if (work.exception instanceof RuntimeException)
          throw (RuntimeException) work.exception;
        else
          throw new RuntimeException(work.exception);
      }
    }
  }

  private static class NoWaitLoggerOperation extends LoggerOperation {

    public NoWaitLoggerOperation() {
      super(null);
    }

    @Override
    public void await() throws IOException {
      return;
    }
  }

  static final LoggerOperation NO_WAIT_LOGGER_OP = new NoWaitLoggerOperation();

  @Override
  public boolean equals(Object obj) {
    // filename is unique
    if (obj == null)
      return false;
    if (obj instanceof DfsLogger)
      return getFileName().equals(((DfsLogger) obj).getFileName());
    return false;
  }

  @Override
  public int hashCode() {
    // filename is unique
    return getFileName().hashCode();
  }

  private final ServerResources conf;
  private FSDataOutputStream logFile;
  private DataOutputStream encryptingLogFile = null;
  private Method sync;
  private Method flush;
  private String logPath;
  private Daemon syncThread;

  /* Track what's actually in +r/!0 for this logger ref */
  private String metaReference;
  private AtomicLong syncCounter;
  private AtomicLong flushCounter;
  private final long slowFlushMillis;

  private DfsLogger(ServerResources conf) {
    this.conf = conf;
    this.slowFlushMillis = conf.getConfiguration()
        .getTimeInMillis(Property.TSERV_SLOW_FLUSH_MILLIS);
  }

  public DfsLogger(ServerResources conf, AtomicLong syncCounter, AtomicLong flushCounter)
      throws IOException {
    this(conf);
    this.syncCounter = syncCounter;
    this.flushCounter = flushCounter;
  }

  /**
   * Reference a pre-existing log file.
   *
   * @param meta
   *          the cq for the "log" entry in +r/!0
   */
  public DfsLogger(ServerResources conf, String filename, String meta) throws IOException {
    this(conf);
    this.logPath = filename;
    metaReference = meta;
  }

  public static DFSLoggerInputStreams readHeaderAndReturnStream(FSDataInputStream input,
      AccumuloConfiguration conf) throws IOException {
    DataInputStream decryptingInput;

    byte[] magic = DfsLogger.LOG_FILE_HEADER_V4.getBytes(UTF_8);
    byte[] magicBuffer = new byte[magic.length];
    try {
      input.readFully(magicBuffer);
      if (Arrays.equals(magicBuffer, magic)) {
        CryptoService cryptoService = CryptoServiceFactory.getConfigured(conf);
        CryptoEnvironment env = new CryptoEnvironment(Scope.WAL,
            conf.getAllPropertiesWithPrefix(Property.TABLE_PREFIX));
        env.setParameters(input.readUTF());
        FileDecrypter decrypter = cryptoService.getFileDecrypter(env);
        log.debug("Using {} for decrypting WAL", cryptoService.getClass().getSimpleName());
        decryptingInput = cryptoService instanceof NoCryptoService ? input
            : new DataInputStream(decrypter.decryptStream(input));
      } else {
        log.error("Unsupported WAL version.");
        input.seek(0);
        decryptingInput = input;
      }
    } catch (Exception e) {
      log.warn("Got EOFException trying to read WAL header information,"
          + " assuming the rest of the file has no data.");
      // A TabletServer might have died before the (complete) header was written
      throw new LogHeaderIncompleteException(e);
    }

    return new DFSLoggerInputStreams(input, decryptingInput);
  }

  /**
   * Opens a Write-Ahead Log file and writes the necessary header information and OPEN entry to the
   * file. The file is ready to be used for ingest if this method returns successfully. If an
   * exception is thrown from this method, it is the callers responsibility to ensure that
   * {@link #close()} is called to prevent leaking the file handle and/or syncing thread.
   *
   * @param address
   *          The address of the host using this WAL
   */
  public synchronized void open(String address) throws IOException {
    String filename = UUID.randomUUID().toString();
    log.debug("Address is {}", address);
    String logger = Joiner.on("+").join(address.split(":"));

    log.debug("DfsLogger.open() begin");
    VolumeManager fs = conf.getFileSystem();

    VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironment(ChooserScope.LOGGER);
    logPath = fs.choose(chooserEnv, ServerConstants.getBaseUris()) + Path.SEPARATOR
        + ServerConstants.WAL_DIR + Path.SEPARATOR + logger + Path.SEPARATOR + filename;

    metaReference = toString();
    LoggerOperation op = null;
    try {
      short replication = (short) conf.getConfiguration().getCount(Property.TSERV_WAL_REPLICATION);
      if (replication == 0)
        replication = fs.getDefaultReplication(new Path(logPath));
      long blockSize = getWalBlockSize(conf.getConfiguration());
      if (conf.getConfiguration().getBoolean(Property.TSERV_WAL_SYNC))
        logFile = fs.createSyncable(new Path(logPath), 0, replication, blockSize);
      else
        logFile = fs.create(new Path(logPath), true, 0, replication, blockSize);
      sync = logFile.getClass().getMethod("hsync");
      flush = logFile.getClass().getMethod("hflush");

      // Initialize the log file with a header and its encryption
      CryptoService cryptoService = CryptoServiceFactory.getConfigured(conf.getConfiguration());
      logFile.write(LOG_FILE_HEADER_V4.getBytes(UTF_8));

      log.debug("Using {} for encrypting WAL {}", cryptoService.getClass().getSimpleName(),
          filename);
      CryptoEnvironment env = new CryptoEnvironment(Scope.WAL,
          conf.getConfiguration().getAllPropertiesWithPrefix(Property.TABLE_PREFIX));
      FileEncrypter encrypter = cryptoService.getFileEncrypter(env);
      logFile.writeUTF(encrypter.getParameters());

      encryptingLogFile = new DataOutputStream(
          encrypter.encryptStream(new NoFlushOutputStream(logFile)));

      LogFileKey key = new LogFileKey();
      key.event = OPEN;
      key.tserverSession = filename;
      key.filename = filename;
      op = logFileData(Collections.singletonList(new Pair<>(key, EMPTY)), Durability.SYNC);
    } catch (Exception ex) {
      if (logFile != null)
        logFile.close();
      logFile = null;
      encryptingLogFile = null;
      throw new IOException(ex);
    }

    syncThread = new Daemon(new LoggingRunnable(log, new LogSyncingTask()));
    syncThread.setName("Accumulo WALog thread " + this);
    syncThread.start();
    op.await();
    log.debug("Got new write-ahead log: {}", this);
  }

  static long getWalBlockSize(AccumuloConfiguration conf) {
    long blockSize = conf.getAsBytes(Property.TSERV_WAL_BLOCKSIZE);
    if (blockSize == 0)
      blockSize = (long) (conf.getAsBytes(Property.TSERV_WALOG_MAX_SIZE) * 1.1);
    return blockSize;
  }

  @Override
  public String toString() {
    String fileName = getFileName();
    if (fileName.contains(":"))
      return getLogger() + "/" + getFileName();
    return fileName;
  }

  /**
   * get the cq needed to reference this logger's entry in +r/!0
   */
  public String getMeta() {
    if (null == metaReference) {
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
      if (closed)
        return;
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
    if (workQueue.size() != 0) {
      log.error("WAL work queue not empty after sync thread exited");
      throw new IllegalStateException("WAL work queue not empty after sync thread exited");
    }

    if (encryptingLogFile != null)
      try {
        logFile.close();
      } catch (IOException ex) {
        log.error("Failed to close log file", ex);
        throw new LogClosedException();
      }
  }

  public synchronized void defineTablet(long seq, int tid, KeyExtent tablet) throws IOException {
    // write this log to the METADATA table
    final LogFileKey key = new LogFileKey();
    key.event = DEFINE_TABLET;
    key.seq = seq;
    key.tabletId = tid;
    key.tablet = tablet;
    try {
      write(key, EMPTY);
    } catch (IllegalArgumentException e) {
      log.error("Signature of sync method changed. Accumulo is likely"
          + " incompatible with this version of Hadoop.");
      throw new RuntimeException(e);
    }
  }

  private synchronized void write(LogFileKey key, LogFileValue value) throws IOException {
    key.write(encryptingLogFile);
    value.write(encryptingLogFile);
    encryptingLogFile.flush();
  }

  public LoggerOperation log(long seq, int tid, Mutation mutation, Durability durability)
      throws IOException {
    return logManyTablets(Collections.singletonList(
        new TabletMutations(tid, seq, Collections.singletonList(mutation), durability)));
  }

  private LoggerOperation logFileData(List<Pair<LogFileKey,LogFileValue>> keys,
      Durability durability) throws IOException {
    DfsLogger.LogWork work = new DfsLogger.LogWork(new CountDownLatch(1), durability);
    synchronized (DfsLogger.this) {
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
    }

    if (durability == Durability.LOG)
      return NO_WAIT_LOGGER_OP;

    synchronized (closeLock) {
      // use a different lock for close check so that adding to work queue does not need
      // to wait on walog I/O operations

      if (closed)
        throw new LogClosedException();
      workQueue.add(work);
    }

    return new LoggerOperation(work);
  }

  public LoggerOperation logManyTablets(List<TabletMutations> mutations) throws IOException {
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
      if (tabletMutations.getDurability().ordinal() > durability.ordinal()) {
        durability = tabletMutations.getDurability();
      }
    }
    return logFileData(data, chooseDurabilityForGroupCommit(mutations));
  }

  static Durability chooseDurabilityForGroupCommit(List<TabletMutations> mutations) {
    Durability result = Durability.NONE;
    for (TabletMutations tabletMutations : mutations) {
      if (tabletMutations.getDurability().ordinal() > result.ordinal()) {
        result = tabletMutations.getDurability();
      }
    }
    return result;
  }

  public LoggerOperation minorCompactionFinished(long seq, int tid, String fqfn,
      Durability durability) throws IOException {
    LogFileKey key = new LogFileKey();
    key.event = COMPACTION_FINISH;
    key.seq = seq;
    key.tabletId = tid;
    return logFileData(Collections.singletonList(new Pair<>(key, EMPTY)), durability);
  }

  public LoggerOperation minorCompactionStarted(long seq, int tid, String fqfn,
      Durability durability) throws IOException {
    LogFileKey key = new LogFileKey();
    key.event = COMPACTION_START;
    key.seq = seq;
    key.tabletId = tid;
    key.filename = fqfn;
    return logFileData(Collections.singletonList(new Pair<>(key, EMPTY)), durability);
  }

  public String getLogger() {
    String parts[] = logPath.split("/");
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
    if (null != logFile) {
      OutputStream os = logFile.getWrappedStream();
      if (os instanceof DFSOutputStream) {
        return ((DFSOutputStream) os).getPipeline();
      }
    }

    // Don't have a pipeline or can't figure it out.
    return EMPTY_PIPELINE;
  }

}
