/**
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
package org.apache.accumulo.server.tabletserver.log;

import static org.apache.accumulo.server.logger.LogEvents.COMPACTION_FINISH;
import static org.apache.accumulo.server.logger.LogEvents.COMPACTION_START;
import static org.apache.accumulo.server.logger.LogEvents.DEFINE_TABLET;
import static org.apache.accumulo.server.logger.LogEvents.MANY_MUTATIONS;
import static org.apache.accumulo.server.logger.LogEvents.OPEN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.tabletserver.thrift.TabletMutations;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.server.logger.LogFileKey;
import org.apache.accumulo.server.logger.LogFileValue;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Wrap a connection to a logger.
 * 
 */
public class DfsLogger {
  private static Logger log = Logger.getLogger(DfsLogger.class);
  
  public static class LogClosedException extends IOException {
    private static final long serialVersionUID = 1L;

    public LogClosedException() {
      super("LogClosed");
    }
  }

  public interface ServerResources {
    AccumuloConfiguration getConfiguration();
    
    FileSystem getFileSystem();
    
    Set<TServerInstance> getCurrentTServers();
  }

  private LinkedBlockingQueue<DfsLogger.LogWork> workQueue = new LinkedBlockingQueue<DfsLogger.LogWork>();
  
  private String closeLock = new String("foo");
  
  private static final DfsLogger.LogWork CLOSED_MARKER = new DfsLogger.LogWork(null, null);
  
  private static final LogFileValue EMPTY = new LogFileValue();
  
  private boolean closed = false;

  private class LogSyncingTask implements Runnable {

    @Override
    public void run() {
      ArrayList<DfsLogger.LogWork> work = new ArrayList<DfsLogger.LogWork>();
      while (true) {
        work.clear();
        
        try {
          work.add(workQueue.take());
        } catch (InterruptedException ex) {
          continue;
        }
        workQueue.drainTo(work);
        
        synchronized (closeLock) {
          if (!closed) {
            try {
              logFile.sync();
            } catch (IOException ex) {
              log.warn("Exception syncing " + ex);
              for (DfsLogger.LogWork logWork : work) {
                logWork.exception = ex;
              }
            }
          } else {
            for (DfsLogger.LogWork logWork : work) {
              logWork.exception = new LogClosedException();
            }
          }
        }
        
        boolean sawClosedMarker = false;
        for (DfsLogger.LogWork logWork : work)
          if (logWork == CLOSED_MARKER)
            sawClosedMarker = true;
          else
            logWork.latch.countDown();
        
        if (sawClosedMarker) {
          synchronized (closeLock) {
            closeLock.notifyAll();
          }
          break;
        }
      }
    }
  }

  static class LogWork {
    List<TabletMutations> mutations;
    CountDownLatch latch;
    volatile Exception exception;
    
    public LogWork(List<TabletMutations> mutations, CountDownLatch latch) {
      this.mutations = mutations;
      this.latch = latch;
    }
  }

  public static class LoggerOperation {
    private LogWork work;
    
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

  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    // filename is unique
    if (obj == null)
      return false;
    if (obj instanceof DfsLogger)
      return getFileName().equals(((DfsLogger) obj).getFileName());
    return false;
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#hashCode()
   */
  @Override
  public int hashCode() {
    // filename is unique
    return getFileName().hashCode();
  }
  
  private ServerResources conf;
  private FSDataOutputStream logFile;
  private Path logPath;
  private String logger;
  
  public DfsLogger(ServerResources conf) throws IOException {
    this.conf = conf;
  }
  
  public DfsLogger(ServerResources conf, String logger, String filename) throws IOException {
    this.conf = conf;
    this.logger = logger;
    this.logPath = new Path(Constants.getWalDirectory(conf.getConfiguration()), filename);
  }

  public synchronized void open(String address) throws IOException {
    String filename = UUID.randomUUID().toString();
    logger = StringUtil.join(Arrays.asList(address.split(":")), "+");

    logPath = new Path(Constants.getWalDirectory(conf.getConfiguration()) + "/" + logger + "/" + filename);
    try {
      FileSystem fs = conf.getFileSystem();
      short replication = (short) conf.getConfiguration().getCount(Property.TSERV_WAL_REPLICATION);
      if (replication == 0)
        replication = (short) fs.getDefaultReplication();
      long blockSize = conf.getConfiguration().getMemoryInBytes(Property.TSERV_WAL_BLOCKSIZE);
      if (blockSize == 0)
        blockSize = (long) (conf.getConfiguration().getMemoryInBytes(Property.TSERV_WALOG_MAX_SIZE) * 1.1);
      int checkSum = fs.getConf().getInt("io.bytes.per.checksum", 512);
      blockSize -= blockSize % checkSum;
      blockSize = Math.max(blockSize, checkSum);
      logFile = fs.create(logPath, true, fs.getConf().getInt("io.file.buffer.size", 4096), replication, blockSize);
      LogFileKey key = new LogFileKey();
      key.event = OPEN;
      key.tserverSession = filename;
      key.filename = filename;
      write(key, EMPTY);
      log.debug("Got new write-ahead log: " + this);
    } catch (IOException ex) {
      if (logFile != null)
        logFile.close();
      logFile = null;
      throw ex;
    }
    
    Thread t = new Daemon(new LogSyncingTask());
    t.setName("Accumulo WALog thread " + toString());
    t.start();
  }
  
  /* (non-Javadoc)
   * @see org.apache.accumulo.server.tabletserver.log.IRemoteLogger#toString()
   */
  @Override
  public String toString() {
    return getLogger() + "/" + getFileName();
  }
  
  public String getLogger() {
    return logger;
  }
  
  public String getFileName() {
    return logPath.getName();
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
      while (!workQueue.isEmpty())
        try {
          closeLock.wait();
        } catch (InterruptedException e) {
          log.info("Interrupted");
        }
    }

    if (logFile != null)
      try {
        logFile.close();
      } catch (IOException ex) {
        log.error(ex);
        throw new LogClosedException();
      }
  }
  
  public synchronized void defineTablet(int seq, int tid, KeyExtent tablet) throws IOException {
    // write this log to the METADATA table
    final LogFileKey key = new LogFileKey();
    key.event = DEFINE_TABLET;
    key.seq = seq;
    key.tid = tid;
    key.tablet = tablet;
    try {
      write(key, EMPTY);
      logFile.sync();
    } catch (IOException ex) {
      log.error(ex);
      throw ex;
    }
  }
  
  /**
   * @param key
   * @param empty2
   * @throws IOException
   */
  private synchronized void write(LogFileKey key, LogFileValue value) throws IOException {
    key.write(logFile);
    value.write(logFile);
  }

  public LoggerOperation log(int seq, int tid, Mutation mutation) throws IOException {
    return logManyTablets(Collections.singletonList(new TabletMutations(tid, seq, Collections.singletonList(mutation.toThrift()))));
  }
  
  public LoggerOperation logManyTablets(List<TabletMutations> mutations) throws IOException {
    DfsLogger.LogWork work = new DfsLogger.LogWork(mutations, new CountDownLatch(1));
    
    synchronized (DfsLogger.this) {
      try {
        for (TabletMutations mutation : mutations) {
          LogFileKey key = new LogFileKey();
          key.event = MANY_MUTATIONS;
          key.seq = mutation.seq;
          key.tid = mutation.tabletID;
          LogFileValue value = new LogFileValue();
          Mutation[] m = new Mutation[mutation.mutations.size()];
          for (int i = 0; i < m.length; i++)
            m[i] = new Mutation(mutation.mutations.get(i));
          value.mutations = m;
          write(key, value);
        }
      } catch (Exception e) {
        log.error(e, e);
        work.exception = e;
      }
    }

    synchronized (closeLock) {
      // use a different lock for close check so that adding to work queue does not need
      // to wait on walog I/O operations

      if (closed)
        throw new LogClosedException();
      workQueue.add(work);
    }

    return new LoggerOperation(work);
  }
  
  public synchronized void minorCompactionFinished(int seq, int tid, String fqfn) throws IOException {
    LogFileKey key = new LogFileKey();
    key.event = COMPACTION_FINISH;
    key.seq = seq;
    key.tid = tid;
    try {
      write(key, EMPTY);
    } catch (IOException ex) {
      log.error(ex);
      throw ex;
    }
  }
  
  public synchronized void minorCompactionStarted(int seq, int tid, String fqfn) throws IOException {
    LogFileKey key = new LogFileKey();
    key.event = COMPACTION_START;
    key.seq = seq;
    key.tid = tid;
    key.filename = fqfn;
    try {
      write(key, EMPTY);
    } catch (IOException ex) {
      log.error(ex);
      throw ex;
    }
  }
  
}
