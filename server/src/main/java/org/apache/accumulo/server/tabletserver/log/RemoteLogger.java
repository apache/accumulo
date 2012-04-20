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
package org.apache.accumulo.server.tabletserver.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.LogCopyInfo;
import org.apache.accumulo.core.tabletserver.thrift.LogFile;
import org.apache.accumulo.core.tabletserver.thrift.LoggerClosedException;
import org.apache.accumulo.core.tabletserver.thrift.MutationLogger;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchLogIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletMutations;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

/**
 * Wrap a connection to a logger.
 * 
 */
public class RemoteLogger {
  private static Logger log = Logger.getLogger(RemoteLogger.class);
  
  private LinkedBlockingQueue<LogWork> workQueue = new LinkedBlockingQueue<LogWork>();
  
  private String closeLock = new String("foo");
  
  private static final LogWork CLOSED_MARKER = new LogWork(null, null);
  
  private boolean closed = false;

  public static class LoggerOperation {
    private LogWork work;
    
    public LoggerOperation(LogWork work) {
      this.work = work;
    }
    
    public void await() throws NoSuchLogIDException, LoggerClosedException, TException {
      try {
        work.latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      
      if (work.exception != null) {
        if (work.exception instanceof NoSuchLogIDException)
          throw (NoSuchLogIDException) work.exception;
        else if (work.exception instanceof LoggerClosedException)
          throw (LoggerClosedException) work.exception;
        else if (work.exception instanceof TException)
          throw (TException) work.exception;
        else if (work.exception instanceof RuntimeException)
          throw (RuntimeException) work.exception;
        else
          throw new RuntimeException(work.exception);
      }
    }
  }

  private static class LogWork {
    List<TabletMutations> mutations;
    CountDownLatch latch;
    volatile Exception exception;
    
    public LogWork(List<TabletMutations> mutations, CountDownLatch latch) {
      this.mutations = mutations;
      this.latch = latch;
    }
  }
  
  private class LogWriterTask implements Runnable {

    @Override
    public void run() {
      try {
        ArrayList<LogWork> work = new ArrayList<LogWork>();
        ArrayList<TabletMutations> mutations = new ArrayList<TabletMutations>();
        while (true) {
          
          work.clear();
          mutations.clear();
          
          work.add(workQueue.take());
          workQueue.drainTo(work);
          
          for (LogWork logWork : work)
            if (logWork != CLOSED_MARKER)
              mutations.addAll(logWork.mutations);
          
          synchronized (RemoteLogger.this) {
            try {
              client.logManyTablets(null, logFile.id, mutations);
            } catch (Exception e) {
              for (LogWork logWork : work)
                if (logWork != CLOSED_MARKER)
                  logWork.exception = e;
            }
          }
          
          boolean sawClosedMarker = false;
          for (LogWork logWork : work)
            if (logWork == CLOSED_MARKER)
              sawClosedMarker = true;
            else
              logWork.latch.countDown();
          
          if (sawClosedMarker)
            break;
        }
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    }
  }

  @Override
  public boolean equals(Object obj) {
    // filename is unique
    if (obj == null)
      return false;
    if (obj instanceof RemoteLogger)
      return getFileName().equals(((RemoteLogger) obj).getFileName());
    return false;
  }
  
  @Override
  public int hashCode() {
    // filename is unique
    return getFileName().hashCode();
  }
  
  private final String logger;
  private final LogFile logFile;
  private final UUID tserverSession;
  private MutationLogger.Iface client = null;
  
  public RemoteLogger(String address, UUID tserverUUID, AccumuloConfiguration conf) throws ThriftSecurityException, LoggerClosedException, TException,
      IOException {
    
    logger = address;
    tserverSession = tserverUUID;
    try {
      client = ThriftUtil.getClient(new MutationLogger.Client.Factory(), address, Property.LOGGER_PORT, Property.TSERV_LOGGER_TIMEOUT, conf);
      logFile = client.create(null, SecurityConstants.getSystemCredentials(), tserverSession.toString());
      log.debug("Got new write-ahead log: " + this);
    } catch (ThriftSecurityException tse) {
      ThriftUtil.returnClient(client);
      client = null;
      throw tse;
    } catch (LoggerClosedException lce) {
      ThriftUtil.returnClient(client);
      client = null;
      throw lce;
    } catch (TException te) {
      ThriftUtil.returnClient(client);
      client = null;
      throw te;
    }
    
    Thread t = new Daemon(new LogWriterTask());
    t.setName("Accumulo WALog thread " + toString());
    t.start();
  }
  
  public RemoteLogger(String address, AccumuloConfiguration conf) throws IOException {
    logger = address;
    tserverSession = null;
    logFile = null;
    try {
      client = ThriftUtil.getClient(new MutationLogger.Client.Factory(), address, Property.LOGGER_PORT, Property.TSERV_LOGGER_TIMEOUT, conf);
    } catch (TTransportException e) {
      throw new IOException(e);
    }
  }
  
  // Fake placeholder for logs used during recovery
  public RemoteLogger(String logger, String filename, UUID tserverUUID) {
    this.client = null;
    this.logger = logger;
    this.logFile = new LogFile(filename, -1);
    this.tserverSession = null;
  }
  
  @Override
  public String toString() {
    return getLogger() + "/" + getFileName();
  }
  
  public String getLogger() {
    return logger;
  }
  
  public String getFileName() {
    return logFile.name;
  }
  
  public synchronized void close() throws NoSuchLogIDException, LoggerClosedException, TException {
    
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

    try {
      if (client != null)
        client.close(null, logFile.id);
    } finally {
      MutationLogger.Iface tmp = client;
      client = null;
      ThriftUtil.returnClient(tmp);
    }
  }
  
  public synchronized void defineTablet(int seq, int tid, KeyExtent tablet) throws NoSuchLogIDException, LoggerClosedException, TException {
    client.defineTablet(null, logFile.id, seq, tid, tablet.toThrift());
  }
  
  public LoggerOperation log(int seq, int tid, Mutation mutation) throws NoSuchLogIDException, LoggerClosedException, TException {
    return logManyTablets(Collections.singletonList(new TabletMutations(tid, seq, Collections.singletonList(mutation.toThrift()))));
  }
  
  public LoggerOperation logManyTablets(List<TabletMutations> mutations) throws NoSuchLogIDException, LoggerClosedException, TException {
    LogWork work = new LogWork(mutations, new CountDownLatch(1));
    
    synchronized (closeLock) {
      // use a differnt lock for close check so that adding to work queue does not need
      // to wait on walog I/O operations

      if (closed)
        throw new NoSuchLogIDException();
      workQueue.add(work);
    }

    return new LoggerOperation(work);
  }
  
  public synchronized void minorCompactionFinished(int seq, int tid, String fqfn) throws NoSuchLogIDException, LoggerClosedException, TException {
    client.minorCompactionFinished(null, logFile.id, seq, tid, fqfn);
  }
  
  public synchronized void minorCompactionStarted(int seq, int tid, String fqfn) throws NoSuchLogIDException, LoggerClosedException, TException {
    client.minorCompactionStarted(null, logFile.id, seq, tid, fqfn);
  }
  
  public synchronized LogCopyInfo startCopy(String name, String fullyQualifiedFileName) throws ThriftSecurityException, TException {
    return client.startCopy(null, SecurityConstants.getSystemCredentials(), name, fullyQualifiedFileName, true);
  }
  
  public synchronized List<String> getClosedLogs() throws ThriftSecurityException, TException {
    return client.getClosedLogs(null, SecurityConstants.getSystemCredentials());
  }
  
  public synchronized void removeFile(List<String> files) throws ThriftSecurityException, TException {
    client.remove(null, SecurityConstants.getSystemCredentials(), files);
  }
  
}
