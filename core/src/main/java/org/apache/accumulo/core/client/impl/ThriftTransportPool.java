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
package org.apache.accumulo.core.client.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.Pair;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class ThriftTransportPool {

  private static final Random random = new Random();
  private long killTime = 1000 * 3;

  private Map<ThriftTransportKey,List<CachedConnection>> cache = new HashMap<>();
  private Map<ThriftTransportKey,Long> errorCount = new HashMap<>();
  private Map<ThriftTransportKey,Long> errorTime = new HashMap<>();
  private Set<ThriftTransportKey> serversWarnedAbout = new HashSet<>();

  private CountDownLatch closerExitLatch;

  private static final Logger log = LoggerFactory.getLogger(ThriftTransportPool.class);

  private static final Long ERROR_THRESHOLD = 20l;
  private static final int STUCK_THRESHOLD = 2 * 60 * 1000;

  private static class CachedConnection {

    public CachedConnection(CachedTTransport t) {
      this.transport = t;
    }

    void setReserved(boolean reserved) {
      this.transport.setReserved(reserved);
    }

    boolean isReserved() {
      return this.transport.reserved;
    }

    CachedTTransport transport;

    long lastReturnTime;
  }

  public static class TransportPoolShutdownException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }

  private static class Closer implements Runnable {
    final ThriftTransportPool pool;
    private CountDownLatch closerExitLatch;

    public Closer(ThriftTransportPool pool, CountDownLatch closerExitLatch) {
      this.pool = pool;
      this.closerExitLatch = closerExitLatch;
    }

    private void closeConnections() {
      while (true) {

        ArrayList<CachedConnection> connectionsToClose = new ArrayList<>();

        synchronized (pool) {
          for (List<CachedConnection> ccl : pool.getCache().values()) {
            Iterator<CachedConnection> iter = ccl.iterator();
            while (iter.hasNext()) {
              CachedConnection cachedConnection = iter.next();

              if (!cachedConnection.isReserved() && System.currentTimeMillis() - cachedConnection.lastReturnTime > pool.killTime) {
                connectionsToClose.add(cachedConnection);
                iter.remove();
              }
            }
          }

          for (List<CachedConnection> ccl : pool.getCache().values()) {
            for (CachedConnection cachedConnection : ccl) {
              cachedConnection.transport.checkForStuckIO(STUCK_THRESHOLD);
            }
          }

          Iterator<Entry<ThriftTransportKey,Long>> iter = pool.errorTime.entrySet().iterator();
          while (iter.hasNext()) {
            Entry<ThriftTransportKey,Long> entry = iter.next();
            long delta = System.currentTimeMillis() - entry.getValue();
            if (delta >= STUCK_THRESHOLD) {
              pool.errorCount.remove(entry.getKey());
              iter.remove();
            }
          }
        }

        // close connections outside of sync block
        for (CachedConnection cachedConnection : connectionsToClose) {
          cachedConnection.transport.close();
        }

        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          log.debug("Sleep interrupted in closeConnections()", e);
        }
      }
    }

    @Override
    public void run() {
      try {
        closeConnections();
      } catch (TransportPoolShutdownException e) {} finally {
        closerExitLatch.countDown();
      }
    }
  }

  static class CachedTTransport extends TTransport {

    private ThriftTransportKey cacheKey;
    private TTransport wrappedTransport;
    private boolean sawError = false;

    private volatile String ioThreadName = null;
    private volatile long ioStartTime = 0;
    private volatile boolean reserved = false;

    private String stuckThreadName = null;

    int ioCount = 0;
    int lastIoCount = -1;

    private void sawError(Exception e) {
      sawError = true;
    }

    final void setReserved(boolean reserved) {
      this.reserved = reserved;
      if (reserved) {
        ioThreadName = Thread.currentThread().getName();
        ioCount = 0;
        lastIoCount = -1;
      } else {
        if ((ioCount & 1) == 1) {
          // connection unreserved, but it seems io may still be happening
          log.warn("Connection returned to thrift connection pool that may still be in use {} {}", ioThreadName, Thread.currentThread().getName(),
              new Exception());
        }

        ioCount = 0;
        lastIoCount = -1;
        ioThreadName = null;
      }
      checkForStuckIO(STUCK_THRESHOLD);
    }

    final void checkForStuckIO(long threshold) {
      // checking for stuck io needs to be light weight.

      // Tried to call System.currentTimeMillis() and Thread.currentThread() before every io operation.... this dramatically slowed things down. So switched to
      // incrementing a counter before and after each io operation.

      if ((ioCount & 1) == 1) {
        // when ioCount is odd, it means I/O is currently happening
        if (ioCount == lastIoCount) {
          // still doing same I/O operation as last time this
          // functions was called
          long delta = System.currentTimeMillis() - ioStartTime;
          if (delta >= threshold && stuckThreadName == null) {
            stuckThreadName = ioThreadName;
            log.warn("Thread \"{}\" stuck on IO to {} for at least {} ms", ioThreadName, cacheKey, delta);
          }
        } else {
          // remember this ioCount and the time we saw it, need to see
          // if it changes
          lastIoCount = ioCount;
          ioStartTime = System.currentTimeMillis();

          if (stuckThreadName != null) {
            // doing I/O, but ioCount changed so no longer stuck
            log.info("Thread \"{}\" no longer stuck on IO to {} sawError = {}", stuckThreadName, cacheKey, sawError);
            stuckThreadName = null;
          }
        }
      } else {
        // I/O is not currently happening
        if (stuckThreadName != null) {
          // no longer stuck, and was stuck in the past
          log.info("Thread \"{}\" no longer stuck on IO to {} sawError = {}", stuckThreadName, cacheKey, sawError);
          stuckThreadName = null;
        }
      }
    }

    public CachedTTransport(TTransport transport, ThriftTransportKey cacheKey2) {
      this.wrappedTransport = transport;
      this.cacheKey = cacheKey2;
    }

    @Override
    public boolean isOpen() {
      return wrappedTransport.isOpen();
    }

    @Override
    public void open() throws TTransportException {
      try {
        ioCount++;
        wrappedTransport.open();
      } catch (TTransportException tte) {
        sawError(tte);
        throw tte;
      } finally {
        ioCount++;
      }
    }

    @Override
    public int read(byte[] arg0, int arg1, int arg2) throws TTransportException {
      try {
        ioCount++;
        return wrappedTransport.read(arg0, arg1, arg2);
      } catch (TTransportException tte) {
        sawError(tte);
        throw tte;
      } finally {
        ioCount++;
      }
    }

    @Override
    public int readAll(byte[] arg0, int arg1, int arg2) throws TTransportException {
      try {
        ioCount++;
        return wrappedTransport.readAll(arg0, arg1, arg2);
      } catch (TTransportException tte) {
        sawError(tte);
        throw tte;
      } finally {
        ioCount++;
      }
    }

    @Override
    public void write(byte[] arg0, int arg1, int arg2) throws TTransportException {
      try {
        ioCount++;
        wrappedTransport.write(arg0, arg1, arg2);
      } catch (TTransportException tte) {
        sawError(tte);
        throw tte;
      } finally {
        ioCount++;
      }
    }

    @Override
    public void write(byte[] arg0) throws TTransportException {
      try {
        ioCount++;
        wrappedTransport.write(arg0);
      } catch (TTransportException tte) {
        sawError(tte);
        throw tte;
      } finally {
        ioCount++;
      }
    }

    @Override
    public void close() {
      try {
        ioCount++;
        wrappedTransport.close();
      } finally {
        ioCount++;
      }

    }

    @Override
    public void flush() throws TTransportException {
      try {
        ioCount++;
        wrappedTransport.flush();
      } catch (TTransportException tte) {
        sawError(tte);
        throw tte;
      } finally {
        ioCount++;
      }
    }

    @Override
    public boolean peek() {
      try {
        ioCount++;
        return wrappedTransport.peek();
      } finally {
        ioCount++;
      }
    }

    @Override
    public byte[] getBuffer() {
      try {
        ioCount++;
        return wrappedTransport.getBuffer();
      } finally {
        ioCount++;
      }
    }

    @Override
    public int getBufferPosition() {
      try {
        ioCount++;
        return wrappedTransport.getBufferPosition();
      } finally {
        ioCount++;
      }
    }

    @Override
    public int getBytesRemainingInBuffer() {
      try {
        ioCount++;
        return wrappedTransport.getBytesRemainingInBuffer();
      } finally {
        ioCount++;
      }
    }

    @Override
    public void consumeBuffer(int len) {
      try {
        ioCount++;
        wrappedTransport.consumeBuffer(len);
      } finally {
        ioCount++;
      }
    }

    public ThriftTransportKey getCacheKey() {
      return cacheKey;
    }

  }

  private ThriftTransportPool() {}

  public TTransport getTransport(HostAndPort location, long milliseconds, ClientContext context) throws TTransportException {
    return getTransport(new ThriftTransportKey(location, milliseconds, context));
  }

  private TTransport getTransport(ThriftTransportKey cacheKey) throws TTransportException {
    synchronized (this) {
      // atomically reserve location if it exist in cache
      List<CachedConnection> ccl = getCache().get(cacheKey);

      if (ccl == null) {
        ccl = new LinkedList<>();
        getCache().put(cacheKey, ccl);
      }

      for (CachedConnection cachedConnection : ccl) {
        if (!cachedConnection.isReserved()) {
          cachedConnection.setReserved(true);
          log.trace("Using existing connection to {}", cacheKey.getServer());
          return cachedConnection.transport;
        }
      }
    }

    return createNewTransport(cacheKey);
  }

  @VisibleForTesting
  public Pair<String,TTransport> getAnyTransport(List<ThriftTransportKey> servers, boolean preferCachedConnection) throws TTransportException {

    servers = new ArrayList<>(servers);

    if (preferCachedConnection) {
      HashSet<ThriftTransportKey> serversSet = new HashSet<>(servers);

      synchronized (this) {

        // randomly pick a server from the connection cache
        serversSet.retainAll(getCache().keySet());

        if (serversSet.size() > 0) {
          ArrayList<ThriftTransportKey> cachedServers = new ArrayList<>(serversSet);
          Collections.shuffle(cachedServers, random);

          for (ThriftTransportKey ttk : cachedServers) {
            for (CachedConnection cachedConnection : getCache().get(ttk)) {
              if (!cachedConnection.isReserved()) {
                cachedConnection.setReserved(true);
                final String serverAddr = ttk.getServer().toString();
                log.trace("Using existing connection to {}", serverAddr);
                return new Pair<>(serverAddr, cachedConnection.transport);
              }
            }
          }
        }
      }
    }

    int retryCount = 0;
    while (servers.size() > 0 && retryCount < 10) {
      int index = random.nextInt(servers.size());
      ThriftTransportKey ttk = servers.get(index);

      if (preferCachedConnection) {
        synchronized (this) {
          List<CachedConnection> cachedConnList = getCache().get(ttk);
          if (cachedConnList != null) {
            for (CachedConnection cachedConnection : cachedConnList) {
              if (!cachedConnection.isReserved()) {
                cachedConnection.setReserved(true);
                final String serverAddr = ttk.getServer().toString();
                log.trace("Using existing connection to {} timeout {}", serverAddr, ttk.getTimeout());
                return new Pair<>(serverAddr, cachedConnection.transport);
              }
            }
          }
        }
      }

      try {
        return new Pair<>(ttk.getServer().toString(), createNewTransport(ttk));
      } catch (TTransportException tte) {
        log.debug("Failed to connect to {}", servers.get(index), tte);
        servers.remove(index);
        retryCount++;
      }
    }

    throw new TTransportException("Failed to connect to a server");
  }

  private TTransport createNewTransport(ThriftTransportKey cacheKey) throws TTransportException {
    TTransport transport = ThriftUtil.createClientTransport(cacheKey.getServer(), (int) cacheKey.getTimeout(), cacheKey.getSslParams(),
        cacheKey.getSaslParams());

    log.trace("Creating new connection to connection to {}", cacheKey.getServer());

    CachedTTransport tsc = new CachedTTransport(transport, cacheKey);

    CachedConnection cc = new CachedConnection(tsc);
    cc.setReserved(true);

    try {
      synchronized (this) {
        List<CachedConnection> ccl = getCache().get(cacheKey);

        if (ccl == null) {
          ccl = new LinkedList<>();
          getCache().put(cacheKey, ccl);
        }

        ccl.add(cc);
      }
    } catch (TransportPoolShutdownException e) {
      cc.transport.close();
      throw e;
    }
    return cc.transport;
  }

  public void returnTransport(TTransport tsc) {
    if (tsc == null) {
      return;
    }

    boolean existInCache = false;
    CachedTTransport ctsc = (CachedTTransport) tsc;

    ArrayList<CachedConnection> closeList = new ArrayList<>();

    synchronized (this) {
      List<CachedConnection> ccl = getCache().get(ctsc.getCacheKey());
      for (Iterator<CachedConnection> iterator = ccl.iterator(); iterator.hasNext();) {
        CachedConnection cachedConnection = iterator.next();
        if (cachedConnection.transport == tsc) {
          if (ctsc.sawError) {
            closeList.add(cachedConnection);
            iterator.remove();

            log.trace("Returned connection had error {}", ctsc.getCacheKey());

            Long ecount = errorCount.get(ctsc.getCacheKey());
            if (ecount == null)
              ecount = 0l;
            ecount++;
            errorCount.put(ctsc.getCacheKey(), ecount);

            Long etime = errorTime.get(ctsc.getCacheKey());
            if (etime == null) {
              errorTime.put(ctsc.getCacheKey(), System.currentTimeMillis());
            }

            if (ecount >= ERROR_THRESHOLD && !serversWarnedAbout.contains(ctsc.getCacheKey())) {
              log.warn("Server {} had {} failures in a short time period, will not complain anymore", ctsc.getCacheKey(), ecount);
              serversWarnedAbout.add(ctsc.getCacheKey());
            }

            cachedConnection.setReserved(false);

          } else {
            log.trace("Returned connection {} ioCount: {}", ctsc.getCacheKey(), cachedConnection.transport.ioCount);

            cachedConnection.lastReturnTime = System.currentTimeMillis();
            cachedConnection.setReserved(false);
          }
          existInCache = true;
          break;
        }
      }

      // remove all unreserved cached connection when a sever has an error, not just the connection that was returned
      if (ctsc.sawError) {
        for (Iterator<CachedConnection> iterator = ccl.iterator(); iterator.hasNext();) {
          CachedConnection cachedConnection = iterator.next();
          if (!cachedConnection.isReserved()) {
            closeList.add(cachedConnection);
            iterator.remove();
          }
        }
      }
    }

    // close outside of sync block
    for (CachedConnection cachedConnection : closeList) {
      try {
        cachedConnection.transport.close();
      } catch (Exception e) {
        log.debug("Failed to close connection w/ errors", e);
      }
    }

    if (!existInCache) {
      log.warn("Returned tablet server connection to cache that did not come from cache");
      // close outside of sync block
      tsc.close();
    }
  }

  /**
   * Set the time after which idle connections should be closed
   */
  public synchronized void setIdleTime(long time) {
    this.killTime = time;
    log.debug("Set thrift transport pool idle time to {}", time);
  }

  private static ThriftTransportPool instance = new ThriftTransportPool();
  private static final AtomicBoolean daemonStarted = new AtomicBoolean(false);

  public static ThriftTransportPool getInstance() {
    if (daemonStarted.compareAndSet(false, true)) {
      CountDownLatch closerExitLatch = new CountDownLatch(1);
      new Daemon(new Closer(instance, closerExitLatch), "Thrift Connection Pool Checker").start();
      instance.setCloserExitLatch(closerExitLatch);
    }
    return instance;
  }

  private synchronized void setCloserExitLatch(CountDownLatch closerExitLatch) {
    this.closerExitLatch = closerExitLatch;
  }

  public void shutdown() {
    synchronized (this) {
      if (cache == null)
        return;

      // close any connections in the pool... even ones that are in use
      for (List<CachedConnection> ccl : getCache().values()) {
        Iterator<CachedConnection> iter = ccl.iterator();
        while (iter.hasNext()) {
          CachedConnection cc = iter.next();
          try {
            cc.transport.close();
          } catch (Exception e) {
            log.debug("Error closing transport during shutdown", e);
          }
        }
      }

      // this will render the pool unusable and cause the background thread to exit
      this.cache = null;
    }

    try {
      closerExitLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<ThriftTransportKey,List<CachedConnection>> getCache() {
    if (cache == null)
      throw new TransportPoolShutdownException();
    return cache;
  }
}
