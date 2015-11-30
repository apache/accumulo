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

import java.security.SecurityPermission;
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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.SslConnectionParams;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;

public class ThriftTransportPool {
  private static SecurityPermission TRANSPORT_POOL_PERMISSION = new SecurityPermission("transportPoolPermission");

  private static final Random random = new Random();
  private long killTime = 1000 * 3;

  // Caches for Thrift connections to servers -- see ACCUMULO-4065
  //
  // For use with "normal" (not marked oneway) Thrift calls. Only one user at a time (no concurrent use).
  private Map<ThriftTransportKey,List<CachedConnection>> cache = new HashMap<ThriftTransportKey,List<CachedConnection>>();
  // For oneway thrift calls. Only one client can issue one calls at a time, but multiple calls can be ACKed under
  // the hood concurrently (oneway calls still have a response from the server, but the client never sees that response).
  // If a non-oneway thread was waiting for its response after a oneway call, it might see the oneway call's response, not its own.
  // If we only make oneway calls (which ignore the responses), we should be safe.
  private Map<ThriftTransportKey,List<CachedConnection>> onewayCache = new HashMap<ThriftTransportKey,List<CachedConnection>>();

  private Map<ThriftTransportKey,Long> errorCount = new HashMap<ThriftTransportKey,Long>();
  private Map<ThriftTransportKey,Long> errorTime = new HashMap<ThriftTransportKey,Long>();
  private Set<ThriftTransportKey> serversWarnedAbout = new HashSet<ThriftTransportKey>();

  private CountDownLatch closerExitLatch;

  private static final Logger log = Logger.getLogger(ThriftTransportPool.class);

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

    final CachedTTransport transport;

    long lastReturnTime;
  }

  public static class TransportPoolShutdownException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }

  /**
   * Runnable for a thread which cleans up idle or errored connections to servers. Visible for testing only.
   */
  public static class Closer implements Runnable {
    final ThriftTransportPool pool;
    private final CountDownLatch closerExitLatch;
    private final AtomicBoolean keepRunning;

    public Closer(ThriftTransportPool pool, CountDownLatch closerExitLatch, AtomicBoolean keepRunning) {
      this.pool = pool;
      this.closerExitLatch = closerExitLatch;
      this.keepRunning = keepRunning;
    }

    private void closeConnections() {
      while (keepRunning.get()) {

        ArrayList<CachedConnection> connectionsToClose = new ArrayList<CachedConnection>();

        synchronized (pool) {
          // Collect any "idle" connections and remove them from the cache
          collectIdleConnections(connectionsToClose, pool.getCache(), pool.killTime);

          // Check for any stuck connections
          for (List<CachedConnection> ccl : pool.getCache().values()) {
            for (CachedConnection cachedConnection : ccl) {
              cachedConnection.transport.checkForStuckIO(STUCK_THRESHOLD);
            }
          }

          // Collect any "idle" oneway connections and remove them from the oneway cache
          collectIdleConnections(connectionsToClose, pool.getOnewayCache(), pool.killTime);

          // Ignore checking for stuck oneway connections since we aren't blocked on those responses.

          // Clean up any errors in memory
          Iterator<Entry<ThriftTransportKey,Long>> errorTimeIter = pool.errorTime.entrySet().iterator();
          while (errorTimeIter.hasNext()) {
            Entry<ThriftTransportKey,Long> entry = errorTimeIter.next();
            long delta = System.currentTimeMillis() - entry.getValue();
            if (delta >= STUCK_THRESHOLD) {
              pool.errorCount.remove(entry.getKey());
              errorTimeIter.remove();
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
          e.printStackTrace();
        }
      }
    }

    /**
     * Inspect the <code>cache</code> of connections, adding any whose idle time exceed <code>idleTimeout</code> in age to
     * <code>connectionsToClose</code> and removing them from the <code>cache</code>.
     *
     * @param connectionsToClose A list which connections to close will be added into.
     * @param cache The cache of thrift connections.
     * @param idleTimeout The number of milliseconds since last use that connections should be considered "idle".
     */
    private void collectIdleConnections(ArrayList<CachedConnection> connectionsToClose, Map<ThriftTransportKey,List<CachedConnection>> cache, long idleTimeout) {
      // Collect any "idle" connections and remove them from the cache
      for (List<CachedConnection> ccl : cache.values()) {
        Iterator<CachedConnection> iter = ccl.iterator();
        while (iter.hasNext()) {
          CachedConnection cachedConnection = iter.next();

          if (!cachedConnection.isReserved() && System.currentTimeMillis() - cachedConnection.lastReturnTime > idleTimeout) {
            connectionsToClose.add(cachedConnection);
            iter.remove();
          }
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

  /**
   * Wrapper around a Thrift TTransport that does I/O counting (for "stuckness"), error tracking, and reservations.
   */
  public static class CachedTTransport extends TTransport {

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
          // connection unreserved, but it seems io may still be
          // happening
          log.warn("Connection returned to thrift connection pool that may still be in use " + ioThreadName + " " + Thread.currentThread().getName(),
              new Exception());
        }

        ioCount = 0;
        lastIoCount = -1;
        ioThreadName = null;
      }
      checkForStuckIO(STUCK_THRESHOLD);
    }

    final void checkForStuckIO(long threshold) {
      /*
       * checking for stuck io needs to be light weight.
       *
       * Tried to call System.currentTimeMillis() and Thread.currentThread() before every io operation.... this dramatically slowed things down. So switched to
       * incrementing a counter before and after each io operation.
       */

      if ((ioCount & 1) == 1) {
        // when ioCount is odd, it means I/O is currently happening
        if (ioCount == lastIoCount) {
          // still doing same I/O operation as last time this
          // functions was called
          long delta = System.currentTimeMillis() - ioStartTime;
          if (delta >= threshold && stuckThreadName == null) {
            stuckThreadName = ioThreadName;
            log.warn("Thread \"" + ioThreadName + "\" stuck on IO  to " + cacheKey + " for at least " + delta + " ms");
          }
        } else {
          // remember this ioCount and the time we saw it, need to see
          // if it changes
          lastIoCount = ioCount;
          ioStartTime = System.currentTimeMillis();

          if (stuckThreadName != null) {
            // doing I/O, but ioCount changed so no longer stuck
            log.info("Thread \"" + stuckThreadName + "\" no longer stuck on IO  to " + cacheKey + " sawError = " + sawError);
            stuckThreadName = null;
          }
        }
      } else {
        // I/O is not currently happening
        if (stuckThreadName != null) {
          // no longer stuck, and was stuck in the past
          log.info("Thread \"" + stuckThreadName + "\" no longer stuck on IO  to " + cacheKey + " sawError = " + sawError);
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

  /**
   * Visible <em>ONLY</em> for testing. Not for general purpose use.
   *
   * @param killTime The amount of time before reaping idle connections.
   */
  public ThriftTransportPool(long killTime) {
    this.killTime = killTime;
  }

  public TTransport getTransportWithDefaultTimeout(HostAndPort addr, AccumuloConfiguration conf, boolean oneway) throws TTransportException {
    return getTransport(String.format("%s:%d", addr.getHostText(), addr.getPort()), conf.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT),
        SslConnectionParams.forClient(conf), oneway);
  }

  public TTransport getTransport(String location, long milliseconds, SslConnectionParams sslParams, boolean oneway) throws TTransportException {
    return getTransport(new ThriftTransportKey(location, milliseconds, sslParams, oneway));
  }

  private void logConnectionCreation(ThriftTransportKey cacheKey, TTransport transport) {
    logConnectionMessage("Created new ", cacheKey, transport);
  }

  private void logConnectionReuse(ThriftTransportKey cacheKey, TTransport transport) {
    logConnectionMessage("Using existing ", cacheKey, transport);
  }

  private void logConnectionMessage(String prefix, ThriftTransportKey cacheKey, TTransport transport) {
    log.trace(prefix + (cacheKey.isOneway() ? "oneway " : "") + "connection to " + cacheKey.getLocation() + ":" + cacheKey.getPort() + " "
        + Integer.toHexString(transport.hashCode()));
  }

  private TTransport getTransport(ThriftTransportKey cacheKey) throws TTransportException {
    synchronized (this) {
      // Choose the appropriate cache to pull from
      final Map<ThriftTransportKey,List<CachedConnection>> localCache;
      if (cacheKey.isOneway()) {
        localCache = getOnewayCache();
      } else {
        localCache = getCache();
      }

      // atomically reserve location if it exist in cache
      List<CachedConnection> ccl = localCache.get(cacheKey);

      // Make sure we put the list back into the cache if it wasn't there already.
      if (ccl == null) {
        ccl = new LinkedList<CachedConnection>();
        localCache.put(cacheKey, ccl);
      }

      // Find a connection that isn't reserved
      for (CachedConnection cachedConnection : ccl) {
        if (!cachedConnection.isReserved()) {
          cachedConnection.setReserved(true);
          // Log the reuse of the connection
          if (log.isTraceEnabled()) {
            logConnectionReuse(cacheKey, cachedConnection.transport);
          }

          return cachedConnection.transport;
        }
      }
    }

    return createNewTransport(cacheKey);
  }

  @VisibleForTesting
  public Pair<String,TTransport> getAnyTransport(List<ThriftTransportKey> servers, boolean preferCachedConnection, boolean oneway) throws TTransportException {

    servers = new ArrayList<ThriftTransportKey>(servers);

    if (preferCachedConnection) {
      final HashSet<ThriftTransportKey> serversSet = new HashSet<ThriftTransportKey>(servers);

      synchronized (this) {
        final Map<ThriftTransportKey,List<CachedConnection>> localCache;
        if (oneway) {
          localCache = getOnewayCache();
        } else {
          localCache = getCache();
        }

        // randomly pick a server from the connection cache
        serversSet.retainAll(localCache.keySet());

        if (serversSet.size() > 0) {
          ArrayList<ThriftTransportKey> cachedServers = new ArrayList<ThriftTransportKey>(serversSet);
          Collections.shuffle(cachedServers, random);

          for (ThriftTransportKey ttk : cachedServers) {
            // If any of the connection are unreserved, reserve and return it.
            for (CachedConnection cachedConnection : localCache.get(ttk)) {
              if (!cachedConnection.isReserved()) {
                cachedConnection.setReserved(true);
                if (log.isTraceEnabled()) {
                  logConnectionReuse(ttk, cachedConnection.transport);
                }
                return new Pair<String,TTransport>(ttk.getLocation() + ":" + ttk.getPort(), cachedConnection.transport);
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

      // Always try to choose a cached connection before making a new connection
      if (preferCachedConnection) {
        synchronized (this) {
          final Map<ThriftTransportKey,List<CachedConnection>> localCache;
          if (oneway) {
            localCache = getOnewayCache();
          } else {
            localCache = getCache();
          }

          // Try to find an unreserved connection to the server, then reserve and return it.
          List<CachedConnection> cachedConnList = localCache.get(ttk);
          if (cachedConnList != null) {
            for (CachedConnection cachedConnection : cachedConnList) {
              if (!cachedConnection.isReserved()) {
                cachedConnection.setReserved(true);
                if (log.isTraceEnabled()) {
                  logConnectionReuse(ttk, cachedConnection.transport);
                }
                return new Pair<String,TTransport>(ttk.getLocation() + ":" + ttk.getPort(), cachedConnection.transport);
              }
            }
          }
        }
      }

      try {
        return new Pair<String,TTransport>(ttk.getLocation() + ":" + ttk.getPort(), createNewTransport(ttk));
      } catch (TTransportException tte) {
        log.debug("Failed to connect to " + servers.get(index), tte);
        servers.remove(index);
        retryCount++;
      }
    }

    throw new TTransportException("Failed to connect to a server");
  }

  private TTransport createNewTransport(ThriftTransportKey cacheKey) throws TTransportException {
    TTransport transport = ThriftUtil.createClientTransport(HostAndPort.fromParts(cacheKey.getLocation(), cacheKey.getPort()), (int) cacheKey.getTimeout(),
        cacheKey.getSslParams());

    CachedTTransport tsc = new CachedTTransport(transport, cacheKey);

    CachedConnection cc = new CachedConnection(tsc);
    cc.setReserved(true);

    try {
      synchronized (this) {
        Map<ThriftTransportKey,List<CachedConnection>> localCache;
        if (cacheKey.isOneway()) {
          localCache = getOnewayCache();
        } else {
          localCache = getCache();
        }

        List<CachedConnection> ccl = localCache.get(cacheKey);

        if (ccl == null) {
          ccl = new LinkedList<CachedConnection>();
          localCache.put(cacheKey, ccl);
        }

        ccl.add(cc);
      }
    } catch (TransportPoolShutdownException e) {
      cc.transport.close();
      throw e;
    }
    if (log.isTraceEnabled()) {
      logConnectionCreation(cacheKey, cc.transport);
    }
    return cc.transport;
  }

  public void returnTransport(TTransport tsc) {
    if (tsc == null) {
      return;
    }

    boolean existInCache = false;
    CachedTTransport ctsc = (CachedTTransport) tsc;

    ArrayList<TTransport> closeList = new ArrayList<TTransport>();

    synchronized (this) {
      Map<ThriftTransportKey,List<CachedConnection>> localCache;
      if (ctsc.getCacheKey().isOneway()) {
        localCache = getOnewayCache();
      } else {
        localCache = getCache();
      }

      List<CachedConnection> ccl = localCache.get(ctsc.getCacheKey());
      for (Iterator<CachedConnection> iterator = ccl.iterator(); iterator.hasNext();) {
        CachedConnection cachedConnection = iterator.next();
        if (cachedConnection.transport == tsc) {
          if (ctsc.sawError) {
            closeList.add(cachedConnection.transport);
            iterator.remove();

            if (log.isTraceEnabled())
              log.trace("Returned connection had error " + ctsc.getCacheKey() + " " + Integer.toHexString(tsc.hashCode()));

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
              log.warn("Server " + ctsc.getCacheKey() + " had " + ecount + " failures in a short time period, will not complain anymore ");
              serversWarnedAbout.add(ctsc.getCacheKey());
            }

            cachedConnection.setReserved(false);

          } else {
            if (log.isTraceEnabled())
              log.trace("Returned connection " + ctsc.getCacheKey() + " ioCount : " + cachedConnection.transport.ioCount + " " + Integer.toHexString(tsc.hashCode()));

            cachedConnection.lastReturnTime = System.currentTimeMillis();
            cachedConnection.setReserved(false);
          }
          existInCache = true;
          break;
        }
      }

      // remove all unreserved cached connection when a sever has an error, not just the connection that was returned
      if (ctsc.sawError) {
        collectUnreservedTransports(closeList, ccl);

        // The above removed the connections for the cache in use for this connection. We also want to check the opposite.
        final Map<ThriftTransportKey,List<CachedConnection>> otherCache;
        if (ctsc.getCacheKey().isOneway()) {
          // Check the non-oneway cache
          otherCache = getCache();
        } else {
          // Check the oneway cache
          otherCache = getOnewayCache();
        }

        // Make sure we grab unreserved connections to the same server from the other cache too
        final ThriftTransportKey cacheKey = ctsc.getCacheKey();
        for (Entry<ThriftTransportKey,List<CachedConnection>> entry : otherCache.entrySet()) {
          if (cacheKey.isSameLocation(entry.getKey())) {
            collectUnreservedTransports(closeList, entry.getValue());
          }
        }
      }
    }

    // close outside of sync block
    for (TTransport cachedTransport : closeList) {
      try {
        cachedTransport.close();
      } catch (Exception e) {
        log.debug("Failed to close connection w/ errors", e);
      }
    }

    if (!existInCache) {
      log.warn("Returned tablet server connection to cache that did not come from cache: " + Integer.toHexString(tsc.hashCode()));
      // close outside of sync block
      tsc.close();
    }
  }

  private void collectUnreservedTransports(ArrayList<TTransport> transportsToClose, List<CachedConnection> connections) {
    Iterator<CachedConnection> iter = connections.iterator();
    while (iter.hasNext()) {
      CachedConnection cc = iter.next();
      if (!cc.isReserved()) {
        transportsToClose.add(cc.transport);
        iter.remove();
      }
    }
  }

  /**
   * Set the time after which idle connections should be closed
   */
  public synchronized void setIdleTime(long time) {
    this.killTime = time;
    log.debug("Set thrift transport pool idle time to " + time);
  }

  private static ThriftTransportPool instance = new ThriftTransportPool();
  private static final AtomicBoolean daemonStarted = new AtomicBoolean(false);

  public static ThriftTransportPool getInstance() {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(TRANSPORT_POOL_PERMISSION);
    }

    if (daemonStarted.compareAndSet(false, true)) {
      CountDownLatch closerExitLatch = new CountDownLatch(1);
      new Daemon(new Closer(instance, closerExitLatch, new AtomicBoolean(true)), "Thrift Connection Pool Checker").start();
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
      closeAllConnections(getCache());
      // same for the oneway cache
      closeAllConnections(getOnewayCache());

      // this will render the pool unusable and cause the background thread to exit
      this.cache = null;
      this.onewayCache = null;
    }

    try {
      closerExitLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Close all connections in the provided <code>cache</code>, regardless of reserved status.
   *
   * @param cache The cache of thrift connections.
   */
  private void closeAllConnections(Map<ThriftTransportKey,List<CachedConnection>> cache) {
    for (List<CachedConnection> ccl : cache.values()) {
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
  }

  //VisibleForTesting
  public List<CachedConnection> getCachedConnections(ThriftTransportKey ttk) {
    return getCache().get(ttk);
  }

  //VisibleForTesting
  public Map<ThriftTransportKey,List<CachedConnection>> getCache() {
    if (cache == null)
      throw new TransportPoolShutdownException();
    return cache;
  }

  //VisibleForTesting
  public List<CachedConnection> getOnewayCachedConnections(ThriftTransportKey ttk) {
    return getOnewayCache().get(ttk);
  }

  //VisibleForTesting
  public Map<ThriftTransportKey,List<CachedConnection>> getOnewayCache() {
    if (null == onewayCache) {
      throw new TransportPoolShutdownException();
    }
    return onewayCache;
  }
}
