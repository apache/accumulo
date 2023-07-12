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
package org.apache.accumulo.tserver.session;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.tabletserver.thrift.ActiveScan;
import org.apache.accumulo.core.tabletserver.thrift.ScanState;
import org.apache.accumulo.core.tabletserver.thrift.ScanType;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.tserver.scan.ScanRunState;
import org.apache.accumulo.tserver.scan.ScanTask;
import org.apache.accumulo.tserver.session.Session.State;
import org.apache.accumulo.tserver.tablet.ScanBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class SessionManager {
  private static final Logger log = LoggerFactory.getLogger(SessionManager.class);

  private static final SecureRandom random = new SecureRandom();
  private final ConcurrentMap<Long,Session> sessions = new ConcurrentHashMap<>();
  private final long maxIdle;
  private final long maxUpdateIdle;
  private final BlockingQueue<Session> deferredCleanupQueue = new ArrayBlockingQueue<>(5000);
  private final Long expiredSessionMarker = (long) -1;
  private final AccumuloConfiguration aconf;
  private final ServerContext ctx;

  public SessionManager(ServerContext context) {
    this.ctx = context;
    this.aconf = context.getConfiguration();
    maxUpdateIdle = aconf.getTimeInMillis(Property.TSERV_UPDATE_SESSION_MAXIDLE);
    maxIdle = aconf.getTimeInMillis(Property.TSERV_SESSION_MAXIDLE);

    Runnable r = () -> sweep(maxIdle, maxUpdateIdle);

    ThreadPools.watchCriticalScheduledTask(context.getScheduledExecutor().scheduleWithFixedDelay(r,
        0, Math.max(maxIdle / 2, 1000), TimeUnit.MILLISECONDS));
  }

  public long createSession(Session session, boolean reserve) {
    long sid = random.nextLong();

    synchronized (session) {
      Preconditions.checkArgument(session.state == State.NEW);
      session.state = reserve ? State.RESERVED : State.UNRESERVED;
      session.startTime = session.lastAccessTime = System.currentTimeMillis();
    }

    while (sessions.putIfAbsent(sid, session) != null) {
      sid = random.nextLong();
    }

    return sid;
  }

  public long getMaxIdleTime() {
    return maxIdle;
  }

  /**
   * while a session is reserved, it cannot be canceled or removed
   */

  public Session reserveSession(long sessionId) {
    Session session = sessions.get(sessionId);
    if (session != null) {
      synchronized (session) {
        if (session.state == State.RESERVED) {
          throw new IllegalStateException(
              "Attempted to reserved session that is already reserved " + sessionId);
        }
        if (session.state == State.REMOVED) {
          return null;
        }
        session.state = State.RESERVED;
      }
    }

    return session;

  }

  public Session reserveSession(long sessionId, boolean wait) {
    Session session = sessions.get(sessionId);
    if (session != null) {
      synchronized (session) {

        if (session.state == State.REMOVED) {
          return null;
        }

        while (wait && session.state == State.RESERVED) {
          try {
            session.wait(1000);
          } catch (InterruptedException e) {
            throw new RuntimeException();
          }
        }

        if (session.state == State.RESERVED) {
          throw new IllegalStateException(
              "Attempted to reserved session that is already reserved " + sessionId);
        }
        if (session.state == State.REMOVED) {
          return null;
        }
        session.state = State.RESERVED;
      }
    }

    return session;

  }

  public void unreserveSession(Session session) {
    synchronized (session) {
      if (session.state == State.REMOVED) {
        return;
      }
      if (session.state != State.RESERVED) {
        throw new IllegalStateException("Cannon unreserve, state: " + session.state);
      }
      session.notifyAll();
      session.state = State.UNRESERVED;
      session.lastAccessTime = System.currentTimeMillis();
    }
  }

  public void unreserveSession(long sessionId) {
    Session session = getSession(sessionId);
    if (session != null) {
      unreserveSession(session);
    }
  }

  public Session getSession(long sessionId) {
    Session session = sessions.get(sessionId);

    if (session != null) {
      synchronized (session) {
        if (session.state == State.REMOVED) {
          return null;
        }
        session.lastAccessTime = System.currentTimeMillis();
      }
    }

    return session;
  }

  public Session removeSession(long sessionId) {
    return removeSession(sessionId, false);
  }

  public Session removeSession(long sessionId, boolean unreserve) {

    Session session = sessions.remove(sessionId);
    if (session != null) {
      boolean doCleanup = false;
      synchronized (session) {
        if (session.state != State.REMOVED) {
          if (unreserve) {
            unreserveSession(session);
          }
          doCleanup = true;
          session.state = State.REMOVED;
        }
      }

      if (doCleanup) {
        cleanup(session);
      }
    }

    return session;
  }

  static void cleanup(BlockingQueue<Session> deferredCleanupQueue, Session session) {
    if (!session.cleanup()) {
      var retry = Retry.builder().infiniteRetries().retryAfter(25, MILLISECONDS)
          .incrementBy(25, MILLISECONDS).maxWait(5, SECONDS).backOffFactor(1.5)
          .logInterval(1, MINUTES).createRetry();

      while (!deferredCleanupQueue.offer(session)) {
        if (session.cleanup()) {
          break;
        }

        try {
          retry.waitForNextAttempt(log, "Unable to cleanup session or defer cleanup " + session);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        retry.logRetry(log, "Unable to cleanup session or defer cleanup " + session);
      }

      retry.logCompletion(log, "Cleaned up session or deferred cleanup " + session);
    }
  }

  private void cleanup(Session session) {
    cleanup(deferredCleanupQueue, session);
  }

  private void sweep(final long maxIdle, final long maxUpdateIdle) {
    List<Session> sessionsToCleanup = new LinkedList<>();
    Iterator<Session> iter = sessions.values().iterator();
    while (iter.hasNext()) {
      Session session = iter.next();
      synchronized (session) {
        if (session.state == State.UNRESERVED) {
          long configuredIdle = maxIdle;
          if (session instanceof UpdateSession) {
            configuredIdle = maxUpdateIdle;
          }
          long idleTime = System.currentTimeMillis() - session.lastAccessTime;
          if (idleTime > configuredIdle) {
            log.info("Closing idle session from user={}, client={}, idle={}ms", session.getUser(),
                session.client, idleTime);
            iter.remove();
            sessionsToCleanup.add(session);
            session.state = State.REMOVED;
          }
        }
      }
    }

    // do clean up outside of lock for TabletServer
    deferredCleanupQueue.drainTo(sessionsToCleanup);

    // make a pass through and remove everything that can be cleaned up before calling the
    // cleanup(Session) method which may block when it can not clean up a session.
    sessionsToCleanup.removeIf(Session::cleanup);

    sessionsToCleanup.forEach(this::cleanup);
  }

  public void removeIfNotAccessed(final long sessionId, final long delay) {
    Session session = sessions.get(sessionId);
    if (session != null) {
      long tmp;
      synchronized (session) {
        tmp = session.lastAccessTime;
      }
      final long removeTime = tmp;
      Runnable r = new Runnable() {
        @Override
        public void run() {
          Session session2 = sessions.get(sessionId);
          if (session2 != null) {
            boolean shouldRemove = false;
            synchronized (session2) {
              if (session2.lastAccessTime == removeTime && session2.state == State.UNRESERVED) {
                session2.state = State.REMOVED;
                shouldRemove = true;
              }
            }

            if (shouldRemove) {
              log.info("Closing not accessed session from user=" + session2.getUser() + ", client="
                  + session2.client + ", duration=" + delay + "ms");
              sessions.remove(sessionId);
              cleanup(session2);
            }
          }
        }
      };

      ScheduledFuture<?> future =
          ctx.getScheduledExecutor().schedule(r, delay, TimeUnit.MILLISECONDS);
      ThreadPools.watchNonCriticalScheduledTask(future);
    }
  }

  public Map<TableId,MapCounter<ScanRunState>> getActiveScansPerTable() {
    Map<TableId,MapCounter<ScanRunState>> counts = new HashMap<>();

    Set<Entry<Long,Session>> copiedIdleSessions = new HashSet<>();

    /**
     * Add sessions so that get the list returned in the active scans call
     */
    for (Session session : deferredCleanupQueue) {
      copiedIdleSessions.add(Maps.immutableEntry(expiredSessionMarker, session));
    }

    List.of(sessions.entrySet(), copiedIdleSessions).forEach(set -> set.forEach(entry -> {

      Session session = entry.getValue();
      ScanTask<?> nbt = null;
      TableId tableID = null;

      if (session instanceof SingleScanSession) {
        SingleScanSession ss = (SingleScanSession) session;
        nbt = ss.nextBatchTask;
        tableID = ss.extent.tableId();
      } else if (session instanceof MultiScanSession) {
        MultiScanSession mss = (MultiScanSession) session;
        nbt = mss.lookupTask;
        tableID = mss.threadPoolExtent.tableId();
      }

      if (nbt != null) {
        ScanRunState srs = nbt.getScanRunState();
        if (srs != ScanRunState.FINISHED) {
          counts.computeIfAbsent(tableID, unusedKey -> new MapCounter<>()).increment(srs, 1);
        }
      }
    }));

    return counts;
  }

  public List<ActiveScan> getActiveScans() {

    final List<ActiveScan> activeScans = new ArrayList<>();
    final long ct = System.currentTimeMillis();
    final Set<Entry<Long,Session>> copiedIdleSessions = new HashSet<>();

    /**
     * Add sessions so that get the list returned in the active scans call
     */
    for (Session session : deferredCleanupQueue) {
      copiedIdleSessions.add(Maps.immutableEntry(expiredSessionMarker, session));
    }

    List.of(sessions.entrySet(), copiedIdleSessions).forEach(s -> s.forEach(entry -> {
      Session session = entry.getValue();
      if (session instanceof SingleScanSession) {
        SingleScanSession ss = (SingleScanSession) session;

        ScanState state = ScanState.RUNNING;

        ScanTask<ScanBatch> nbt = ss.nextBatchTask;
        if (nbt == null) {
          state = ScanState.IDLE;
        } else {
          switch (nbt.getScanRunState()) {
            case QUEUED:
              state = ScanState.QUEUED;
              break;
            case FINISHED:
              state = ScanState.IDLE;
              break;
            case RUNNING:
            default:
              /* do nothing */
              break;
          }
        }

        var params = ss.scanParams;
        ActiveScan activeScan = new ActiveScan(ss.client, ss.getUser(),
            ss.extent.tableId().canonical(), ct - ss.startTime, ct - ss.lastAccessTime,
            ScanType.SINGLE, state, ss.extent.toThrift(),
            params.getColumnSet().stream().map(Column::toThrift).collect(Collectors.toList()),
            params.getSsiList(), params.getSsio(), params.getAuthorizations().getAuthorizationsBB(),
            params.getClassLoaderContext());

        // scanId added by ACCUMULO-2641 is an optional thrift argument and not available in
        // ActiveScan constructor
        activeScan.setScanId(entry.getKey());
        activeScans.add(activeScan);

      } else if (session instanceof MultiScanSession) {
        MultiScanSession mss = (MultiScanSession) session;

        ScanState state = ScanState.RUNNING;

        ScanTask<MultiScanResult> nbt = mss.lookupTask;
        if (nbt == null) {
          state = ScanState.IDLE;
        } else {
          switch (nbt.getScanRunState()) {
            case QUEUED:
              state = ScanState.QUEUED;
              break;
            case FINISHED:
              state = ScanState.IDLE;
              break;
            case RUNNING:
            default:
              /* do nothing */
              break;
          }
        }

        var params = mss.scanParams;
        activeScans.add(new ActiveScan(mss.client, mss.getUser(),
            mss.threadPoolExtent.tableId().canonical(), ct - mss.startTime, ct - mss.lastAccessTime,
            ScanType.BATCH, state, mss.threadPoolExtent.toThrift(),
            params.getColumnSet().stream().map(Column::toThrift).collect(Collectors.toList()),
            params.getSsiList(), params.getSsio(), params.getAuthorizations().getAuthorizationsBB(),
            params.getClassLoaderContext()));
      }
    }));

    return activeScans;
  }
}
