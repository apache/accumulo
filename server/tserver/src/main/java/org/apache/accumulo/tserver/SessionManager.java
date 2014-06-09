package org.apache.accumulo.tserver;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.impl.Translator;
import org.apache.accumulo.core.client.impl.Translators;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.thrift.MultiScanResult;
import org.apache.accumulo.core.tabletserver.thrift.ActiveScan;
import org.apache.accumulo.core.tabletserver.thrift.ScanState;
import org.apache.accumulo.core.tabletserver.thrift.ScanType;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.tserver.TabletServer.ScanRunState;
import org.apache.accumulo.tserver.TabletServer.ScanTask;
import org.apache.accumulo.tserver.session.MultiScanSession;
import org.apache.accumulo.tserver.session.ScanSession;
import org.apache.accumulo.tserver.session.Session;
import org.apache.accumulo.tserver.tablet.ScanBatch;

public class SessionManager {

  private final SecureRandom random = new SecureRandom();
  private final Map<Long,Session> sessions = new HashMap<Long,Session>();
  private final long maxIdle;
  private final AccumuloConfiguration aconf;

  SessionManager(AccumuloConfiguration conf) {
    aconf = conf;
    maxIdle = conf.getTimeInMillis(Property.TSERV_SESSION_MAXIDLE);

    Runnable r = new Runnable() {
      @Override
      public void run() {
        sweep(maxIdle);
      }
    };

    SimpleTimer.getInstance(conf).schedule(r, 0, Math.max(maxIdle / 2, 1000));
  }

  synchronized long createSession(Session session, boolean reserve) {
    long sid = random.nextLong();

    while (sessions.containsKey(sid)) {
      sid = random.nextLong();
    }

    sessions.put(sid, session);

    session.reserved = reserve;

    session.startTime = session.lastAccessTime = System.currentTimeMillis();

    return sid;
  }

  long getMaxIdleTime() {
    return maxIdle;
  }

  /**
   * while a session is reserved, it cannot be canceled or removed
   *
   * @param sessionId
   */

  synchronized Session reserveSession(long sessionId) {
    Session session = sessions.get(sessionId);
    if (session != null) {
      if (session.reserved)
        throw new IllegalStateException();
      session.reserved = true;
    }

    return session;

  }

  synchronized Session reserveSession(long sessionId, boolean wait) {
    Session session = sessions.get(sessionId);
    if (session != null) {
      while (wait && session.reserved) {
        try {
          wait(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException();
        }
      }

      if (session.reserved)
        throw new IllegalStateException();
      session.reserved = true;
    }

    return session;

  }

  synchronized void unreserveSession(Session session) {
    if (!session.reserved)
      throw new IllegalStateException();
    notifyAll();
    session.reserved = false;
    session.lastAccessTime = System.currentTimeMillis();
  }

  synchronized void unreserveSession(long sessionId) {
    Session session = getSession(sessionId);
    if (session != null)
      unreserveSession(session);
  }

  synchronized Session getSession(long sessionId) {
    Session session = sessions.get(sessionId);
    if (session != null)
      session.lastAccessTime = System.currentTimeMillis();
    return session;
  }

  Session removeSession(long sessionId) {
    return removeSession(sessionId, false);
  }

  Session removeSession(long sessionId, boolean unreserve) {
    Session session = null;
    synchronized (this) {
      session = sessions.remove(sessionId);
      if (unreserve && session != null)
        unreserveSession(session);
    }

    // do clean up out side of lock..
    if (session != null)
      session.cleanup();

    return session;
  }

  private void sweep(long maxIdle) {
    List<Session> sessionsToCleanup = new ArrayList<Session>();
    synchronized (this) {
      Iterator<Session> iter = sessions.values().iterator();
      while (iter.hasNext()) {
        Session session = iter.next();
        long idleTime = System.currentTimeMillis() - session.lastAccessTime;
        if (idleTime > maxIdle && !session.reserved) {
          iter.remove();
          sessionsToCleanup.add(session);
        }
      }
    }

    // do clean up outside of lock
    for (Session session : sessionsToCleanup) {
      session.cleanup();
    }
  }

  synchronized void removeIfNotAccessed(final long sessionId, long delay) {
    Session session = sessions.get(sessionId);
    if (session != null) {
      final long removeTime = session.lastAccessTime;
      TimerTask r = new TimerTask() {
        @Override
        public void run() {
          Session sessionToCleanup = null;
          synchronized (SessionManager.this) {
            Session session2 = sessions.get(sessionId);
            if (session2 != null && session2.lastAccessTime == removeTime && !session2.reserved) {
              sessions.remove(sessionId);
              sessionToCleanup = session2;
            }
          }

          // call clean up outside of lock
          if (sessionToCleanup != null)
            sessionToCleanup.cleanup();
        }
      };

      SimpleTimer.getInstance(aconf).schedule(r, delay);
    }
  }

  public synchronized Map<String,MapCounter<ScanRunState>> getActiveScansPerTable() {
    Map<String,MapCounter<ScanRunState>> counts = new HashMap<String,MapCounter<ScanRunState>>();
    for (Entry<Long,Session> entry : sessions.entrySet()) {

      Session session = entry.getValue();
      @SuppressWarnings("rawtypes")
      ScanTask nbt = null;
      String tableID = null;

      if (session instanceof ScanSession) {
        ScanSession ss = (ScanSession) session;
        nbt = ss.nextBatchTask;
        tableID = ss.extent.getTableId().toString();
      } else if (session instanceof MultiScanSession) {
        MultiScanSession mss = (MultiScanSession) session;
        nbt = mss.lookupTask;
        tableID = mss.threadPoolExtent.getTableId().toString();
      }

      if (nbt == null)
        continue;

      ScanRunState srs = nbt.getScanRunState();

      if (srs == ScanRunState.FINISHED)
        continue;

      MapCounter<ScanRunState> stateCounts = counts.get(tableID);
      if (stateCounts == null) {
        stateCounts = new MapCounter<ScanRunState>();
        counts.put(tableID, stateCounts);
      }

      stateCounts.increment(srs, 1);
    }

    return counts;
  }

  public synchronized List<ActiveScan> getActiveScans() {

    List<ActiveScan> activeScans = new ArrayList<ActiveScan>();

    long ct = System.currentTimeMillis();

    for (Entry<Long,Session> entry : sessions.entrySet()) {
      Session session = entry.getValue();
      if (session instanceof ScanSession) {
        ScanSession ss = (ScanSession) session;

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

        activeScans.add(new ActiveScan(ss.client, ss.getUser(), ss.extent.getTableId().toString(), ct - ss.startTime, ct - ss.lastAccessTime, ScanType.SINGLE,
            state, ss.extent.toThrift(), Translator.translate(ss.columnSet, Translators.CT), ss.ssiList, ss.ssio, ss.auths.getAuthorizationsBB()));

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

        activeScans.add(new ActiveScan(mss.client, mss.getUser(), mss.threadPoolExtent.getTableId().toString(), ct - mss.startTime, ct - mss.lastAccessTime,
            ScanType.BATCH, state, mss.threadPoolExtent.toThrift(), Translator.translate(mss.columnSet, Translators.CT), mss.ssiList, mss.ssio, mss.auths
                .getAuthorizationsBB()));
      }
    }

    return activeScans;
  }
}