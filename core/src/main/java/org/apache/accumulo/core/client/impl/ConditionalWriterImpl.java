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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletServerMutations;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.thrift.TCMResult;
import org.apache.accumulo.core.data.thrift.TCMStatus;
import org.apache.accumulo.core.data.thrift.TCondition;
import org.apache.accumulo.core.data.thrift.TConditionalMutation;
import org.apache.accumulo.core.data.thrift.TConditionalSession;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooUtil.LockID;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.accumulo.trace.thrift.TInfo;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;

class ConditionalWriterImpl implements ConditionalWriter {

  private static ThreadPoolExecutor cleanupThreadPool = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

  static {
    cleanupThreadPool.allowCoreThreadTimeOut(true);
  }

  private static final Logger log = Logger.getLogger(ConditionalWriterImpl.class);

  private static final int MAX_SLEEP = 30000;

  private Authorizations auths;
  private VisibilityEvaluator ve;
  @SuppressWarnings("unchecked")
  private Map<Text,Boolean> cache = Collections.synchronizedMap(new LRUMap(1000));
  private Instance instance;
  private Credentials credentials;
  private TabletLocator locator;
  private String tableId;
  private long timeout;

  private static class ServerQueue {
    BlockingQueue<TabletServerMutations<QCMutation>> queue = new LinkedBlockingQueue<TabletServerMutations<QCMutation>>();
    boolean taskQueued = false;
  }

  private Map<String,ServerQueue> serverQueues;
  private DelayQueue<QCMutation> failedMutations = new DelayQueue<QCMutation>();
  private ScheduledThreadPoolExecutor threadPool;

  private class RQIterator implements Iterator<Result> {

    private BlockingQueue<Result> rq;
    private int count;

    public RQIterator(BlockingQueue<Result> resultQueue, int count) {
      this.rq = resultQueue;
      this.count = count;
    }

    @Override
    public boolean hasNext() {
      return count > 0;
    }

    @Override
    public Result next() {
      if (count <= 0)
        throw new NoSuchElementException();

      try {
        Result result = rq.poll(1, TimeUnit.SECONDS);
        while (result == null) {

          if (threadPool.isShutdown()) {
            throw new NoSuchElementException("ConditionalWriter closed");
          }

          result = rq.poll(1, TimeUnit.SECONDS);
        }
        count--;
        return result;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  private static class QCMutation extends ConditionalMutation implements Delayed {
    private BlockingQueue<Result> resultQueue;
    private long resetTime;
    private long delay = 50;
    private long entryTime;

    QCMutation(ConditionalMutation cm, BlockingQueue<Result> resultQueue, long entryTime) {
      super(cm);
      this.resultQueue = resultQueue;
      this.entryTime = entryTime;
    }

    @Override
    public int compareTo(Delayed o) {
      QCMutation oqcm = (QCMutation) o;
      return Long.valueOf(resetTime).compareTo(Long.valueOf(oqcm.resetTime));
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof QCMutation)
        return compareTo((QCMutation) o) == 0;
      return false;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(delay - (System.currentTimeMillis() - resetTime), TimeUnit.MILLISECONDS);
    }

    void resetDelay() {
      delay = Math.min(delay * 2, MAX_SLEEP);
      resetTime = System.currentTimeMillis();
    }

    void queueResult(Result result) {
      resultQueue.add(result);
    }
  }

  private ServerQueue getServerQueue(String location) {
    ServerQueue serverQueue;
    synchronized (serverQueues) {
      serverQueue = serverQueues.get(location);
      if (serverQueue == null) {

        serverQueue = new ServerQueue();
        serverQueues.put(location, serverQueue);
      }
    }
    return serverQueue;
  }

  private class CleanupTask implements Runnable {
    private List<SessionID> sessions;

    CleanupTask(List<SessionID> activeSessions) {
      this.sessions = activeSessions;
    }

    @Override
    public void run() {
      TabletClientService.Iface client = null;

      for (SessionID sid : sessions) {
        if (!sid.isActive())
          continue;

        TInfo tinfo = Tracer.traceInfo();
        try {
          client = getClient(sid.location);
          client.closeConditionalUpdate(tinfo, sid.sessionID);
        } catch (Exception e) {} finally {
          ThriftUtil.returnClient((TServiceClient) client);
        }

      }
    }
  }

  private void queueRetry(List<QCMutation> mutations, String server) {

    if (timeout < Long.MAX_VALUE) {

      long time = System.currentTimeMillis();

      ArrayList<QCMutation> mutations2 = new ArrayList<ConditionalWriterImpl.QCMutation>(mutations.size());

      for (QCMutation qcm : mutations) {
        qcm.resetDelay();
        if (time + qcm.getDelay(TimeUnit.MILLISECONDS) > qcm.entryTime + timeout) {
          TimedOutException toe;
          if (server != null)
            toe = new TimedOutException(Collections.singleton(server));
          else
            toe = new TimedOutException("Conditional mutation timed out");

          qcm.queueResult(new Result(toe, qcm, server));
        } else {
          mutations2.add(qcm);
        }
      }

      if (mutations2.size() > 0)
        failedMutations.addAll(mutations2);

    } else {
      for (QCMutation qcm : mutations)
        qcm.resetDelay();
      failedMutations.addAll(mutations);
    }
  }

  private void queue(List<QCMutation> mutations) {
    List<QCMutation> failures = new ArrayList<QCMutation>();
    Map<String,TabletServerMutations<QCMutation>> binnedMutations = new HashMap<String,TabletLocator.TabletServerMutations<QCMutation>>();

    try {
      locator.binMutations(credentials, mutations, binnedMutations, failures);

      if (failures.size() == mutations.size())
        if (!Tables.exists(instance, tableId))
          throw new TableDeletedException(tableId);
        else if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
          throw new TableOfflineException(instance, tableId);

    } catch (Exception e) {
      for (QCMutation qcm : mutations)
        qcm.queueResult(new Result(e, qcm, null));

      // do not want to queue anything that was put in before binMutations() failed
      failures.clear();
      binnedMutations.clear();
    }

    if (failures.size() > 0)
      queueRetry(failures, null);

    for (Entry<String,TabletServerMutations<QCMutation>> entry : binnedMutations.entrySet()) {
      queue(entry.getKey(), entry.getValue());
    }

  }

  private void queue(String location, TabletServerMutations<QCMutation> mutations) {

    ServerQueue serverQueue = getServerQueue(location);

    synchronized (serverQueue) {
      serverQueue.queue.add(mutations);
      // never execute more than one task per server
      if (!serverQueue.taskQueued) {
        threadPool.execute(new LoggingRunnable(log, Trace.wrap(new SendTask(location))));
        serverQueue.taskQueued = true;
      }
    }

  }

  private void reschedule(SendTask task) {
    ServerQueue serverQueue = getServerQueue(task.location);
    // just finished processing work for this server, could reschedule if it has more work or immediately process the work
    // this code reschedules the the server for processing later... there may be other queues with
    // more data that need to be processed... also it will give the current server time to build
    // up more data... the thinking is that rescheduling instead or processing immediately will result
    // in bigger batches and less RPC overhead

    synchronized (serverQueue) {
      if (serverQueue.queue.size() > 0)
        threadPool.execute(new LoggingRunnable(log, Trace.wrap(task)));
      else
        serverQueue.taskQueued = false;
    }

  }

  private TabletServerMutations<QCMutation> dequeue(String location) {
    BlockingQueue<TabletServerMutations<QCMutation>> queue = getServerQueue(location).queue;

    ArrayList<TabletServerMutations<QCMutation>> mutations = new ArrayList<TabletLocator.TabletServerMutations<QCMutation>>();
    queue.drainTo(mutations);

    if (mutations.size() == 0)
      return null;

    if (mutations.size() == 1) {
      return mutations.get(0);
    } else {
      // merge multiple request to a single tablet server
      TabletServerMutations<QCMutation> tsm = mutations.get(0);

      for (int i = 1; i < mutations.size(); i++) {
        for (Entry<KeyExtent,List<QCMutation>> entry : mutations.get(i).getMutations().entrySet()) {
          List<QCMutation> list = tsm.getMutations().get(entry.getKey());
          if (list == null) {
            list = new ArrayList<QCMutation>();
            tsm.getMutations().put(entry.getKey(), list);
          }

          list.addAll(entry.getValue());
        }
      }

      return tsm;
    }
  }

  ConditionalWriterImpl(Instance instance, Credentials credentials, String tableId, ConditionalWriterConfig config) {
    this.instance = instance;
    this.credentials = credentials;
    this.auths = config.getAuthorizations();
    this.ve = new VisibilityEvaluator(config.getAuthorizations());
    this.threadPool = new ScheduledThreadPoolExecutor(config.getMaxWriteThreads());
    this.locator = TabletLocator.getLocator(instance, new Text(tableId));
    this.serverQueues = new HashMap<String,ServerQueue>();
    this.tableId = tableId;
    this.timeout = config.getTimeout(TimeUnit.MILLISECONDS);

    Runnable failureHandler = new Runnable() {

      @Override
      public void run() {
        List<QCMutation> mutations = new ArrayList<QCMutation>();
        failedMutations.drainTo(mutations);
        if (mutations.size() > 0)
          queue(mutations);
      }
    };

    failureHandler = new LoggingRunnable(log, failureHandler);

    threadPool.scheduleAtFixedRate(failureHandler, 250, 250, TimeUnit.MILLISECONDS);
  }

  @Override
  public Iterator<Result> write(Iterator<ConditionalMutation> mutations) {

    BlockingQueue<Result> resultQueue = new LinkedBlockingQueue<Result>();

    List<QCMutation> mutationList = new ArrayList<QCMutation>();

    int count = 0;

    long entryTime = System.currentTimeMillis();

    mloop: while (mutations.hasNext()) {
      ConditionalMutation mut = mutations.next();
      count++;

      if (mut.getConditions().size() == 0)
        throw new IllegalArgumentException("ConditionalMutation had no conditions " + new String(mut.getRow()));

      for (Condition cond : mut.getConditions()) {
        if (!isVisible(cond.getVisibility())) {
          resultQueue.add(new Result(Status.INVISIBLE_VISIBILITY, mut, null));
          continue mloop;
        }
      }

      // copy the mutations so that even if caller changes it, it will not matter
      mutationList.add(new QCMutation(mut, resultQueue, entryTime));
    }

    queue(mutationList);

    return new RQIterator(resultQueue, count);

  }

  private class SendTask implements Runnable {

    String location;

    public SendTask(String location) {
      this.location = location;

    }

    @Override
    public void run() {
      try {
        TabletServerMutations<QCMutation> mutations = dequeue(location);
        if (mutations != null)
          sendToServer(location, mutations);
      } finally {
        reschedule(this);
      }
    }
  }

  private static class CMK {

    QCMutation cm;
    KeyExtent ke;

    public CMK(KeyExtent ke, QCMutation cm) {
      this.ke = ke;
      this.cm = cm;
    }
  }

  private static class SessionID {
    String location;
    String lockId;
    long sessionID;
    boolean reserved;
    long lastAccessTime;
    long ttl;

    boolean isActive() {
      return System.currentTimeMillis() - lastAccessTime < ttl * .95;
    }
  }

  private HashMap<String,SessionID> cachedSessionIDs = new HashMap<String,SessionID>();

  private SessionID reserveSessionID(String location, TabletClientService.Iface client, TInfo tinfo) throws ThriftSecurityException, TException {
    // avoid cost of repeatedly making RPC to create sessions, reuse sessions
    synchronized (cachedSessionIDs) {
      SessionID sid = cachedSessionIDs.get(location);
      if (sid != null) {
        if (sid.reserved)
          throw new IllegalStateException();

        if (!sid.isActive()) {
          cachedSessionIDs.remove(location);
        } else {
          sid.reserved = true;
          return sid;
        }
      }
    }

    TConditionalSession tcs = client.startConditionalUpdate(tinfo, credentials.toThrift(instance), ByteBufferUtil.toByteBuffers(auths.getAuthorizations()),
        tableId);

    synchronized (cachedSessionIDs) {
      SessionID sid = new SessionID();
      sid.reserved = true;
      sid.sessionID = tcs.sessionId;
      sid.lockId = tcs.tserverLock;
      sid.ttl = tcs.ttl;
      sid.location = location;
      if (cachedSessionIDs.put(location, sid) != null)
        throw new IllegalStateException();

      return sid;
    }

  }

  private void invalidateSessionID(String location) {
    synchronized (cachedSessionIDs) {
      cachedSessionIDs.remove(location);
    }

  }

  private void unreserveSessionID(String location) {
    synchronized (cachedSessionIDs) {
      SessionID sid = cachedSessionIDs.get(location);
      if (sid != null) {
        if (!sid.reserved)
          throw new IllegalStateException();
        sid.reserved = false;
        sid.lastAccessTime = System.currentTimeMillis();
      }
    }
  }

  List<SessionID> getActiveSessions() {
    ArrayList<SessionID> activeSessions = new ArrayList<SessionID>();
    for (SessionID sid : cachedSessionIDs.values())
      if (sid.isActive())
        activeSessions.add(sid);
    return activeSessions;
  }

  private TabletClientService.Iface getClient(String location) throws TTransportException {
    TabletClientService.Iface client;
    if (timeout < ServerConfigurationUtil.getConfiguration(instance).getTimeInMillis(Property.GENERAL_RPC_TIMEOUT))
      client = ThriftUtil.getTServerClient(location, ServerConfigurationUtil.getConfiguration(instance), timeout);
    else
      client = ThriftUtil.getTServerClient(location, ServerConfigurationUtil.getConfiguration(instance));
    return client;
  }

  private void sendToServer(String location, TabletServerMutations<QCMutation> mutations) {
    TabletClientService.Iface client = null;

    TInfo tinfo = Tracer.traceInfo();

    Map<Long,CMK> cmidToCm = new HashMap<Long,CMK>();
    MutableLong cmid = new MutableLong(0);

    SessionID sessionId = null;

    try {
      Map<TKeyExtent,List<TConditionalMutation>> tmutations = new HashMap<TKeyExtent,List<TConditionalMutation>>();

      CompressedIterators compressedIters = new CompressedIterators();
      convertMutations(mutations, cmidToCm, cmid, tmutations, compressedIters);

      // getClient() call must come after converMutations in case it throws a TException
      client = getClient(location);

      List<TCMResult> tresults = null;
      while (tresults == null) {
        try {
          sessionId = reserveSessionID(location, client, tinfo);
          tresults = client.conditionalUpdate(tinfo, sessionId.sessionID, tmutations, compressedIters.getSymbolTable());
        } catch (NoSuchScanIDException nssie) {
          sessionId = null;
          invalidateSessionID(location);
        }
      }

      HashSet<KeyExtent> extentsToInvalidate = new HashSet<KeyExtent>();

      ArrayList<QCMutation> ignored = new ArrayList<QCMutation>();

      for (TCMResult tcmResult : tresults) {
        if (tcmResult.status == TCMStatus.IGNORED) {
          CMK cmk = cmidToCm.get(tcmResult.cmid);
          ignored.add(cmk.cm);
          extentsToInvalidate.add(cmk.ke);
        } else {
          QCMutation qcm = cmidToCm.get(tcmResult.cmid).cm;
          qcm.queueResult(new Result(fromThrift(tcmResult.status), qcm, location));
        }
      }

      for (KeyExtent ke : extentsToInvalidate) {
        locator.invalidateCache(ke);
      }

      queueRetry(ignored, location);

    } catch (ThriftSecurityException tse) {
      AccumuloSecurityException ase = new AccumuloSecurityException(credentials.getPrincipal(), tse.getCode(), Tables.getPrintableTableInfoFromId(instance,
          tableId), tse);
      queueException(location, cmidToCm, ase);
    } catch (TTransportException e) {
      locator.invalidateCache(location);
      invalidateSession(location, mutations, cmidToCm, sessionId);
    } catch (TApplicationException tae) {
      queueException(location, cmidToCm, new AccumuloServerException(location, tae));
    } catch (TException e) {
      locator.invalidateCache(location);
      invalidateSession(location, mutations, cmidToCm, sessionId);
    } catch (Exception e) {
      queueException(location, cmidToCm, e);
    } finally {
      if (sessionId != null)
        unreserveSessionID(location);
      ThriftUtil.returnClient((TServiceClient) client);
    }
  }

  private void queueRetry(Map<Long,CMK> cmidToCm, String location) {
    ArrayList<QCMutation> ignored = new ArrayList<QCMutation>();
    for (CMK cmk : cmidToCm.values())
      ignored.add(cmk.cm);
    queueRetry(ignored, location);
  }

  private void queueException(String location, Map<Long,CMK> cmidToCm, Exception e) {
    for (CMK cmk : cmidToCm.values())
      cmk.cm.queueResult(new Result(e, cmk.cm, location));
  }

  private void invalidateSession(String location, TabletServerMutations<QCMutation> mutations, Map<Long,CMK> cmidToCm, SessionID sessionId) {
    if (sessionId == null) {
      queueRetry(cmidToCm, location);
    } else {
      try {
        invalidateSession(sessionId, location);
        for (CMK cmk : cmidToCm.values())
          cmk.cm.queueResult(new Result(Status.UNKNOWN, cmk.cm, location));
      } catch (Exception e2) {
        queueException(location, cmidToCm, e2);
      }
    }
  }

  /*
   * The purpose of this code is to ensure that a conditional mutation will not execute when its status is unknown. This allows a user to read the row when the
   * status is unknown and not have to worry about the tserver applying the mutation after the scan.
   *
   * If a conditional mutation is taking a long time to process, then this method will wait for it to finish... unless this exceeds timeout.
   */
  private void invalidateSession(SessionID sessionId, String location) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    long sleepTime = 50;

    long startTime = System.currentTimeMillis();

    LockID lid = new LockID(ZooUtil.getRoot(instance) + Constants.ZTSERVERS, sessionId.lockId);

    ZooCacheFactory zcf = new ZooCacheFactory();
    while (true) {
      if (!ZooLock.isLockHeld(zcf.getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut()), lid)) {
        // ACCUMULO-1152 added a tserver lock check to the tablet location cache, so this invalidation prevents future attempts to contact the
        // tserver even its gone zombie and is still running w/o a lock
        locator.invalidateCache(location);
        return;
      }

      try {
        // if the mutation is currently processing, this method will block until its done or times out
        invalidateSession(sessionId.sessionID, location);

        return;
      } catch (TApplicationException tae) {
        throw new AccumuloServerException(location, tae);
      } catch (TException e) {
        locator.invalidateCache(location);
      }

      if ((System.currentTimeMillis() - startTime) + sleepTime > timeout)
        throw new TimedOutException(Collections.singleton(location));

      UtilWaitThread.sleep(sleepTime);
      sleepTime = Math.min(2 * sleepTime, MAX_SLEEP);

    }

  }

  private void invalidateSession(long sessionId, String location) throws TException {
    TabletClientService.Iface client = null;

    TInfo tinfo = Tracer.traceInfo();

    try {
      client = getClient(location);
      client.invalidateConditionalUpdate(tinfo, sessionId);
    } finally {
      ThriftUtil.returnClient((TServiceClient) client);
    }
  }

  private Status fromThrift(TCMStatus status) {
    switch (status) {
      case ACCEPTED:
        return Status.ACCEPTED;
      case REJECTED:
        return Status.REJECTED;
      case VIOLATED:
        return Status.VIOLATED;
      default:
        throw new IllegalArgumentException(status.toString());
    }
  }

  private void convertMutations(TabletServerMutations<QCMutation> mutations, Map<Long,CMK> cmidToCm, MutableLong cmid,
      Map<TKeyExtent,List<TConditionalMutation>> tmutations, CompressedIterators compressedIters) {

    for (Entry<KeyExtent,List<QCMutation>> entry : mutations.getMutations().entrySet()) {
      TKeyExtent tke = entry.getKey().toThrift();
      ArrayList<TConditionalMutation> tcondMutaions = new ArrayList<TConditionalMutation>();

      List<QCMutation> condMutations = entry.getValue();

      for (QCMutation cm : condMutations) {
        TMutation tm = cm.toThrift();

        List<TCondition> conditions = convertConditions(cm, compressedIters);

        cmidToCm.put(cmid.longValue(), new CMK(entry.getKey(), cm));
        TConditionalMutation tcm = new TConditionalMutation(conditions, tm, cmid.longValue());
        cmid.increment();
        tcondMutaions.add(tcm);
      }

      tmutations.put(tke, tcondMutaions);
    }
  }

  private List<TCondition> convertConditions(ConditionalMutation cm, CompressedIterators compressedIters) {
    List<TCondition> conditions = new ArrayList<TCondition>(cm.getConditions().size());

    for (Condition cond : cm.getConditions()) {
      long ts = 0;
      boolean hasTs = false;

      if (cond.getTimestamp() != null) {
        ts = cond.getTimestamp();
        hasTs = true;
      }

      ByteBuffer iters = compressedIters.compress(cond.getIterators());

      TCondition tc = new TCondition(ByteBufferUtil.toByteBuffers(cond.getFamily()), ByteBufferUtil.toByteBuffers(cond.getQualifier()),
          ByteBufferUtil.toByteBuffers(cond.getVisibility()), ts, hasTs, ByteBufferUtil.toByteBuffers(cond.getValue()), iters);

      conditions.add(tc);
    }

    return conditions;
  }

  private boolean isVisible(ByteSequence cv) {
    Text testVis = new Text(cv.toArray());
    if (testVis.getLength() == 0)
      return true;

    Boolean b = cache.get(testVis);
    if (b != null)
      return b;

    try {
      Boolean bb = ve.evaluate(new ColumnVisibility(testVis));
      cache.put(new Text(testVis), bb);
      return bb;
    } catch (VisibilityParseException e) {
      return false;
    } catch (BadArgumentException e) {
      return false;
    }
  }

  @Override
  public Result write(ConditionalMutation mutation) {
    return write(Collections.singleton(mutation).iterator()).next();
  }

  @Override
  public void close() {
    threadPool.shutdownNow();
    cleanupThreadPool.execute(new CleanupTask(getActiveSessions()));
  }

}
