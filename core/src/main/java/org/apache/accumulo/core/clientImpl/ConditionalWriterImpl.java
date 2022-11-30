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
package org.apache.accumulo.core.clientImpl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.clientImpl.TabletLocator.TabletServerMutations;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TCMResult;
import org.apache.accumulo.core.dataImpl.thrift.TCMStatus;
import org.apache.accumulo.core.dataImpl.thrift.TCondition;
import org.apache.accumulo.core.dataImpl.thrift.TConditionalMutation;
import org.apache.accumulo.core.dataImpl.thrift.TConditionalSession;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TMutation;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.LockID;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;

class ConditionalWriterImpl implements ConditionalWriter {

  private static final int MAX_SLEEP = 30000;

  private Authorizations auths;
  private VisibilityEvaluator ve;
  private Map<Text,Boolean> cache = Collections.synchronizedMap(new LRUMap<>(1000));
  private final ClientContext context;
  private TabletLocator locator;
  private final TableId tableId;
  private final String tableName;
  private long timeout;
  private final Durability durability;
  private final String classLoaderContext;

  private static class ServerQueue {
    BlockingQueue<TabletServerMutations<QCMutation>> queue = new LinkedBlockingQueue<>();
    boolean taskQueued = false;
  }

  private Map<String,ServerQueue> serverQueues;
  private DelayQueue<QCMutation> failedMutations = new DelayQueue<>();
  private ScheduledThreadPoolExecutor threadPool;
  private final ScheduledFuture<?> failureTaskFuture;

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
      if (count <= 0) {
        throw new NoSuchElementException();
      }

      try {
        Result result = rq.poll(1, SECONDS);
        while (result == null) {

          if (threadPool.isShutdown()) {
            throw new NoSuchElementException("ConditionalWriter closed");
          }

          result = rq.poll(1, SECONDS);
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
      return Long.compare(resetTime, oqcm.resetTime);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof QCMutation) {
        return compareTo((QCMutation) o) == 0;
      }
      return false;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(delay - (System.currentTimeMillis() - resetTime), MILLISECONDS);
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
        if (!sid.isActive()) {
          continue;
        }

        TInfo tinfo = TraceUtil.traceInfo();
        try {
          client = getClient(sid.location);
          client.closeConditionalUpdate(tinfo, sid.sessionID);
        } catch (Exception e) {} finally {
          ThriftUtil.returnClient((TServiceClient) client, context);
        }

      }
    }
  }

  private void queueRetry(List<QCMutation> mutations, HostAndPort server) {

    if (timeout < Long.MAX_VALUE) {

      long time = System.currentTimeMillis();

      ArrayList<QCMutation> mutations2 = new ArrayList<>(mutations.size());

      for (QCMutation qcm : mutations) {
        qcm.resetDelay();
        if (time + qcm.getDelay(MILLISECONDS) > qcm.entryTime + timeout) {
          TimedOutException toe;
          if (server != null) {
            toe = new TimedOutException(Collections.singleton(server.toString()));
          } else {
            toe = new TimedOutException("Conditional mutation timed out");
          }

          qcm.queueResult(new Result(toe, qcm, (server == null ? null : server.toString())));
        } else {
          mutations2.add(qcm);
        }
      }

      if (!mutations2.isEmpty()) {
        failedMutations.addAll(mutations2);
      }

    } else {
      mutations.forEach(QCMutation::resetDelay);
      failedMutations.addAll(mutations);
    }
  }

  private void queue(List<QCMutation> mutations) {
    List<QCMutation> failures = new ArrayList<>();
    Map<String,TabletServerMutations<QCMutation>> binnedMutations = new HashMap<>();

    try {
      locator.binMutations(context, mutations, binnedMutations, failures);

      if (failures.size() == mutations.size()) {
        context.requireNotDeleted(tableId);
        context.requireNotOffline(tableId, tableName);
      }

    } catch (Exception e) {
      mutations.forEach(qcm -> qcm.queueResult(new Result(e, qcm, null)));

      // do not want to queue anything that was put in before binMutations() failed
      failures.clear();
      binnedMutations.clear();
    }

    if (!failures.isEmpty()) {
      queueRetry(failures, null);
    }

    binnedMutations.forEach(this::queue);
  }

  private void queue(String location, TabletServerMutations<QCMutation> mutations) {

    ServerQueue serverQueue = getServerQueue(location);

    synchronized (serverQueue) {
      serverQueue.queue.add(mutations);
      // never execute more than one task per server
      if (!serverQueue.taskQueued) {
        threadPool.execute(new SendTask(location));
        serverQueue.taskQueued = true;
      }
    }

  }

  private void reschedule(SendTask task) {
    ServerQueue serverQueue = getServerQueue(task.location);
    // just finished processing work for this server, could reschedule if it has more work or
    // immediately process the work
    // this code reschedules the the server for processing later... there may be other queues with
    // more data that need to be processed... also it will give the current server time to build
    // up more data... the thinking is that rescheduling instead or processing immediately will
    // result in bigger batches and less RPC overhead

    synchronized (serverQueue) {
      if (serverQueue.queue.isEmpty()) {
        serverQueue.taskQueued = false;
      } else {
        threadPool.execute(task);
      }
    }

  }

  private TabletServerMutations<QCMutation> dequeue(String location) {
    var queue = getServerQueue(location).queue;

    var mutations = new ArrayList<TabletServerMutations<QCMutation>>();
    queue.drainTo(mutations);

    if (mutations.isEmpty()) {
      return null;
    }

    if (mutations.size() == 1) {
      return mutations.get(0);
    } else {
      // merge multiple request to a single tablet server
      TabletServerMutations<QCMutation> tsm = mutations.get(0);

      for (int i = 1; i < mutations.size(); i++) {
        mutations.get(i).getMutations().forEach((keyExtent, mutationList) -> tsm.getMutations()
            .computeIfAbsent(keyExtent, k -> new ArrayList<>()).addAll(mutationList));
      }

      return tsm;
    }
  }

  ConditionalWriterImpl(ClientContext context, TableId tableId, String tableName,
      ConditionalWriterConfig config) {
    this.context = context;
    this.auths = config.getAuthorizations();
    this.ve = new VisibilityEvaluator(config.getAuthorizations());
    this.threadPool = context.threadPools().createScheduledExecutorService(
        config.getMaxWriteThreads(), this.getClass().getSimpleName(), false);
    this.locator = new SyncingTabletLocator(context, tableId);
    this.serverQueues = new HashMap<>();
    this.tableId = tableId;
    this.tableName = tableName;
    this.timeout = config.getTimeout(MILLISECONDS);
    this.durability = config.getDurability();
    this.classLoaderContext = config.getClassLoaderContext();

    Runnable failureHandler = () -> {
      List<QCMutation> mutations = new ArrayList<>();
      failedMutations.drainTo(mutations);
      if (!mutations.isEmpty()) {
        queue(mutations);
      }
    };

    failureTaskFuture = threadPool.scheduleAtFixedRate(failureHandler, 250, 250, MILLISECONDS);
  }

  @Override
  public Iterator<Result> write(Iterator<ConditionalMutation> mutations) {

    ThreadPools.ensureRunning(failureTaskFuture,
        "Background task that re-queues failed mutations has exited.");

    BlockingQueue<Result> resultQueue = new LinkedBlockingQueue<>();

    List<QCMutation> mutationList = new ArrayList<>();

    int count = 0;

    long entryTime = System.currentTimeMillis();

    mloop: while (mutations.hasNext()) {
      ConditionalMutation mut = mutations.next();
      count++;

      if (mut.getConditions().isEmpty()) {
        throw new IllegalArgumentException(
            "ConditionalMutation had no conditions " + new String(mut.getRow(), UTF_8));
      }

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
        if (mutations != null) {
          sendToServer(HostAndPort.fromString(location), mutations);
        }
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
    HostAndPort location;
    String lockId;
    long sessionID;
    boolean reserved;
    long lastAccessTime;
    long ttl;

    boolean isActive() {
      return System.currentTimeMillis() - lastAccessTime < ttl * .95;
    }
  }

  private HashMap<HostAndPort,SessionID> cachedSessionIDs = new HashMap<>();

  private SessionID reserveSessionID(HostAndPort location, TabletClientService.Iface client,
      TInfo tinfo) throws ThriftSecurityException, TException {
    // avoid cost of repeatedly making RPC to create sessions, reuse sessions
    synchronized (cachedSessionIDs) {
      SessionID sid = cachedSessionIDs.get(location);
      if (sid != null) {
        if (sid.reserved) {
          throw new IllegalStateException();
        }

        if (sid.isActive()) {
          sid.reserved = true;
          return sid;
        } else {
          cachedSessionIDs.remove(location);
        }
      }
    }

    TConditionalSession tcs = client.startConditionalUpdate(tinfo, context.rpcCreds(),
        ByteBufferUtil.toByteBuffers(auths.getAuthorizations()), tableId.canonical(),
        DurabilityImpl.toThrift(durability), this.classLoaderContext);

    synchronized (cachedSessionIDs) {
      SessionID sid = new SessionID();
      sid.reserved = true;
      sid.sessionID = tcs.sessionId;
      sid.lockId = tcs.tserverLock;
      sid.ttl = tcs.ttl;
      sid.location = location;
      if (cachedSessionIDs.put(location, sid) != null) {
        throw new IllegalStateException();
      }

      return sid;
    }

  }

  private void invalidateSessionID(HostAndPort location) {
    synchronized (cachedSessionIDs) {
      cachedSessionIDs.remove(location);
    }

  }

  private void unreserveSessionID(HostAndPort location) {
    synchronized (cachedSessionIDs) {
      SessionID sid = cachedSessionIDs.get(location);
      if (sid != null) {
        if (!sid.reserved) {
          throw new IllegalStateException();
        }
        sid.reserved = false;
        sid.lastAccessTime = System.currentTimeMillis();
      }
    }
  }

  List<SessionID> getActiveSessions() {
    ArrayList<SessionID> activeSessions = new ArrayList<>();
    for (SessionID sid : cachedSessionIDs.values()) {
      if (sid.isActive()) {
        activeSessions.add(sid);
      }
    }
    return activeSessions;
  }

  private TabletClientService.Iface getClient(HostAndPort location) throws TTransportException {
    TabletClientService.Iface client;
    if (timeout < context.getClientTimeoutInMillis()) {
      client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, location, context, timeout);
    } else {
      client = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, location, context);
    }
    return client;
  }

  private void sendToServer(HostAndPort location, TabletServerMutations<QCMutation> mutations) {
    TabletClientService.Iface client = null;

    TInfo tinfo = TraceUtil.traceInfo();

    Map<Long,CMK> cmidToCm = new HashMap<>();
    MutableLong cmid = new MutableLong(0);

    SessionID sessionId = null;

    try {
      Map<TKeyExtent,List<TConditionalMutation>> tmutations = new HashMap<>();

      CompressedIterators compressedIters = new CompressedIterators();
      convertMutations(mutations, cmidToCm, cmid, tmutations, compressedIters);

      // getClient() call must come after converMutations in case it throws a TException
      client = getClient(location);

      List<TCMResult> tresults = null;
      while (tresults == null) {
        try {
          sessionId = reserveSessionID(location, client, tinfo);
          tresults = client.conditionalUpdate(tinfo, sessionId.sessionID, tmutations,
              compressedIters.getSymbolTable());
        } catch (NoSuchScanIDException nssie) {
          sessionId = null;
          invalidateSessionID(location);
        }
      }

      HashSet<KeyExtent> extentsToInvalidate = new HashSet<>();

      ArrayList<QCMutation> ignored = new ArrayList<>();

      for (TCMResult tcmResult : tresults) {
        if (tcmResult.status == TCMStatus.IGNORED) {
          CMK cmk = cmidToCm.get(tcmResult.cmid);
          ignored.add(cmk.cm);
          extentsToInvalidate.add(cmk.ke);
        } else {
          QCMutation qcm = cmidToCm.get(tcmResult.cmid).cm;
          qcm.queueResult(new Result(fromThrift(tcmResult.status), qcm, location.toString()));
        }
      }

      for (KeyExtent ke : extentsToInvalidate) {
        locator.invalidateCache(ke);
      }

      queueRetry(ignored, location);

    } catch (ThriftSecurityException tse) {
      AccumuloSecurityException ase =
          new AccumuloSecurityException(context.getCredentials().getPrincipal(), tse.getCode(),
              context.getPrintableTableInfoFromId(tableId), tse);
      queueException(location, cmidToCm, ase);
    } catch (TApplicationException tae) {
      queueException(location, cmidToCm, new AccumuloServerException(location.toString(), tae));
    } catch (TException e) {
      locator.invalidateCache(context, location.toString());
      invalidateSession(location, cmidToCm, sessionId);
    } catch (Exception e) {
      queueException(location, cmidToCm, e);
    } finally {
      if (sessionId != null) {
        unreserveSessionID(location);
      }
      ThriftUtil.returnClient((TServiceClient) client, context);
    }
  }

  private void queueRetry(Map<Long,CMK> cmidToCm, HostAndPort location) {
    ArrayList<QCMutation> ignored = new ArrayList<>();
    for (CMK cmk : cmidToCm.values()) {
      ignored.add(cmk.cm);
    }
    queueRetry(ignored, location);
  }

  private void queueException(HostAndPort location, Map<Long,CMK> cmidToCm, Exception e) {
    for (CMK cmk : cmidToCm.values()) {
      cmk.cm.queueResult(new Result(e, cmk.cm, location.toString()));
    }
  }

  private void invalidateSession(HostAndPort location, Map<Long,CMK> cmidToCm,
      SessionID sessionId) {
    if (sessionId == null) {
      queueRetry(cmidToCm, location);
    } else {
      try {
        invalidateSession(sessionId, location);
        for (CMK cmk : cmidToCm.values()) {
          cmk.cm.queueResult(new Result(Status.UNKNOWN, cmk.cm, location.toString()));
        }
      } catch (Exception e2) {
        queueException(location, cmidToCm, e2);
      }
    }
  }

  /**
   * The purpose of this code is to ensure that a conditional mutation will not execute when its
   * status is unknown. This allows a user to read the row when the status is unknown and not have
   * to worry about the tserver applying the mutation after the scan.
   *
   * <p>
   * If a conditional mutation is taking a long time to process, then this method will wait for it
   * to finish... unless this exceeds timeout.
   */
  private void invalidateSession(SessionID sessionId, HostAndPort location)
      throws AccumuloException {

    long sleepTime = 50;

    long startTime = System.currentTimeMillis();

    LockID lid = new LockID(context.getZooKeeperRoot() + Constants.ZTSERVERS, sessionId.lockId);

    while (true) {
      if (!ServiceLock.isLockHeld(context.getZooCache(), lid)) {
        // ACCUMULO-1152 added a tserver lock check to the tablet location cache, so this
        // invalidation prevents future attempts to contact the
        // tserver even its gone zombie and is still running w/o a lock
        locator.invalidateCache(context, location.toString());
        return;
      }

      try {
        // if the mutation is currently processing, this method will block until its done or times
        // out
        invalidateSession(sessionId.sessionID, location);

        return;
      } catch (TApplicationException tae) {
        throw new AccumuloServerException(location.toString(), tae);
      } catch (TException e) {
        locator.invalidateCache(context, location.toString());
      }

      if ((System.currentTimeMillis() - startTime) + sleepTime > timeout) {
        throw new TimedOutException(Collections.singleton(location.toString()));
      }

      sleepUninterruptibly(sleepTime, MILLISECONDS);
      sleepTime = Math.min(2 * sleepTime, MAX_SLEEP);

    }

  }

  private void invalidateSession(long sessionId, HostAndPort location) throws TException {
    TabletClientService.Iface client = null;

    TInfo tinfo = TraceUtil.traceInfo();

    try {
      client = getClient(location);
      client.invalidateConditionalUpdate(tinfo, sessionId);
    } finally {
      ThriftUtil.returnClient((TServiceClient) client, context);
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

  private void convertMutations(TabletServerMutations<QCMutation> mutations, Map<Long,CMK> cmidToCm,
      MutableLong cmid, Map<TKeyExtent,List<TConditionalMutation>> tmutations,
      CompressedIterators compressedIters) {

    mutations.getMutations().forEach((keyExtent, mutationList) -> {
      var tcondMutaions = new ArrayList<TConditionalMutation>();

      for (var cm : mutationList) {
        TMutation tm = cm.toThrift();

        List<TCondition> conditions = convertConditions(cm, compressedIters);

        cmidToCm.put(cmid.longValue(), new CMK(keyExtent, cm));
        TConditionalMutation tcm = new TConditionalMutation(conditions, tm, cmid.longValue());
        cmid.increment();
        tcondMutaions.add(tcm);
      }

      tmutations.put(keyExtent.toThrift(), tcondMutaions);
    });
  }

  private static final Comparator<Long> TIMESTAMP_COMPARATOR =
      Comparator.nullsFirst(Comparator.reverseOrder());

  static final Comparator<Condition> CONDITION_COMPARATOR =
      Comparator.comparing(Condition::getFamily).thenComparing(Condition::getQualifier)
          .thenComparing(Condition::getVisibility)
          .thenComparing(Condition::getTimestamp, TIMESTAMP_COMPARATOR);

  private List<TCondition> convertConditions(ConditionalMutation cm,
      CompressedIterators compressedIters) {
    List<TCondition> conditions = new ArrayList<>(cm.getConditions().size());

    // sort conditions inorder to get better lookup performance. Sort on client side so tserver does
    // not have to do it.
    Condition[] ca = cm.getConditions().toArray(new Condition[cm.getConditions().size()]);
    Arrays.sort(ca, CONDITION_COMPARATOR);

    for (Condition cond : ca) {
      long ts = 0;
      boolean hasTs = false;

      if (cond.getTimestamp() != null) {
        ts = cond.getTimestamp();
        hasTs = true;
      }

      ByteBuffer iters = compressedIters.compress(cond.getIterators());

      TCondition tc = new TCondition(ByteBufferUtil.toByteBuffers(cond.getFamily()),
          ByteBufferUtil.toByteBuffers(cond.getQualifier()),
          ByteBufferUtil.toByteBuffers(cond.getVisibility()), ts, hasTs,
          ByteBufferUtil.toByteBuffers(cond.getValue()), iters);

      conditions.add(tc);
    }

    return conditions;
  }

  private boolean isVisible(ByteSequence cv) {
    Text testVis = new Text(cv.toArray());
    if (testVis.getLength() == 0) {
      return true;
    }

    Boolean b = cache.get(testVis);
    if (b != null) {
      return b;
    }

    try {
      boolean bb = ve.evaluate(new ColumnVisibility(testVis));
      cache.put(new Text(testVis), bb);
      return bb;
    } catch (VisibilityParseException | BadArgumentException e) {
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
    context.executeCleanupTask(Threads.createNamedRunnable("ConditionalWriterCleanupTask",
        new CleanupTask(getActiveSessions())));
  }

}
