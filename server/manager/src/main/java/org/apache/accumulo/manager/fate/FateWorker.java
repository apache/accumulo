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
package org.apache.accumulo.manager.fate;

import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FatePartition;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.thrift.FateWorkerService;
import org.apache.accumulo.core.fate.thrift.TFatePartition;
import org.apache.accumulo.core.fate.thrift.TFatePartitions;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.manager.metrics.fate.FateExecutorMetricsProducer;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FateWorker implements FateWorkerService.Iface {

  private static final Logger log = LoggerFactory.getLogger(FateWorker.class);
  private final ServerContext context;
  private final AuditedSecurityOperation security;
  private final LiveTServerSet liveTserverSet;
  private final FateFactory fateFactory;
  private Fate<FateEnv> fate;
  private FateWorkerEnv fateWorkerEnv;

  public interface FateFactory {
    Fate<FateEnv> create(FateEnv env, FateStore<FateEnv> store, ServerContext context);
  }

  public FateWorker(ServerContext ctx, LiveTServerSet liveTServerSet, FateFactory fateFactory) {
    this.context = ctx;
    this.security = ctx.getSecurityOperation();
    this.fate = null;
    this.liveTserverSet = liveTServerSet;
    this.fateFactory = fateFactory;
  }

  public synchronized void setLock(ServiceLock lock) {
    fateWorkerEnv = new FateWorkerEnv(context, lock, liveTserverSet);
    Predicate<ZooUtil.LockID> isLockHeld = l -> ServiceLock.isLockHeld(context.getZooCache(), l);
    UserFateStore<FateEnv> store =
        new UserFateStore<>(context, SystemTables.FATE.tableName(), lock.getLockID(), isLockHeld);
    this.fate = fateFactory.create(fateWorkerEnv, store, context);
  }

  private Long expectedUpdateId = null;

  @Override
  public TFatePartitions getPartitions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    // generate a new one time use update id
    long updateId = RANDOM.get().nextLong();

    // Getting the partitions and setting the new update id must be mutually exclusive with any
    // updates of the partitions concurrently executing. This ensures the new update id goes with
    // the current partitions returned.
    synchronized (this) {
      // invalidate any queued partitions update that have not executed yet and set the new update
      // id
      expectedUpdateId = updateId;

      if (fate == null) {
        return new TFatePartitions(updateId, List.of());
      } else {
        return new TFatePartitions(updateId,
            fate.getPartitions().stream().map(FatePartition::toThrift).toList());
      }
    }
  }

  @Override
  public boolean setPartitions(TInfo tinfo, TCredentials credentials, long updateId,
      List<TFatePartition> desired) throws ThriftSecurityException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    synchronized (this) {
      if (fate != null && expectedUpdateId != null && updateId == expectedUpdateId) {
        // Set to null which makes it so that an update id can only be used once.
        expectedUpdateId = null;
        var desiredSet = desired.stream().map(FatePartition::from).collect(Collectors.toSet());
        var oldPartitions = fate.setPartitions(desiredSet);
        log.info("Changed partitions from {} to {}", oldPartitions, desiredSet);
        return true;
      } else {
        log.debug(
            "Did not change partitions to {} expectedUpdateId:{} updateId:{} localFate==null:{}",
            desired, expectedUpdateId, updateId, fate == null);
        return false;
      }
    }
  }

  @Override
  public void seeded(TInfo tinfo, TCredentials credentials, List<TFatePartition> tpartitions)
      throws TException {

    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    Fate<FateEnv> localFate;
    synchronized (this) {
      localFate = fate;
    }

    if (localFate != null) {
      localFate.seeded(tpartitions.stream().map(FatePartition::from).collect(Collectors.toSet()));
    }
  }

  public synchronized void stop() {
    fate.shutdown(1, TimeUnit.MINUTES);
    fate.close();
    fateWorkerEnv.stop();
    fate = null;
    fateWorkerEnv = null;
  }

  public synchronized List<MetricsProducer> getMetricsProducers() {
    Preconditions.checkState(fate != null, "Not started yet");
    return List.of(new FateExecutorMetricsProducer(context, fate.getFateExecutors(), context
        .getConfiguration().getTimeInMillis(Property.MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL)));

  }
}
