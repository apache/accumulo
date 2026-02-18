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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FatePartition;
import org.apache.accumulo.core.fate.thrift.FateWorkerService;
import org.apache.accumulo.core.fate.thrift.TFatePartition;
import org.apache.accumulo.core.fate.thrift.TFatePartitions;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.util.LazySingletons;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tableOps.TraceRepo;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FateWorker implements FateWorkerService.Iface {

  private static final Logger log = LoggerFactory.getLogger(FateWorker.class);
  private final ServerContext context;
  private final AuditedSecurityOperation security;
  private volatile Fate<FateEnv> fate;
  private volatile FateWorkerEnv fateWorkerEnv;

  public FateWorker(ServerContext ctx) {
    this.context = ctx;
    this.security = ctx.getSecurityOperation();
    this.fate = null;
    // TODO fate metrics... in the manager process it does not setup metrics until after it gets the
    // lock... also may want these metrics tagged differently for the server
  }

  public void setLock(ServiceLock lock) {
    fateWorkerEnv = new FateWorkerEnv(context, lock);
    Predicate<ZooUtil.LockID> isLockHeld = l -> ServiceLock.isLockHeld(context.getZooCache(), l);
    UserFateStore<FateEnv> store =
        new UserFateStore<>(context, SystemTables.FATE.tableName(), lock.getLockID(), isLockHeld);
    this.fate = new Fate<>(fateWorkerEnv, store, false, TraceRepo::toLogString,
        context.getConfiguration(), context.getScheduledExecutor());
  }

  private Long expectedUpdateId = null;

  @Override
  public TFatePartitions getPartitions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    var localFate = fate;

    // generate a new one time use update id
    long updateId = LazySingletons.RANDOM.get().nextLong();

    // Getting the partitions and setting the new update id must be mutually exclusive with any
    // updates of the partitions concurrently executing. This ensures the new update id goes with
    // the current partitions returned.
    synchronized (this) {
      // invalidate any queued partitions update that have not executed yet and set the new update
      // id
      expectedUpdateId = updateId;

      if (localFate == null) {
        return new TFatePartitions(updateId, List.of());
      } else {
        return new TFatePartitions(updateId,
            localFate.getPartitions().stream().map(FatePartition::toThrift).toList());
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
      var localFate = fate;
      if (localFate != null && expectedUpdateId != null && updateId == expectedUpdateId) {
        // Set to null which makes it so that an update id can only be used once.
        expectedUpdateId = null;
        var desiredSet = desired.stream().map(FatePartition::from).collect(Collectors.toSet());
        var oldPartitions = localFate.setPartitions(desiredSet);
        log.info("Changed partitions from {} to {}", oldPartitions, desiredSet);
        return true;
      } else {
        log.debug(
            "Did not change partitions to {} expectedUpdateId:{} updateId:{} localFate==null:{}",
            desired, expectedUpdateId, updateId, localFate == null);
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

  public void stop() {
    fate.shutdown(1, TimeUnit.MINUTES);
    fate.close();
    fateWorkerEnv.stop();
  }
}
