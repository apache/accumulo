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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FatePartition;
import org.apache.accumulo.core.fate.thrift.FateWorkerService;
import org.apache.accumulo.core.fate.thrift.TFatePartition;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tableOps.TraceRepo;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FateWorker implements FateWorkerService.Iface {

  private static final Logger log = LoggerFactory.getLogger(FateWorker.class);
  private final ServerContext context;
  private final AuditedSecurityOperation security;
  private final Set<FatePartition> currentPartitions;
  private volatile Fate<FateEnv> fate;


  public FateWorker(ServerContext ctx, Supplier<ServiceLock> serviceLockSupplier) {
    this.context = ctx;
    this.security = ctx.getSecurityOperation();
    this.currentPartitions = Collections.synchronizedSet(new HashSet<>());
    this.fate = null;
  }

  public void setLock(ServiceLock lock){
    FateEnv env = new FateWorkerEnv(context, lock);
    Predicate<ZooUtil.LockID> isLockHeld =
            l -> ServiceLock.isLockHeld(context.getZooCache(), l);
    UserFateStore<FateEnv> store = new UserFateStore<>(context,
            SystemTables.FATE.tableName(), lock.getLockID(), isLockHeld);
    this.fate = new Fate<>(env, store, false, TraceRepo::toLogString,
            context.getConfiguration(), context.getScheduledExecutor());
    // TODO where will the 2 fate cleanup task run?

  }

  @Override
  public List<TFatePartition> getPartitions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    synchronized (currentPartitions) {
      return currentPartitions.stream().map(FatePartition::toThrift).toList();
    }
  }

  @Override
  public boolean setPartitions(TInfo tinfo, TCredentials credentials, List<TFatePartition> expected,
      List<TFatePartition> desired) throws ThriftSecurityException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    var expectedSet = expected.stream().map(FatePartition::from).collect(Collectors.toSet());
    synchronized (currentPartitions) {
      if (currentPartitions.equals(expectedSet) && fate != null) {
        expectedSet.forEach(p -> log.info("old partition {}", p));
        currentPartitions.clear();
        desired.stream().map(FatePartition::from).forEach(currentPartitions::add);
        desired.stream().map(FatePartition::from).forEach(p -> log.info("new partition {}", p));
        log.info("Changed partitions from {} to {}", expectedSet, currentPartitions);
        fate.setPartitions(Set.copyOf(currentPartitions));
        return true;
      } else {
        log.info("Did not change partitions to {} because {} != {}", desired, expectedSet,
            currentPartitions);
        return false;
      }
    }
  }
}
