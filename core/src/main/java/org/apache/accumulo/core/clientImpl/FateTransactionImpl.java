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

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.admin.FateTransaction;
import org.apache.accumulo.core.clientImpl.thrift.TFateTransaction;
import org.apache.accumulo.core.data.FateTxId;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.ServiceLock;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;

public class FateTransactionImpl implements FateTransaction, Comparable<Long> {
  private final long txid;
  private final FateTransaction.Status status;
  private final String debug;
  private final List<String> hlocks;
  private final List<String> wlocks;
  private final String top;
  private final long timeCreated;
  private final String stackInfo;

  public FateTransactionImpl(Long tid, String status, String debug, List<String> hlocks,
      List<String> wlocks, String top, Long timeCreated, String stackInfo) {
    this.txid = tid;
    this.status = FateTransaction.Status.valueOf(status);
    this.debug = debug;
    this.hlocks = Collections.unmodifiableList(hlocks);
    this.wlocks = Collections.unmodifiableList(wlocks);
    this.top = top;
    this.timeCreated = timeCreated;
    this.stackInfo = stackInfo;
  }

  @Override
  public FateTxId getId() {
    return FateTxId.of(txid);
  }

  @Override
  public Status getStatus() {
    return status;
  }

  @Override
  public String getDebug() {
    return debug;
  }

  @Override
  public List<String> getHeldLocks() {
    return hlocks;
  }

  @Override
  public List<String> getWaitingLocks() {
    return wlocks;
  }

  @Override
  public String getTop() {
    return top;
  }

  @Override
  public String getTimeCreatedFormatted() {
    return timeCreated > 0 ? new Date(timeCreated).toInstant().atZone(ZoneOffset.UTC)
        .format(DateTimeFormatter.ISO_DATE_TIME) : "ERROR";
  }

  @Override
  public long getTimeCreated() {
    return timeCreated;
  }

  @Override
  public String getStackInfo() {
    return stackInfo;
  }

  @Override
  public void fail() {
    // executeAdminOperation(AdminOperation.FAIL, Set.of(getId()), null);
  }

  @Override
  public void delete() throws AccumuloException {
    // executeAdminOperation(AdminOperation.DELETE, Set.of(getId()), null);
  }

  public List<FateTransaction> fateStatus(Set<FateTxId> txids, List<String> tStatus)
      throws AccumuloException {
    // set collections to empty if user passes in null
    if (txids == null) {
      txids = Set.of();
    }
    if (tStatus == null) {
      tStatus = List.of();
    }
    // List<FateTransaction> txStatus = new ArrayList<>();
    // for (var tx : executeAdminOperation(AdminOperation.PRINT, txids, tStatus)) {
    // txStatus.add(new FateTransactionImpl(tx.getTxid(), tx.getTstatus(), tx.getDebug(),
    // tx.getHlocks(), tx.getWlocks(), tx.getTop(), tx.getTimecreated(), tx.getStackInfo()));
    // }
    // return txStatus;
    return null;
  }

  private <T> boolean failTx(AdminUtil<T> admin, ZooStore<T> zs, ZooReaderWriter zk,
      ServiceLock.ServiceLockPath managerLockPath, String[] args) {
    boolean success = true;
    for (int i = 1; i < args.length; i++) {
      if (!admin.prepFail(zs, zk, managerLockPath, args[i])) {
        System.out.printf("Could not fail transaction: %s%n", args[i]);
        return !success;
      }
    }
    return success;
  }

  @Override
  public int compareTo(Long o) {
    return Long.compare(this.getId().canonical(), o);
  }

  public static FateTransaction fromThrift(TFateTransaction tFateTx) {
    return new FateTransactionImpl(tFateTx.txid, tFateTx.tstatus, tFateTx.debug, tFateTx.hlocks,
        tFateTx.wlocks, tFateTx.top, tFateTx.timecreated, tFateTx.stackInfo);
  }
}
