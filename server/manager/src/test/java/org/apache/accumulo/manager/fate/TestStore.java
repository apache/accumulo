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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.fate.FateTransactionStatus;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.zookeeper.KeeperException;

/**
 * Transient in memory store for transactions.
 */
public class TestStore extends ZooStore {

  private long nextId = 1;
  private Map<Long,FateTransactionStatus> statuses = new HashMap<>();
  private Set<Long> reserved = new HashSet<>();

  public TestStore(String path, ZooReaderWriter zk) throws KeeperException, InterruptedException {
    super(path, zk);
  }

  @Override
  public long create() {
    statuses.put(nextId, FateTransactionStatus.NEW);
    return nextId++;
  }

  @Override
  public void reserve(long tid) {
    if (reserved.contains(tid))
      throw new IllegalStateException(); // zoo store would wait, but do not expect test to reserve
                                         // twice... if test change, then change this
    reserved.add(tid);
  }

  @Override
  public boolean tryReserve(long tid) {
    synchronized (this) {
      if (!reserved.contains(tid)) {
        reserve(tid);
        return true;
      }
      return false;
    }
  }

  @Override
  public void unreserve(long tid, long deferTime) {
    if (!reserved.remove(tid)) {
      throw new IllegalStateException();
    }
  }

  @Override
  public FateTransactionStatus getStatus(long tid) {
    if (!reserved.contains(tid))
      throw new IllegalStateException();

    FateTransactionStatus status = statuses.get(tid);
    if (status == null)
      return FateTransactionStatus.UNKNOWN;
    return status;
  }

  @Override
  public void setStatus(long tid, FateTransactionStatus status) {
    if (!reserved.contains(tid))
      throw new IllegalStateException();
    if (!statuses.containsKey(tid))
      throw new IllegalStateException();
    statuses.put(tid, status);
  }

  @Override
  public void delete(long tid) {
    if (!reserved.contains(tid))
      throw new IllegalStateException();
    statuses.remove(tid);
  }

  @Override
  public List<Long> list() {
    return new ArrayList<>(statuses.keySet());
  }

}
