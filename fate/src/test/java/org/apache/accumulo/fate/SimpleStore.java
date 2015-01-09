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
package org.apache.accumulo.fate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

/**
 * Transient in memory store for transactions.
 */
public class SimpleStore<T> implements TStore<T> {

  private long nextId = 1;
  private Map<Long,TStatus> statuses = new HashMap<Long,TStore.TStatus>();
  private Set<Long> reserved = new HashSet<Long>();

  @Override
  public long create() {
    statuses.put(nextId, TStatus.NEW);
    return nextId++;
  }

  @Override
  public long reserve() {
    throw new NotImplementedException();
  }

  @Override
  public void reserve(long tid) {
    if (reserved.contains(tid))
      throw new IllegalStateException(); // zoo store would wait, but do not expect test to reserve twice... if test change, then change this
    reserved.add(tid);
  }

  @Override
  public void unreserve(long tid, long deferTime) {
    if (!reserved.remove(tid)) {
      throw new IllegalStateException();
    }
  }

  @Override
  public Repo<T> top(long tid) {
    throw new NotImplementedException();
  }

  @Override
  public void push(long tid, Repo<T> repo) throws StackOverflowException {
    throw new NotImplementedException();
  }

  @Override
  public void pop(long tid) {
    throw new NotImplementedException();
  }

  @Override
  public org.apache.accumulo.fate.TStore.TStatus getStatus(long tid) {
    if (!reserved.contains(tid))
      throw new IllegalStateException();

    TStatus status = statuses.get(tid);
    if (status == null)
      return TStatus.UNKNOWN;
    return status;
  }

  @Override
  public void setStatus(long tid, org.apache.accumulo.fate.TStore.TStatus status) {
    if (!reserved.contains(tid))
      throw new IllegalStateException();
    if (!statuses.containsKey(tid))
      throw new IllegalStateException();
    statuses.put(tid, status);
  }

  @Override
  public org.apache.accumulo.fate.TStore.TStatus waitForStatusChange(long tid, EnumSet<org.apache.accumulo.fate.TStore.TStatus> expected) {
    throw new NotImplementedException();
  }

  @Override
  public void setProperty(long tid, String prop, Serializable val) {
    throw new NotImplementedException();
  }

  @Override
  public Serializable getProperty(long tid, String prop) {
    throw new NotImplementedException();
  }

  @Override
  public void delete(long tid) {
    if (!reserved.contains(tid))
      throw new IllegalStateException();
    statuses.remove(tid);
  }

  @Override
  public List<Long> list() {
    return new ArrayList<Long>(statuses.keySet());
  }

}
