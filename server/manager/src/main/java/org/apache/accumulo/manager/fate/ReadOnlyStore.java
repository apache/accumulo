/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.manager.fate;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;

import org.apache.accumulo.fate.FateTransactionStatus;
import org.apache.accumulo.manager.Manager;

/**
 * This store decorates a TStore to make sure it can not be modified.
 *
 * Unlike relying directly on the ReadOnlyTStore interface, this class will not allow subsequent
 * users to cast back to a mutable TStore successfully.
 */
public class ReadOnlyStore implements ReadOnlyTStore {

  private final ZooStore store;

  /**
   * @param store
   *          may not be null
   */
  public ReadOnlyStore(ZooStore store) {
    requireNonNull(store);
    this.store = store;
  }

  @Override
  public long reserve() {
    return store.reserve();
  }

  @Override
  public void reserve(long tid) {
    store.reserve(tid);
  }

  @Override
  public void unreserve(long tid, long deferTime) {
    store.unreserve(tid, deferTime);
  }

  /**
   * Decorates a Repo to make sure it is treated as a ReadOnlyRepo.
   *
   * Similar to ReadOnlyStore, won't allow subsequent user to cast a ReadOnlyRepo back to a mutable
   * Repo.
   */
  protected static class ReadOnlyRepoWrapper implements ReadOnlyRepo {
    private final Repo repo;

    /**
     * @param repo
     *          may not be null
     */
    public ReadOnlyRepoWrapper(Repo repo) {
      requireNonNull(repo);
      this.repo = repo;
    }

    @Override
    public long isReady(long tid, Manager environment) throws Exception {
      return repo.isReady(tid, environment);
    }

    @Override
    public String getDescription() {
      return repo.getDescription();
    }
  }

  @Override
  public ReadOnlyRepo top(long tid) {
    return new ReadOnlyRepoWrapper(store.top(tid));
  }

  @Override
  public FateTransactionStatus getStatus(long tid) {
    return store.getStatus(tid);
  }

  @Override
  public FateTransactionStatus waitForStatusChange(long tid,
      EnumSet<FateTransactionStatus> expected) {
    return store.waitForStatusChange(tid, expected);
  }

  @Override
  public Serializable getProperty(long tid, String prop) {
    return store.getProperty(tid, prop);
  }

  @Override
  public List<Long> list() {
    return store.list();
  }

  @Override
  public List<ReadOnlyRepo> getStack(long tid) {
    return store.getStack(tid);
  }

  @Override
  public long timeCreated(long tid) {
    return store.timeCreated(tid);
  }
}
