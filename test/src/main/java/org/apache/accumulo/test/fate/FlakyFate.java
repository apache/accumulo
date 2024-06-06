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
package org.apache.accumulo.test.fate;

import java.util.function.Function;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.Repo;

import com.google.common.base.Preconditions;

/**
 * An implementation of fate that runs fate steps multiple times to ensure they are idempotent.
 */
public class FlakyFate<T> extends Fate<T> {

  public FlakyFate(T environment, FateStore<T> store, Function<Repo<T>,String> toLogStrFunc,
      AccumuloConfiguration conf) {
    super(environment, store, toLogStrFunc, conf);
  }

  @Override
  protected Repo<T> executeCall(FateId fateId, Repo<T> repo) throws Exception {
    /*
     * This function call assumes that isRead was already called once. So it runs
     * call(),isReady(),call() to simulate a situation like isReady(), call(), fault, isReady()
     * again, call() again.
     */
    var next1 = super.executeCall(fateId, repo);
    Preconditions.checkState(super.executeIsReady(fateId, repo) == 0);
    var next2 = super.executeCall(fateId, repo);
    // do some basic checks to ensure similar things were returned
    if (next1 == null) {
      Preconditions.checkState(next2 == null);
    } else {
      Preconditions.checkState(next2 != null);
      Preconditions.checkState(next1.getClass().equals(next2.getClass()));
    }
    return next2;
  }
}
