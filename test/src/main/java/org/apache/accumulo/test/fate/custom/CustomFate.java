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
package org.apache.accumulo.test.fate.custom;

import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Function;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateExecutor;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.Repo;

public class CustomFate<T> extends Fate<T> {

  /**
   * Creates a Fault-tolerant executor for the given store type.
   *
   * @param runDeadResCleaner Whether this Custom FATE should run a dead reservation cleaner. The
   *        real FATEs need have a cleaner, but may be undesirable in testing.
   * @param toLogStrFunc A function that converts Repo to Strings that are suitable for logging
   */
  public CustomFate(T environment, FateStore<T> store, boolean runDeadResCleaner,
      Function<Repo<T>,String> toLogStrFunc, AccumuloConfiguration conf,
      ScheduledThreadPoolExecutor genSchedExecutor) {
    super(environment, store, runDeadResCleaner, toLogStrFunc, conf, genSchedExecutor);
  }

  public static class CustomFateExecutor<T> extends FateExecutor<T> {
    public CustomFateExecutor(Fate<T> fate, T environment, Set<FateOperation> fateOps, int poolSize,
        String name) {
      super(fate, environment, fateOps, poolSize, name);
    }
  }
}
