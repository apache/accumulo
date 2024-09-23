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
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.Repo;

/**
 * A FATE which performs the dead reservation cleanup with a much shorter delay between
 */
public class FastFate<T> extends Fate<T> {
  public static final long delay = 15;

  public FastFate(T environment, FateStore<T> store, boolean runDeadResCleaner,
      Function<Repo<T>,String> toLogStrFunc, AccumuloConfiguration conf) {
    super(environment, store, runDeadResCleaner, toLogStrFunc, conf);
  }

  @Override
  protected long getDeadResCleanupDelay() {
    return delay;
  }
}
