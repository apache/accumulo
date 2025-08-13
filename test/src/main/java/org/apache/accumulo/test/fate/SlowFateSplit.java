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

import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Function;

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateExecutor;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.test.functional.SlowIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Causes the first step in a Split FATE operation to sleep for
 * {@link SlowFateSplitManager#SLEEP_TIME_MS}. Split was chosen as it can be executed on system and
 * user tables (allowing for testing of meta and user fate ops) and is easy to avoid automatic
 * splits from occurring (just don't have tables exceed the split threshold in testing). This allows
 * us to sleep for splits we initiate via {@link TableOperation}. This is useful when we want a fate
 * thread to be occupied working on some operation. A potential alternative to this is compacting
 * with a {@link SlowIterator} attached to the table, however, this will result in the operation
 * never being ready ({@link Repo#isReady(FateId, Object)}). So, it would exist as an operation to
 * work on, but a thread will briefly reserve it, see it's not isReady, and unreserve it. This class
 * is useful when we need a fate op reserved by a thread and being worked on for a configurable
 * time, but don't have direct access to the Fate objects/are testing Fate as it operates within the
 * Manager instead of directly working with Fate objects.
 */
public class SlowFateSplit<T> extends Fate<T> {
  private static final Logger log = LoggerFactory.getLogger(SlowFateSplit.class);
  private boolean haveSlept = false;

  public SlowFateSplit(T environment, FateStore<T> store, Function<Repo<T>,String> toLogStrFunc,
      AccumuloConfiguration conf) {
    super(environment, store, false, toLogStrFunc, conf, new ScheduledThreadPoolExecutor(2));
  }

  @Override
  protected void startFateExecutors(T environment, AccumuloConfiguration conf,
      Set<FateExecutor<T>> fateExecutors) {
    for (var poolConfig : getPoolConfigurations(conf, getStore().type()).entrySet()) {
      fateExecutors.add(
          new SlowFateSplitExecutor(this, environment, poolConfig.getKey(), poolConfig.getValue()));
    }
  }

  private class SlowFateSplitExecutor extends FateExecutor<T> {
    private SlowFateSplitExecutor(Fate<T> fate, T environment, Set<Fate.FateOperation> fateOps,
        int poolSize) {
      super(fate, environment, fateOps, poolSize);
    }

    @Override
    protected Repo<T> executeCall(FateId fateId, Repo<T> repo) throws Exception {
      var next = super.executeCall(fateId, repo);
      var fateOp = (FateOperation) SlowFateSplit.this.getStore().read(fateId)
          .getTransactionInfo(TxInfo.FATE_OP);
      if (fateOp == SlowFateSplitManager.SLOW_OP && !haveSlept) {
        var sleepTime = SlowFateSplitManager.SLEEP_TIME_MS;
        log.debug("{} sleeping in {} for {}", fateId, getClass().getSimpleName(), sleepTime);
        Thread.sleep(sleepTime);
        log.debug("{} slept in {} for {}", fateId, getClass().getSimpleName(), sleepTime);
        haveSlept = true;
      }
      return next;
    }
  }
}
