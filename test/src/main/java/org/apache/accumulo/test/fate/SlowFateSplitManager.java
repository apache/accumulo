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

import java.io.IOException;

import org.apache.accumulo.core.cli.ServerOpts;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.TraceRepo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * See {@link SlowFateSplit}
 */
public class SlowFateSplitManager extends Manager {
  private static final Logger log = LoggerFactory.getLogger(SlowFateSplitManager.class);
  // causes splits to take at least 10 seconds to complete
  public static final long SLEEP_TIME_MS = 10_000;
  // important that this is an op that can be initiated on some system table as well as user tables
  public static final Fate.FateOperation SLOW_OP = Fate.FateOperation.TABLE_SPLIT;

  protected SlowFateSplitManager(ServerOpts opts, String[] args) throws IOException {
    super(opts, ServerContext::new, args);
  }

  @Override
  protected Fate<FateEnv> initializeFateInstance(ServerContext context, FateStore<FateEnv> store) {
    log.info("Creating Slow Split Fate for {}", store.type());
    return new SlowFateSplit<>(this, store, TraceRepo::toLogString, getConfiguration());
  }

  public static void main(String[] args) throws Exception {
    try (SlowFateSplitManager manager = new SlowFateSplitManager(new ServerOpts(), args)) {
      manager.runServer();
    }
  }
}
