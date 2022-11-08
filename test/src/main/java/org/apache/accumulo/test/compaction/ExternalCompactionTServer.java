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
package org.apache.accumulo.test.compaction;

import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.accumulo.tserver.TabletClientHandler;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.WriteTracker;

public class ExternalCompactionTServer extends TabletServer {

  ExternalCompactionTServer(ServerOpts opts, String[] args) {
    super(opts, args);
  }

  @Override
  protected TabletClientHandler newTabletClientHandler(TransactionWatcher watcher,
      WriteTracker writeTracker) {
    return new NonCommittingExternalCompactionTabletClientHandler(this, watcher, writeTracker);
  }

  public static void main(String[] args) throws Exception {
    try (
        ExternalCompactionTServer tserver = new ExternalCompactionTServer(new ServerOpts(), args)) {
      tserver.runServer();
    }

  }

}
