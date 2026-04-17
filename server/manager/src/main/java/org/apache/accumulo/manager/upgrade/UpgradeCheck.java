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
package org.apache.accumulo.manager.upgrade;

import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;

/**
 * Checks persistent data to see if upgrade is complete and remembers if it is.
 */
public class UpgradeCheck {

  private final ServerContext context;

  private boolean upgradeComplete = false;

  public UpgradeCheck(ServerContext context) {
    this.context = context;
  }

  // Checks in persistent storage if upgrade is complete. Once it sees its complete remembers this
  // and stops checking.
  public synchronized boolean isUpgradeComplete() {
    if (!upgradeComplete) {
      upgradeComplete = AccumuloDataVersion.getCurrentVersion(context) >= AccumuloDataVersion.get();
    }

    return upgradeComplete;
  }
}
