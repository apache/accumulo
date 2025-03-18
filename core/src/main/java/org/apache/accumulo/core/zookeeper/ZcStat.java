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
package org.apache.accumulo.core.zookeeper;

import org.apache.zookeeper.data.Stat;

import com.google.common.annotations.VisibleForTesting;

public class ZcStat {
  private long ephemeralOwner;
  private long mzxid;

  public ZcStat() {}

  public ZcStat(Stat stat) {
    this.ephemeralOwner = stat.getEphemeralOwner();
    this.mzxid = stat.getMzxid();
  }

  public long getEphemeralOwner() {
    return ephemeralOwner;
  }

  public void set(ZcStat cachedStat) {
    this.ephemeralOwner = cachedStat.ephemeralOwner;
    this.mzxid = cachedStat.mzxid;
  }

  @VisibleForTesting
  public void setEphemeralOwner(long ephemeralOwner) {
    this.ephemeralOwner = ephemeralOwner;
  }

  public long getMzxid() {
    return mzxid;
  }
}
