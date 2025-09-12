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
package org.apache.accumulo.core.metadata;

import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.TServerInstance.TServerInstanceInfo;
import org.apache.accumulo.core.util.time.SteadyTime;

/**
 * For a suspended tablet, the time of suspension and the server it was suspended from.
 */
public class SuspendingTServer implements Serializable {

  public static record SuspendingTServerInfo(TServerInstanceInfo tsi, long millis) {
    public SuspendingTServer getSTS() {
      return new SuspendingTServer(tsi.getTSI(), SteadyTime.from(Duration.ofMillis(millis)));
    }
  }

  private static final long serialVersionUID = 1L;

  public final TServerInstance server;
  public final SteadyTime suspensionTime;

  public SuspendingTServer(TServerInstance server, SteadyTime suspensionTime) {
    this.server = Objects.requireNonNull(server);
    this.suspensionTime = Objects.requireNonNull(suspensionTime);
  }

  public static SuspendingTServer fromValue(Value value) {
    return GSON.get().fromJson(value.toString(), SuspendingTServerInfo.class).getSTS();
  }

  private SuspendingTServerInfo toSuspendingTServerInfo() {
    return new SuspendingTServerInfo(server.getTServerInstanceInfo(), suspensionTime.getMillis());
  }

  public Value toValue() {
    return new Value(GSON.get().toJson(toSuspendingTServerInfo()));
  }

  @Override
  public boolean equals(Object rhsObject) {
    if (!(rhsObject instanceof SuspendingTServer rhs)) {
      return false;
    }
    return server.equals(rhs.server) && suspensionTime.equals(rhs.suspensionTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(server, suspensionTime);
  }

  @Override
  public String toString() {
    return server + "[" + suspensionTime + "]";
  }
}
