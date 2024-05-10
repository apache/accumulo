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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.time.SteadyTime;

import com.google.common.net.HostAndPort;

/**
 * For a suspended tablet, the time of suspension and the server it was suspended from.
 */
public class SuspendingTServer {
  public final HostAndPort server;
  public final SteadyTime suspensionTime;

  SuspendingTServer(HostAndPort server, SteadyTime suspensionTime) {
    this.server = Objects.requireNonNull(server);
    this.suspensionTime = Objects.requireNonNull(suspensionTime);
  }

  public static SuspendingTServer fromValue(Value value) {
    String valStr = value.toString();
    String[] parts = valStr.split("[|]", 2);
    return new SuspendingTServer(HostAndPort.fromString(parts[0]),
        SteadyTime.from(Long.parseLong(parts[1]), TimeUnit.MILLISECONDS));
  }

  public static Value toValue(TServerInstance tServer, SteadyTime suspensionTime) {
    return new Value(tServer.getHostPort() + "|" + suspensionTime.getMillis());
  }

  @Override
  public boolean equals(Object rhsObject) {
    if (!(rhsObject instanceof SuspendingTServer)) {
      return false;
    }
    SuspendingTServer rhs = (SuspendingTServer) rhsObject;
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
