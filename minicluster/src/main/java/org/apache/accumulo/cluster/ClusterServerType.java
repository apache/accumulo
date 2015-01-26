/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.cluster;

import org.apache.accumulo.minicluster.ServerType;

/**
 * {@link ServerType} is in the public API. This lets us work around that annoyance.
 */
public enum ClusterServerType {
  MASTER("Master"), ZOOKEEPER("ZooKeeper"), TABLET_SERVER("TServer"), GARBAGE_COLLECTOR("GC"), TRACER("Tracer"), MONITOR("Monitor");

  private final String prettyPrint;

  public String prettyPrint() {
    return prettyPrint;
  }

  ClusterServerType(String prettyPrint) {
    this.prettyPrint = prettyPrint;
  }

  public static ClusterServerType get(ServerType type) {
    switch (type) {
      case MASTER:
        return MASTER;
      case ZOOKEEPER:
        return ZOOKEEPER;
      case TABLET_SERVER:
        return TABLET_SERVER;
      case GARBAGE_COLLECTOR:
        return GARBAGE_COLLECTOR;
    }
    throw new IllegalArgumentException("Unknown server type");
  }

  public ServerType toServerType() {
    switch (this) {
      case MASTER:
        return ServerType.MASTER;
      case ZOOKEEPER:
        return ServerType.ZOOKEEPER;
      case TABLET_SERVER:
        return ServerType.TABLET_SERVER;
      case GARBAGE_COLLECTOR:
        return ServerType.GARBAGE_COLLECTOR;
      default:
        return null;
    }
  }
}
