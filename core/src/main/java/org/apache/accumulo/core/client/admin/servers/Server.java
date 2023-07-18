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
package org.apache.accumulo.core.client.admin.servers;

import java.util.Comparator;
import java.util.Objects;

import com.google.common.base.Preconditions;

public abstract class Server implements Comparator<Server>, Comparable<Server> {

  private final ServerType type;
  private final String host;
  private final int port;

  protected Server(ServerType type, String host, int port) {
    super();
    Preconditions.checkArgument(port > 0);
    this.type = Objects.requireNonNull(type);
    this.host = Objects.requireNonNull(host);
    this.port = port;
  }

  public ServerType getType() {
    return type;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public int compare(Server first, Server second) {
    if (first == second) {
      return 0;
    }
    if (first != null && second == null) {
      return 1;
    }
    if (first == null && second != null) {
      return -1;
    }
    int result = first.getHost().compareTo(second.getHost());
    if (result == 0) {
      result = Integer.compare(first.getPort(), second.getPort());
      if (result == 0) {
        result = first.getType().compareTo(second.getType());
      }
    }
    return result;
  }

  @Override
  public int compareTo(Server other) {
    return compare(this, other);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((host == null) ? 0 : host.hashCode());
    result = prime * result + port;
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (getClass() != other.getClass()) {
      return false;
    }
    return 0 == compareTo((Server) other);
  }

  @Override
  public String toString() {
    return "Server [type=" + type + ", host=" + host + ", port=" + port + "]";
  }

  public String toHostPortString() {
    return host + ":" + port;
  }
}
