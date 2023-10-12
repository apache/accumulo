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

import java.util.Objects;

import com.google.common.base.Preconditions;

public abstract class ServerId<T extends ServerId<?>> implements Comparable<T> {

  private final ServerType type;
  private final String host;
  private final int port;

  protected ServerId(ServerType type, String host, int port) {
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
  public int compareTo(T other) {
    if (this == other) {
      return 0;
    }
    int result = this.getHost().compareTo(other.getHost());
    if (result == 0) {
      result = Integer.compare(this.getPort(), other.getPort());
      if (result == 0) {
        result = this.getType().compareTo(other.getType());
      }
    }
    return result;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + host.hashCode();
    result = prime * result + port;
    result = prime * result + type.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    @SuppressWarnings("unchecked")
    T other = (T) obj;
    return 0 == compareTo(other);
  }

  @Override
  public String toString() {
    return "Server [type=" + type + ", host=" + host + ", port=" + port + "]";
  }

  public String toHostPortString() {
    return host + ":" + port;
  }
}
