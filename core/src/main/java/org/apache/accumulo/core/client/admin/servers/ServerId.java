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

import org.apache.accumulo.core.conf.PropertyType.PortRange;

import com.google.common.base.Preconditions;

/**
 * Object representing the type, resource group, and address of a server process.
 *
 * @since 4.0.0
 */
public class ServerId implements Comparable<ServerId> {

  /**
   * Server process type names.
   *
   * @since 4.0.0
   */
  public enum Type {
    MANAGER, MONITOR, GARBAGE_COLLECTOR, COMPACTOR, SCAN_SERVER, TABLET_SERVER;
  }

  private final Type type;
  private final String resourceGroup;
  private final String host;
  private final int port;

  public ServerId(Type type, String resourceGroup, String host, int port) {
    super();
    Preconditions.checkArgument(port == 0 || PortRange.VALID_RANGE.contains(port),
        "invalid server port value: " + port);
    this.type = Objects.requireNonNull(type);
    this.resourceGroup = Objects.requireNonNull(resourceGroup);
    this.host = Objects.requireNonNull(host);
    this.port = port;
  }

  public Type getType() {
    return type;
  }

  public String getResourceGroup() {
    return this.resourceGroup;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public int compareTo(ServerId other) {
    if (this == other) {
      return 0;
    }
    int result = this.getType().compareTo(other.getType());
    if (result == 0) {
      result = this.getResourceGroup().compareTo(other.getResourceGroup());
      if (result == 0) {
        result = this.getHost().compareTo(other.getHost());
        if (result == 0) {
          result = Integer.compare(this.getPort(), other.getPort());
        }
      }
    }
    return result;
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port, type, resourceGroup);
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
    ServerId other = (ServerId) obj;
    return 0 == compareTo(other);
  }

  @Override
  public String toString() {
    return "Server [type= " + type + ", resource group= " + resourceGroup + ", host= " + host
        + ", port= " + port + "]";
  }

  public String toHostPortString() {
    return host + ":" + port;
  }
}
