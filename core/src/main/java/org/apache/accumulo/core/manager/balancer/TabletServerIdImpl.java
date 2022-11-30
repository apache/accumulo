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
package org.apache.accumulo.core.manager.balancer;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.core.util.HostAndPort;

/**
 * @since 2.1.0
 */
public class TabletServerIdImpl implements TabletServerId {
  private final TServerInstance tServerInstance;

  public static TabletServerIdImpl fromThrift(TServerInstance tsi) {
    return (tsi == null) ? null : new TabletServerIdImpl(tsi);
  }

  public TabletServerIdImpl(String host, int port, String session) {
    requireNonNull(host);
    this.tServerInstance = new TServerInstance(HostAndPort.fromParts(host, port), session);
  }

  public TabletServerIdImpl(TServerInstance tServerInstance) {
    this.tServerInstance = requireNonNull(tServerInstance);
  }

  @Override
  public String getHost() {
    return tServerInstance.getHostAndPort().getHost();
  }

  @Override
  public int getPort() {
    return tServerInstance.getHostAndPort().getPort();
  }

  @Override
  public String getSession() {
    return tServerInstance.getSession();
  }

  @Override
  public int compareTo(TabletServerId o) {
    return tServerInstance.compareTo(((TabletServerIdImpl) o).tServerInstance);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TabletServerIdImpl that = (TabletServerIdImpl) o;
    return tServerInstance.equals(that.tServerInstance);
  }

  @Override
  public int hashCode() {
    return tServerInstance.hashCode();
  }

  @Override
  public String toString() {
    return getHost() + ':' + getPort() + '[' + getSession() + ']';
  }

  public TServerInstance toThrift() {
    return tServerInstance;
  }

  public static TServerInstance toThrift(TabletServerId tabletServerId) {
    if (tabletServerId instanceof TabletServerIdImpl) {
      return ((TabletServerIdImpl) tabletServerId).toThrift();
    } else {
      return new TServerInstance(
          HostAndPort.fromParts(tabletServerId.getHost(), tabletServerId.getPort()),
          tabletServerId.getSession());
    }
  }
}
