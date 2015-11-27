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
package org.apache.accumulo.core.client.impl;

import java.util.Objects;

import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.SslConnectionParams;

import com.google.common.annotations.VisibleForTesting;

@VisibleForTesting
public class ThriftTransportKey {
  private final String location;
  private final int port;
  private final long timeout;
  private final SslConnectionParams sslParams;
  private final boolean oneway;

  private int hash = -1;

  @VisibleForTesting
  public ThriftTransportKey(String location, long timeout, SslConnectionParams sslParams, boolean oneway) {
    ArgumentChecker.notNull(location);
    String[] locationAndPort = location.split(":", 2);
    if (locationAndPort.length == 2) {
      this.location = locationAndPort[0];
      this.port = Integer.parseInt(locationAndPort[1]);
    } else
      throw new IllegalArgumentException("Location was expected to contain port but did not. location=" + location);

    this.timeout = timeout;
    this.sslParams = sslParams;
    this.oneway = oneway;
  }

  String getLocation() {
    return location;
  }

  int getPort() {
    return port;
  }

  long getTimeout() {
    return timeout;
  }

  public boolean isSsl() {
    return sslParams != null;
  }

  public boolean isOneway() {
    return oneway;
  }

  /**
   * Compute whether another <code>ThriftTransportKey</code> references a connection to the same host which <code>this</code>
   * key also references.
   *
   * @param other The other <code>ThriftTransportKey</code>.
   * @return True if <code>other</code> points to the same server as <code>this</code>.
   */
  public boolean isSameLocation(ThriftTransportKey other) {
    return location.equals(Objects.requireNonNull(other).location) && port == other.port;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ThriftTransportKey))
      return false;
    ThriftTransportKey ttk = (ThriftTransportKey) o;
    return location.equals(ttk.location) && port == ttk.port && timeout == ttk.timeout && (!isSsl() || (ttk.isSsl() && sslParams.equals(ttk.sslParams))) && oneway == ttk.isOneway();
  }

  @Override
  public int hashCode() {
    if (hash == -1)
      hash = toString().hashCode();
    return hash;
  }

  @Override
  public String toString() {
    return (isSsl() ? "ssl:" : "") + location + ":" + Integer.toString(port) + " (" + Long.toString(timeout) + ") oneway:" + oneway;
  }

  public SslConnectionParams getSslParams() {
    return sslParams;
  }
}
