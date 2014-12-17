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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.accumulo.core.rpc.SslConnectionParams;

import com.google.common.net.HostAndPort;

class ThriftTransportKey {
  private final HostAndPort server;
  private final long timeout;
  private final SslConnectionParams sslParams;

  private int hash = -1;

  ThriftTransportKey(HostAndPort server, long timeout, ClientContext context) {
    checkNotNull(server, "location is null");
    this.server = server;
    this.timeout = timeout;
    this.sslParams = context.getClientSslParams();
  }

  HostAndPort getServer() {
    return server;
  }

  long getTimeout() {
    return timeout;
  }

  public boolean isSsl() {
    return sslParams != null;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ThriftTransportKey))
      return false;
    ThriftTransportKey ttk = (ThriftTransportKey) o;
    return server.equals(ttk.server) && timeout == ttk.timeout && (!isSsl() || (ttk.isSsl() && sslParams.equals(ttk.sslParams)));
  }

  @Override
  public int hashCode() {
    if (hash == -1)
      hash = toString().hashCode();
    return hash;
  }

  @Override
  public String toString() {
    return (isSsl() ? "ssl:" : "") + server + " (" + Long.toString(timeout) + ")";
  }

  public SslConnectionParams getSslParams() {
    return sslParams;
  }
}
