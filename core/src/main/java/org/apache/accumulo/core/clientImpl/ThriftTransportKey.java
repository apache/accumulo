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
package org.apache.accumulo.core.clientImpl;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.accumulo.core.rpc.SaslConnectionParams;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.util.HostAndPort;

import com.google.common.annotations.VisibleForTesting;

@VisibleForTesting
public class ThriftTransportKey {
  private final HostAndPort server;
  private final long timeout;
  private final SslConnectionParams sslParams;
  private final SaslConnectionParams saslParams;

  private int hash = -1;

  @VisibleForTesting
  public ThriftTransportKey(HostAndPort server, long timeout, ClientContext context) {
    requireNonNull(server, "location is null");
    this.server = server;
    this.timeout = timeout;
    this.sslParams = context.getClientSslParams();
    this.saslParams = context.getSaslParams();
    if (saslParams != null) {
      // TSasl and TSSL transport factories don't play nicely together
      if (sslParams != null) {
        throw new RuntimeException("Cannot use both SSL and SASL thrift transports");
      }
    }
  }

  /**
   * Visible only for testing
   */
  ThriftTransportKey(HostAndPort server, long timeout, SslConnectionParams sslParams,
      SaslConnectionParams saslParams) {
    requireNonNull(server, "location is null");
    this.server = server;
    this.timeout = timeout;
    this.sslParams = sslParams;
    this.saslParams = saslParams;
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

  public boolean isSasl() {
    return saslParams != null;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ThriftTransportKey))
      return false;
    ThriftTransportKey ttk = (ThriftTransportKey) o;
    return server.equals(ttk.server) && timeout == ttk.timeout
        && (!isSsl() || (ttk.isSsl() && sslParams.equals(ttk.sslParams)))
        && (!isSasl() || (ttk.isSasl() && saslParams.equals(ttk.saslParams)));
  }

  public final void precomputeHashCode() {
    hashCode();
  }

  @Override
  public int hashCode() {
    if (hash == -1)
      hash = Objects.hash(server, timeout, sslParams, saslParams);
    return hash;
  }

  @Override
  public String toString() {
    String prefix = "";
    if (isSsl()) {
      prefix = "ssl:";
    } else if (isSasl()) {
      prefix = saslParams + ":";
    }
    return prefix + server + " (" + Long.toString(timeout) + ")";
  }

  public SslConnectionParams getSslParams() {
    return sslParams;
  }

  public SaslConnectionParams getSaslParams() {
    return saslParams;
  }
}
