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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.io.Text;

/**
 * A tablet is assigned to a tablet server at the given address as long as it is alive and well.
 * When the tablet server is restarted, the instance information it advertises will change.
 * Therefore tablet assignments can be considered out-of-date if the tablet server instance
 * information has been changed.
 */
public class TServerInstance implements Comparable<TServerInstance> {

  private final HostAndPort hostAndPort;
  private final String hostPort;
  private final String session;
  private final String hostPortSession;

  public TServerInstance(HostAndPort address, String session) {
    this.hostAndPort = address;
    this.session = session;
    this.hostPort = hostAndPort.toString();
    this.hostPortSession = hostPort + "[" + session + "]";
  }

  public TServerInstance(String formattedString) {
    int pos = formattedString.indexOf("[");
    if (pos < 0 || !formattedString.endsWith("]")) {
      throw new IllegalArgumentException(formattedString);
    }
    this.hostAndPort = HostAndPort.fromString(formattedString.substring(0, pos));
    this.session = formattedString.substring(pos + 1, formattedString.length() - 1);
    this.hostPort = hostAndPort.toString();
    this.hostPortSession = hostPort + "[" + session + "]";
  }

  public TServerInstance(HostAndPort address, long session) {
    this(address, Long.toHexString(session));
  }

  public TServerInstance(String address, long session) {
    this(AddressUtil.parseAddress(address, false), Long.toHexString(session));
  }

  public TServerInstance(Value address, Text session) {
    this(AddressUtil.parseAddress(new String(address.get(), UTF_8), false), session.toString());
  }

  @Override
  public int compareTo(TServerInstance other) {
    if (this == other) {
      return 0;
    }
    return this.getHostPortSession().compareTo(other.getHostPortSession());
  }

  @Override
  public int hashCode() {
    return getHostPortSession().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TServerInstance) {
      return compareTo((TServerInstance) obj) == 0;
    }
    return false;
  }

  @Override
  public String toString() {
    return hostPortSession;
  }

  public String getHostPortSession() {
    return hostPortSession;
  }

  public String getHost() {
    return hostAndPort.getHost();
  }

  public String getHostPort() {
    return hostPort;
  }

  public HostAndPort getHostAndPort() {
    return hostAndPort;
  }

  public String getSession() {
    return session;
  }
}
