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

import java.util.Objects;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.hadoop.io.Text;

import com.google.common.net.HostAndPort;

/**
 * A tablet is assigned to a tablet server at the given address as long as it is alive and well.
 * When the tablet server is restarted, the instance information it advertises will change.
 * Therefore tablet assignments can be considered out-of-date if the tablet server instance
 * information has been changed.
 */
public class TServerInstance implements Comparable<TServerInstance> {

  private static final String FORMAT = "%s#%s#%s";

  private final HostAndPort hostAndPort;
  private final String hostPort;
  private final String session;
  private final String hostPortSessionGroup;
  private final String group;

  public static TServerInstance fromString(String val) {
    Objects.requireNonNull(val, "String value for TServerInstance cannot be null");
    String[] parts = val.split("#");
    if (parts.length != 3) {
      // could throw IllegalArgumentException, but IllegalStateException is something
      // that is already expected in a large portion of the codebase.
      throw new IllegalStateException(
          "Supplied TServerInstance string: " + val + " does not follow format: " + FORMAT);
    }
    return new TServerInstance(parts[0], parts[1], parts[2]);
  }

  public TServerInstance(HostAndPort address, String session, String group) {
    this.hostAndPort = address;
    this.session = session;
    this.hostPort = hostAndPort.toString();
    this.group = group;
    this.hostPortSessionGroup = String.format(FORMAT, hostPort, session, group);
  }

  public TServerInstance(String address, String session, String group) {
    this(AddressUtil.parseAddress(address, false), session, group);
  }

  public TServerInstance(HostAndPort address, long session, String group) {
    this(address, Long.toHexString(session), group);
  }

  public TServerInstance(String address, long session, String group) {
    this(AddressUtil.parseAddress(address, false), Long.toHexString(session), group);
  }

  public TServerInstance(Value address, Text session, String group) {
    this(AddressUtil.parseAddress(new String(address.get(), UTF_8), false), session.toString(),
        group);
  }

  @Override
  public int compareTo(TServerInstance other) {
    if (this == other) {
      return 0;
    }
    return this.getHostPortSessionGroup().compareTo(other.getHostPortSessionGroup());
  }

  @Override
  public int hashCode() {
    return this.hostPortSessionGroup.hashCode();
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
    return hostPortSessionGroup;
  }

  public String getHostPortSessionGroup() {
    return hostPortSessionGroup;
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

  public String getGroup() {
    return group;
  }
}
