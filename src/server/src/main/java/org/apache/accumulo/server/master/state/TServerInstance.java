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
/**
 * 
 */
package org.apache.accumulo.server.master.state;

import java.io.Serializable;
import java.net.InetSocketAddress;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.server.util.AddressUtil;
import org.apache.hadoop.io.Text;

/**
 * A tablet is assigned to a tablet server at the given address as long as it is alive and well. When the tablet server is restarted, the instance information
 * it advertises will change. Therefore tablet assignments can be considered out-of-date if the tablet server instance information has been changed.
 * 
 */
public class TServerInstance implements Comparable<TServerInstance>, Serializable {
  
  private static final long serialVersionUID = 1L;
  
  private InetSocketAddress location;
  private String session;
  private String cachedStringRepresentation;
  
  public TServerInstance(InetSocketAddress address, String session) {
    this.location = address;
    this.session = session;
    this.cachedStringRepresentation = hostPort() + "[" + session + "]";
  }
  
  public TServerInstance(InetSocketAddress address, long session) {
    this(address, Long.toHexString(session));
  }
  
  public TServerInstance(String address, long session) {
    this(AddressUtil.parseAddress(address, Property.TSERV_CLIENTPORT), Long.toHexString(session));
  }
  
  public TServerInstance(Value address, Text session) {
    this(AddressUtil.parseAddress(new String(address.get()), Property.TSERV_CLIENTPORT), session.toString());
  }
  
  public void putLocation(Mutation m) {
    m.put(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY, asColumnQualifier(), asMutationValue());
  }
  
  public void putFutureLocation(Mutation m) {
    m.put(Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY, asColumnQualifier(), asMutationValue());
  }
  
  public void putLastLocation(Mutation m) {
    m.put(Constants.METADATA_LAST_LOCATION_COLUMN_FAMILY, asColumnQualifier(), asMutationValue());
  }
  
  public void clearLastLocation(Mutation m) {
    m.putDelete(Constants.METADATA_LAST_LOCATION_COLUMN_FAMILY, asColumnQualifier());
  }
  
  @Override
  public int compareTo(TServerInstance other) {
    if (this == other)
      return 0;
    return this.toString().compareTo(other.toString());
  }
  
  @Override
  public int hashCode() {
    return toString().hashCode();
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
    return cachedStringRepresentation;
  }
  
  public int port() {
    return getLocation().getPort();
  }
  
  public String host() {
    return getLocation().getAddress().getHostAddress();
  }
  
  public String hostPort() {
    return org.apache.accumulo.core.util.AddressUtil.toString(getLocation());
  }
  
  public Text asColumnQualifier() {
    return new Text(this.getSession());
  }
  
  public Value asMutationValue() {
    return new Value(org.apache.accumulo.core.util.AddressUtil.toString(getLocation()).getBytes());
  }
  
  public InetSocketAddress getLocation() {
    return location;
  }
  
  public String getSession() {
    return session;
  }
}
