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
package org.apache.accumulo.server.master.state;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.hadoop.io.Text;

import com.google.common.net.HostAndPort;

/**
 * A tablet is assigned to a tablet server at the given address as long as it is alive and well. When the tablet server is restarted, the instance information
 * it advertises will change. Therefore tablet assignments can be considered out-of-date if the tablet server instance information has been changed.
 *
 */
public class TServerInstance implements Comparable<TServerInstance>, Serializable {

  private static final long serialVersionUID = 1L;

  // HostAndPort is not Serializable
  private transient HostAndPort location;
  private String session;
  private String cachedStringRepresentation;

  public TServerInstance(HostAndPort address, String session) {
    this.location = address;
    this.session = session;
    this.cachedStringRepresentation = hostPort() + "[" + session + "]";
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

  public void putLocation(Mutation m) {
    m.put(TabletsSection.CurrentLocationColumnFamily.NAME, asColumnQualifier(), asMutationValue());
  }

  public void putFutureLocation(Mutation m) {
    m.put(TabletsSection.FutureLocationColumnFamily.NAME, asColumnQualifier(), asMutationValue());
  }

  public void putLastLocation(Mutation m) {
    m.put(TabletsSection.LastLocationColumnFamily.NAME, asColumnQualifier(), asMutationValue());
  }

  public void clearLastLocation(Mutation m) {
    m.putDelete(TabletsSection.LastLocationColumnFamily.NAME, asColumnQualifier());
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
    return getLocation().getHostText();
  }

  public String hostPort() {
    return getLocation().toString();
  }

  public Text asColumnQualifier() {
    return new Text(this.getSession());
  }

  public Value asMutationValue() {
    return new Value(getLocation().toString().getBytes(UTF_8));
  }

  public HostAndPort getLocation() {
    return location;
  }

  public String getSession() {
    return session;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    out.writeObject(location.toString());
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    location = HostAndPort.fromString(in.readObject().toString());
  }
}
