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
package org.apache.accumulo.core.util;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.google.common.net.HostAndPort;
import com.google.gson.Gson;

public class ServiceLockData implements Comparable<ServiceLockData> {

  private static final Gson gson = new Gson();

  /**
   * Thrift Service list
   */
  public static enum ThriftService {
    CLIENT,
    COORDINATOR,
    COMPACTOR,
    FATE,
    GC,
    MANAGER,
    NONE,
    TABLET_INGEST,
    TABLET_MANAGEMENT,
    TABLET_SCAN,
    TSERV
  }

  /**
   * An object that describes a process, the group assigned to that process, the Thrift service and
   * the address to use to communicate with that service.
   */
  public static class ServiceDescriptor {

    /**
     * The group name that will be used when one is not specified.
     */
    public static final String DEFAULT_GROUP_NAME = "default";

    private final UUID uuid;
    private final ThriftService service;
    private final String address;
    private final String group;

    public ServiceDescriptor(UUID uuid, ThriftService service, String address, String group) {
      super();
      this.uuid = uuid;
      this.service = service;
      this.address = address;
      this.group = Optional.ofNullable(group).orElse(DEFAULT_GROUP_NAME);
    }

    public UUID getUUID() {
      return uuid;
    }

    public ThriftService getService() {
      return service;
    }

    public String getAddress() {
      return address;
    }

    public String getGroup() {
      return group;
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof ServiceDescriptor) {
        return toString().equals(o.toString());
      }
      return false;
    }

    @Override
    public String toString() {
      return gson.toJson(this);
    }

  }

  /**
   * A set of ServiceDescriptor's
   */
  public static class ServiceDescriptors {
    private final Set<ServiceDescriptor> descriptors;

    public ServiceDescriptors() {
      descriptors = new HashSet<>();
    }

    public ServiceDescriptors(HashSet<ServiceDescriptor> descriptors) {
      this.descriptors = descriptors;
    }

    public void addService(ServiceDescriptor sd) {
      this.descriptors.add(sd);
    }

    public Set<ServiceDescriptor> getServices() {
      return descriptors;
    }

    @Override
    public String toString() {
      return gson.toJson(this);
    }

    public static ServiceDescriptors parse(String data) {
      return gson.fromJson(data, ServiceDescriptors.class);
    }
  }

  private EnumMap<ThriftService,ServiceDescriptor> services;

  public ServiceLockData(ServiceDescriptors sds) {
    this.services = new EnumMap<>(ThriftService.class);
    sds.getServices().forEach(sd -> {
      this.services.put(sd.getService(), sd);
    });
  }

  public ServiceLockData(UUID uuid, String address, ThriftService service, String group) {
    this(new ServiceDescriptors(new HashSet<>(
        Collections.singleton(new ServiceDescriptor(uuid, service, address, group)))));
  }

  public static ServiceLockData parse(String lockData) {
    return new ServiceLockData(ServiceDescriptors.parse(lockData));
  }

  public String getAddressString(ThriftService service) {
    ServiceDescriptor sd = services.get(service);
    if (sd == null) {
      return null;
    }
    return sd.getAddress();
  }

  public HostAndPort getAddress(ThriftService service) {
    return AddressUtil.parseAddress(getAddressString(service), false);
  }

  public String getGroup(ThriftService service) {
    ServiceDescriptor sd = services.get(service);
    if (sd == null) {
      return null;
    }
    return sd.getGroup();
  }

  public UUID getServerUUID(ThriftService service) {
    ServiceDescriptor sd = services.get(service);
    if (sd == null) {
      return null;
    }
    return sd.getUUID();
  }

  public String serialize() {
    ServiceDescriptors sd = new ServiceDescriptors();
    services.values().forEach(s -> sd.addService(s));
    return gson.toJson(sd);
  }

  public static ServiceLockData deserialize(String data) {
    ServiceDescriptors sd = gson.fromJson(data, ServiceDescriptors.class);
    return new ServiceLockData(sd);
  }

  @Override
  public String toString() {
    return serialize();
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ServiceLockData) {
      return toString().equals(o.toString());
    }
    return false;
  }

  @Override
  public int compareTo(ServiceLockData other) {
    return toString().compareTo(other.toString());
  }
}
