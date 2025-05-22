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
package org.apache.accumulo.core.lock;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.util.LazySingletons.GSON;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.accumulo.core.util.AddressUtil;

import com.google.common.net.HostAndPort;

public class ServiceLockData implements Comparable<ServiceLockData> {

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

    private final UUID uuid;
    private final ThriftService service;
    private final String address;
    private final String group;

    public ServiceDescriptor(UUID uuid, ThriftService service, String address, String group) {
      this.uuid = requireNonNull(uuid);
      this.service = requireNonNull(service);
      this.address = requireNonNull(address);
      this.group = requireNonNull(group);
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
      ServiceDescriptor other = (ServiceDescriptor) obj;
      return toString().equals(other.toString());
    }

    @Override
    public String toString() {
      return serialize();
    }

    private String serialize() {
      return GSON.get().toJson(new ServiceDescriptorGson(uuid, service, address, group));
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

    public ServiceDescriptors(Set<ServiceDescriptor> descriptors) {
      this.descriptors = descriptors;
    }

    public void addService(ServiceDescriptor sd) {
      this.descriptors.add(sd);
    }

    public Set<ServiceDescriptor> getServices() {
      return descriptors;
    }
  }

  private final EnumMap<ThriftService,ServiceDescriptor> services;

  public ServiceLockData(ServiceDescriptors sds) {
    this.services = new EnumMap<>(ThriftService.class);
    sds.getServices().forEach(sd -> this.services.put(sd.getService(), sd));
  }

  public ServiceLockData(UUID uuid, String address, ThriftService service, String group) {
    this(new ServiceDescriptors(new HashSet<>(
        Collections.singleton(new ServiceDescriptor(uuid, service, address, group)))));
  }

  public String getAddressString(ThriftService service) {
    ServiceDescriptor sd = services.get(service);
    return sd == null ? null : sd.getAddress();
  }

  public HostAndPort getAddress(ThriftService service) {
    String s = getAddressString(service);
    return s == null ? null : AddressUtil.parseAddress(s);
  }

  public String getGroup(ThriftService service) {
    ServiceDescriptor sd = services.get(service);
    return sd == null ? null : sd.getGroup();
  }

  public UUID getServerUUID(ThriftService service) {
    ServiceDescriptor sd = services.get(service);
    return sd == null ? null : sd.getUUID();
  }

  public byte[] serialize() {
    ServiceDescriptorsGson json = new ServiceDescriptorsGson();
    json.descriptors = services.values().stream()
        .map(s -> new ServiceDescriptorGson(s.uuid, s.service, s.address, s.group))
        .collect(Collectors.toSet());
    return GSON.get().toJson(json).getBytes(UTF_8);
  }

  @Override
  public String toString() {
    return new String(serialize(), UTF_8);
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof ServiceLockData ? Objects.equals(toString(), o.toString()) : false;
  }

  @Override
  public int compareTo(ServiceLockData other) {
    return toString().compareTo(other.toString());
  }

  public static Optional<ServiceLockData> parse(byte[] lockData) {
    if (lockData == null) {
      return Optional.empty();
    }
    String data = new String(lockData, UTF_8);
    return data.isBlank() ? Optional.empty()
        : Optional.of(new ServiceLockData(parseServiceDescriptors(data)));
  }

  public static ServiceDescriptors parseServiceDescriptors(String data) {
    return deserialize(GSON.get().fromJson(data, ServiceDescriptorsGson.class));
  }

  private static ServiceDescriptors deserialize(ServiceDescriptorsGson json) {
    return new ServiceDescriptors(json.descriptors.stream()
        .map(s -> new ServiceDescriptor(s.uuid, s.service, s.address, s.group))
        .collect(Collectors.toSet()));
  }

  private static class ServiceDescriptorGson {
    private UUID uuid;
    private ThriftService service;
    private String address;
    private String group;

    // default constructor required for Gson
    @SuppressWarnings("unused")
    public ServiceDescriptorGson() {}

    public ServiceDescriptorGson(UUID uuid, ThriftService service, String address, String group) {
      this.uuid = uuid;
      this.service = service;
      this.address = address;
      this.group = group;
    }
  }

  private static class ServiceDescriptorsGson {
    private Set<ServiceDescriptorGson> descriptors;
  }
}
