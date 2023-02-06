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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;

import com.google.common.net.HostAndPort;

public class ServerLockData implements Comparable<ServerLockData> {

  public static class ServerDescriptor {

    /**
     * The group name that will be used when one is not specified.
     */
    public static final String DEFAULT_GROUP_NAME = "default";

    private final UUID uuid;
    private final Service service;
    private final String address;
    private final String group;

    public ServerDescriptor(UUID uuid, Service service, String address, String group) {
      super();
      this.uuid = uuid;
      this.service = service;
      this.address = address;
      this.group = Optional.ofNullable(group).orElse(DEFAULT_GROUP_NAME);
    }

    public UUID getUUID() {
      return uuid;
    }

    public Service getService() {
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
      if (o instanceof ServerDescriptor) {
        return toString().equals(o.toString());
      }
      return false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(uuid).append(SEPARATOR_CHAR).append(service.name()).append(SEPARATOR_CHAR)
          .append(address).append(SEPARATOR_CHAR).append(group);
      return sb.toString();
    }

    public static ServerDescriptor parse(String data) {
      if (data == null) {
        return null;
      }
      String[] parts = data.split(SEPARATOR_CHAR);
      int len = parts.length;
      if (len == 3 || len == 4) {
        UUID uuid = UUID.fromString(parts[0]);
        Service svc = Service.valueOf(parts[1]);
        String addr = parts[2];
        Optional<String> grp = Optional.empty();
        if (len == 4) {
          grp = Optional.ofNullable(parts[3]);
        }
        return new ServerDescriptor(uuid, svc, addr, grp.orElse(null));
      } else {
        throw new IllegalArgumentException("Invalid ServiceDescriptor: " + data);
      }
    }
  }

  public static class ServerDescriptors {
    private final List<ServerDescriptor> descriptors;

    public ServerDescriptors() {
      descriptors = new ArrayList<>();
    }

    public ServerDescriptors(List<ServerDescriptor> descriptors) {
      this.descriptors = descriptors;
    }

    public void addService(ServerDescriptor sd) {
      this.descriptors.add(sd);
    }

    public List<ServerDescriptor> getServices() {
      return descriptors;
    }

    public static ServerDescriptors parse(String data) {
      ServerDescriptors sd = new ServerDescriptors();
      String[] descriptors = data.split(SERVICE_SEPARATOR);
      for (String d : descriptors) {
        ServerDescriptor des = ServerDescriptor.parse(d);
        if (des != null) {
          sd.addService(des);
        }
      }
      return sd;
    }
  }

  public static enum Service {
    TSERV_CLIENT,
    GC_CLIENT,
    COMPACTOR_CLIENT,
    SSERV_CLIENT,
    COORDINATOR_CLIENT,
    MANAGER_CLIENT,
    MONITOR_CLIENT;
  }

  public static final String SERVICE_SEPARATOR = ";";
  public static final String SEPARATOR_CHAR = "=";

  private EnumMap<Service,ServerDescriptor> services;
  private String stringForm = null;

  public ServerLockData(ServerDescriptors sds) {
    this.services = new EnumMap<>(Service.class);
    sds.getServices().forEach(sd -> {
      this.services.put(sd.getService(), sd);
    });
  }

  public ServerLockData(UUID uuid, String address, Service service, String group) {
    this(ServerDescriptors.parse(uuid + SEPARATOR_CHAR + service.name() + SEPARATOR_CHAR + address
        + SEPARATOR_CHAR + group));
  }

  public static ServerLockData parse(String lockData) {
    return new ServerLockData(ServerDescriptors.parse(lockData));
  }

  public String getAddressString(Service service) {
    ServerDescriptor sd = services.get(service);
    if (sd == null) {
      return null;
    }
    return sd.getAddress();
  }

  public HostAndPort getAddress(Service service) {
    return AddressUtil.parseAddress(getAddressString(service), false);
  }

  public String getGroup(Service service) {
    ServerDescriptor sd = services.get(service);
    if (sd == null) {
      return null;
    }
    return sd.getGroup();
  }

  public UUID getServerUUID(Service service) {
    ServerDescriptor sd = services.get(service);
    if (sd == null) {
      return null;
    }
    return sd.getUUID();
  }

  // DON'T CHANGE THIS; WE'RE USING IT FOR SERIALIZATION!!!
  @Override
  public String toString() {
    if (stringForm == null) {
      StringBuilder sb = new StringBuilder();
      String prefix = "";
      for (Entry<Service,ServerDescriptor> e : services.entrySet()) {
        sb.append(prefix).append(services.get(e.getKey()).toString());
        prefix = SERVICE_SEPARATOR;
      }
      stringForm = sb.toString();
    }
    return stringForm;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ServerLockData) {
      return toString().equals(o.toString());
    }
    return false;
  }

  @Override
  public int compareTo(ServerLockData other) {
    return toString().compareTo(other.toString());
  }
}
