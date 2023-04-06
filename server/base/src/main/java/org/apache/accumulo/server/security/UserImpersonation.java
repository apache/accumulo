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
package org.apache.accumulo.server.security;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;

/**
 * When SASL is enabled, this parses properties from the site configuration to build up a set of all
 * users capable of impersonating another user, the users which may be impersonated and the hosts in
 * which the impersonator may issue requests from.
 *
 * <pre>
 * <code>
 * INSTANCE_RPC_SASL_PROXYUSERS=rpc_user={allowed_accumulo_users=[...], allowed_client_hosts=[...]
 * INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION=rpc_user:user,user,user;...
 * INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION=host,host:host...
 * </code>
 * </pre>
 *
 * @see Property#INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION
 * @see Property#INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION
 */
public class UserImpersonation {

  private static final Set<String> ALWAYS_TRUE = new AlwaysTrueSet<>();
  private static final String ALL = "*";

  public static class AlwaysTrueSet<T> implements Set<T> {

    @Override
    public int size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(Object o) {
      return true;
    }

    @Override
    public Iterator<T> iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <E> E[] toArray(E[] a) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(T e) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      return true;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }

  public static class UsersWithHosts {
    private Set<String> users = new HashSet<>(), hosts = new HashSet<>();
    private boolean allUsers, allHosts;

    public UsersWithHosts() {
      allUsers = allHosts = false;
    }

    public Set<String> getUsers() {
      if (allUsers) {
        return ALWAYS_TRUE;
      }
      return users;
    }

    public Set<String> getHosts() {
      if (allHosts) {
        return ALWAYS_TRUE;
      }
      return hosts;
    }

    public boolean acceptsAllUsers() {
      return allUsers;
    }

    public void setAcceptAllUsers(boolean allUsers) {
      this.allUsers = allUsers;
    }

    public boolean acceptsAllHosts() {
      return allHosts;
    }

    public void setAcceptAllHosts(boolean allHosts) {
      this.allHosts = allHosts;
    }

    public void setUsers(Set<String> users) {
      this.users = users;
      allUsers = false;
    }

    public void setHosts(Set<String> hosts) {
      this.hosts = hosts;
      allHosts = false;
    }
  }

  private final Map<String,UsersWithHosts> proxyUsers;

  public UserImpersonation(AccumuloConfiguration conf) {
    proxyUsers = new HashMap<>();

    final String userConfigString = conf.get(Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION);
    if (Objects.equals(userConfigString,
        Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION.getDefaultValue())) {
      // impersonation is not configured
      return;
    }

    final String hostConfigString = conf.get(Property.INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION);
    // Pull out the config values, defaulting to at least one value
    final String[] userConfigs =
        userConfigString.trim().isEmpty() ? new String[] {""} : userConfigString.split(";");
    final String[] hostConfigs =
        hostConfigString.trim().isEmpty() ? new String[] {""} : hostConfigString.split(";");

    if (userConfigs.length != hostConfigs.length) {
      String msg = String.format(
          "Should have equal number of user and host"
              + " impersonation elements in configuration. Got %d and %d elements, respectively.",
          userConfigs.length, hostConfigs.length);
      throw new IllegalArgumentException(msg);
    }

    for (int i = 0; i < userConfigs.length; i++) {
      final String userConfig = userConfigs[i];
      final String hostConfig = hostConfigs[i];

      final String[] splitUserConfig = userConfig.split(":");
      if (splitUserConfig.length != 2) {
        throw new IllegalArgumentException(
            "Expect a single colon-separated pair, but found '" + userConfig + "'");
      }

      final String remoteUser = splitUserConfig[0];
      final String allowedImpersonationsForRemoteUser = splitUserConfig[1];
      final UsersWithHosts usersWithHosts = new UsersWithHosts();

      proxyUsers.put(remoteUser.trim(), usersWithHosts);

      if (ALL.equals(allowedImpersonationsForRemoteUser)) {
        usersWithHosts.setAcceptAllUsers(true);
      } else {
        String[] allowedUsers = allowedImpersonationsForRemoteUser.split(",");
        Set<String> usersSet = new HashSet<>();
        usersSet.addAll(Arrays.asList(allowedUsers));
        usersWithHosts.setUsers(usersSet);
      }

      if (ALL.equals(hostConfig)) {
        usersWithHosts.setAcceptAllHosts(true);
      } else {
        String[] allowedHosts = hostConfig.split(",");
        Set<String> hostsSet = new HashSet<>();
        hostsSet.addAll(Arrays.asList(allowedHosts));
        usersWithHosts.setHosts(hostsSet);
      }
    }
  }

  public UsersWithHosts get(String remoteUser) {
    return proxyUsers.get(remoteUser);
  }

}
