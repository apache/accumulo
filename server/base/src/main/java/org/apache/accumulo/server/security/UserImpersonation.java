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
package org.apache.accumulo.server.security;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * When SASL is enabled, this parses properties from the site configuration to build up a set of all users capable of impersonating another user, the users
 * which may be impersonated and the hosts in which the impersonator may issue requests from.
 *
 * <code>INSTANCE_RPC_SASL_PROXYUSERS=rpc_user={allowed_accumulo_users=[...], allowed_client_hosts=[...]</code>
 * <code>INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION=rpc_user:user,user,user;...</code>
 * <code>INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION=host,host:host...</code>
 *
 * @see Property#INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION
 * @see Property#INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION
 */
public class UserImpersonation {

  private static final Logger log = LoggerFactory.getLogger(UserImpersonation.class);
  private static final Set<String> ALWAYS_TRUE = new AlwaysTrueSet<>();
  private static final String ALL = "*", USERS = "users", HOSTS = "hosts";

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

    public UsersWithHosts(Set<String> users, Set<String> hosts) {
      this();
      this.users = users;
      this.hosts = hosts;
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

  @SuppressWarnings("deprecation")
  public UserImpersonation(AccumuloConfiguration conf) {
    proxyUsers = new HashMap<>();

    // Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION is treated as the "new config style" switch
    final String userConfig = conf.get(Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION);
    if (!Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION.getDefaultValue().equals(userConfig)) {
      String hostConfig = conf.get(Property.INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION);
      parseOnelineConfiguration(userConfig, hostConfig);
    } else {
      // Otherwise, assume the old-style
      parseMultiPropertyConfiguration(conf.getAllPropertiesWithPrefix(Property.INSTANCE_RPC_SASL_PROXYUSERS));
    }
  }

  /**
   * Parses the impersonation configuration for all users from a single property.
   *
   * @param userConfigString
   *          Semi-colon separated list of {@code remoteUser:alloweduser,alloweduser,...}.
   * @param hostConfigString
   *          Semi-colon separated list of hosts.
   */
  private void parseOnelineConfiguration(String userConfigString, String hostConfigString) {
    // Pull out the config values, defaulting to at least one value
    final String[] userConfigs;
    if (userConfigString.trim().isEmpty()) {
      userConfigs = new String[] {""};
    } else {
      userConfigs = StringUtils.split(userConfigString, ';');
    }
    final String[] hostConfigs;
    if (hostConfigString.trim().isEmpty()) {
      hostConfigs = new String[] {""};
    } else {
      hostConfigs = StringUtils.split(hostConfigString, ';');
    }

    if (userConfigs.length != hostConfigs.length) {
      String msg = String.format("Should have equal number of user and host impersonation elements in configuration. Got %d and %d elements, respectively.",
          userConfigs.length, hostConfigs.length);
      throw new IllegalArgumentException(msg);
    }

    for (int i = 0; i < userConfigs.length; i++) {
      final String userConfig = userConfigs[i];
      final String hostConfig = hostConfigs[i];

      final String[] splitUserConfig = StringUtils.split(userConfig, ':');
      if (2 != splitUserConfig.length) {
        throw new IllegalArgumentException("Expect a single colon-separated pair, but found '" + userConfig + "'");
      }

      final String remoteUser = splitUserConfig[0];
      final String allowedImpersonationsForRemoteUser = splitUserConfig[1];
      final UsersWithHosts usersWithHosts = new UsersWithHosts();

      proxyUsers.put(remoteUser.trim(), usersWithHosts);

      if (ALL.equals(allowedImpersonationsForRemoteUser)) {
        usersWithHosts.setAcceptAllUsers(true);
      } else {
        String[] allowedUsers = StringUtils.split(allowedImpersonationsForRemoteUser, ",");
        Set<String> usersSet = new HashSet<>();
        usersSet.addAll(Arrays.asList(allowedUsers));
        usersWithHosts.setUsers(usersSet);
      }

      if (ALL.equals(hostConfig)) {
        usersWithHosts.setAcceptAllHosts(true);
      } else {
        String[] allowedHosts = StringUtils.split(hostConfig, ",");
        Set<String> hostsSet = new HashSet<>();
        hostsSet.addAll(Arrays.asList(allowedHosts));
        usersWithHosts.setHosts(hostsSet);
      }
    }
  }

  /**
   * Parses all properties that start with {@link Property#INSTANCE_RPC_SASL_PROXYUSERS}. This approach was the original configuration method, but does not work
   * with Ambari.
   *
   * @param configProperties
   *          The relevant configuration properties for impersonation.
   */
  private void parseMultiPropertyConfiguration(Map<String,String> configProperties) {
    @SuppressWarnings("deprecation")
    final String configKey = Property.INSTANCE_RPC_SASL_PROXYUSERS.getKey();
    for (Entry<String,String> entry : configProperties.entrySet()) {
      String aclKey = entry.getKey().substring(configKey.length());
      int index = aclKey.lastIndexOf('.');

      if (-1 == index) {
        throw new RuntimeException("Expected 2 elements in key suffix: " + aclKey);
      }

      final String remoteUser = aclKey.substring(0, index).trim(), usersOrHosts = aclKey.substring(index + 1).trim();
      UsersWithHosts usersWithHosts = proxyUsers.get(remoteUser);
      if (null == usersWithHosts) {
        usersWithHosts = new UsersWithHosts();
        proxyUsers.put(remoteUser, usersWithHosts);
      }

      if (USERS.equals(usersOrHosts)) {
        String userString = entry.getValue().trim();
        if (ALL.equals(userString)) {
          usersWithHosts.setAcceptAllUsers(true);
        } else if (!usersWithHosts.acceptsAllUsers()) {
          Set<String> users = usersWithHosts.getUsers();
          if (null == users) {
            users = new HashSet<>();
            usersWithHosts.setUsers(users);
          }
          String[] userValues = StringUtils.split(userString, ',');
          users.addAll(Arrays.<String> asList(userValues));
        }
      } else if (HOSTS.equals(usersOrHosts)) {
        String hostsString = entry.getValue().trim();
        if (ALL.equals(hostsString)) {
          usersWithHosts.setAcceptAllHosts(true);
        } else if (!usersWithHosts.acceptsAllHosts()) {
          Set<String> hosts = usersWithHosts.getHosts();
          if (null == hosts) {
            hosts = new HashSet<>();
            usersWithHosts.setHosts(hosts);
          }
          String[] hostValues = StringUtils.split(hostsString, ',');
          hosts.addAll(Arrays.<String> asList(hostValues));
        }
      } else {
        log.debug("Ignoring key {}", aclKey);
      }
    }
  }

  public UsersWithHosts get(String remoteUser) {
    return proxyUsers.get(remoteUser);
  }

}
