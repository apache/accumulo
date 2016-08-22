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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.security.UserImpersonation.AlwaysTrueSet;
import org.apache.accumulo.server.security.UserImpersonation.UsersWithHosts;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

public class UserImpersonationTest {

  private ConfigurationCopy cc;
  private AccumuloConfiguration conf;

  @Before
  public void setup() {
    cc = new ConfigurationCopy(new HashMap<String,String>());
    conf = new AccumuloConfiguration() {
      DefaultConfiguration defaultConfig = DefaultConfiguration.getInstance();

      @Override
      public String get(Property property) {
        String value = cc.get(property);
        if (null == value) {
          return defaultConfig.get(property);
        }
        return value;
      }

      @Override
      public void getProperties(Map<String,String> props, Predicate<String> filter) {
        cc.getProperties(props, filter);
      }
    };
  }

  void setValidHosts(String user, String hosts) {
    setUsersOrHosts(user, ".hosts", hosts);
  }

  void setValidUsers(String user, String users) {
    setUsersOrHosts(user, ".users", users);
  }

  @SuppressWarnings("deprecation")
  void setUsersOrHosts(String user, String suffix, String value) {
    cc.set(Property.INSTANCE_RPC_SASL_PROXYUSERS.getKey() + user + suffix, value);
  }

  void setValidHostsNewConfig(String user, String... hosts) {
    cc.set(Property.INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION.getKey(), Joiner.on(';').join(hosts));
  }

  void setValidUsersNewConfig(Map<String,String> remoteToAllowedUsers) {
    StringBuilder sb = new StringBuilder();
    for (Entry<String,String> entry : remoteToAllowedUsers.entrySet()) {
      if (sb.length() > 0) {
        sb.append(";");
      }
      sb.append(entry.getKey()).append(":").append(entry.getValue());
    }
    cc.set(Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION, sb.toString());
  }

  @Test
  public void testAnyUserAndHosts() {
    String server = "server";
    setValidHosts(server, "*");
    setValidUsers(server, "*");
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNotNull(uwh);

    assertTrue(uwh.acceptsAllHosts());
    assertTrue(uwh.acceptsAllUsers());

    assertEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());
  }

  @Test
  public void testAnyUserAndHostsNewConfig() {
    String server = "server";
    setValidHostsNewConfig(server, "*");
    setValidUsersNewConfig(ImmutableMap.of(server, "*"));
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNotNull(uwh);

    assertTrue(uwh.acceptsAllHosts());
    assertTrue(uwh.acceptsAllUsers());

    assertEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());
  }

  @Test
  public void testNoHostByDefault() {
    String server = "server";
    setValidUsers(server, "*");
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNotNull(uwh);

    assertFalse(uwh.acceptsAllHosts());
    assertTrue(uwh.acceptsAllUsers());

    assertNotEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());
  }

  @Test
  public void testNoHostByDefaultNewConfig() {
    String server = "server";
    setValidUsersNewConfig(ImmutableMap.of(server, "*"));
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNotNull(uwh);

    assertFalse(uwh.acceptsAllHosts());
    assertTrue(uwh.acceptsAllUsers());

    assertNotEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());
  }

  @Test
  public void testNoUsersByDefault() {
    String server = "server";
    setValidHosts(server, "*");
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNotNull(uwh);

    assertTrue(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertNotEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());
  }

  @Test
  public void testNoUsersByDefaultNewConfig() {
    String server = "server";
    setValidHostsNewConfig(server, "*");
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNull("Impersonation config should be drive by user element, not host", uwh);
  }

  @Test
  public void testSingleUserAndHost() {
    String server = "server", host = "single_host.domain.com", client = "single_client";
    setValidHosts(server, host);
    setValidUsers(server, client);
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNotNull(uwh);

    assertFalse(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertNotEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertNotEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());

    assertTrue(uwh.getUsers().contains(client));
    assertTrue(uwh.getHosts().contains(host));

    assertFalse(uwh.getUsers().contains("some_other_user"));
    assertFalse(uwh.getHosts().contains("other_host.domain.com"));
  }

  @Test
  public void testSingleUserAndHostNewConfig() {
    String server = "server", host = "single_host.domain.com", client = "single_client";
    setValidHostsNewConfig(server, host);
    setValidUsersNewConfig(ImmutableMap.of(server, client));
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNotNull(uwh);

    assertFalse(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertNotEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertNotEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());

    assertTrue(uwh.getUsers().contains(client));
    assertTrue(uwh.getHosts().contains(host));

    assertFalse(uwh.getUsers().contains("some_other_user"));
    assertFalse(uwh.getHosts().contains("other_host.domain.com"));
  }

  @Test
  public void testMultipleExplicitUsers() {
    String server = "server", client1 = "client1", client2 = "client2", client3 = "client3";
    setValidHosts(server, "*");
    setValidUsers(server, Joiner.on(',').join(client1, client2, client3));
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNotNull(uwh);

    assertTrue(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertNotEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());

    assertTrue(uwh.getUsers().contains(client1));
    assertTrue(uwh.getUsers().contains(client2));
    assertTrue(uwh.getUsers().contains(client3));
    assertFalse(uwh.getUsers().contains("other_client"));
  }

  @Test
  public void testMultipleExplicitUsersNewConfig() {
    String server = "server", client1 = "client1", client2 = "client2", client3 = "client3";
    setValidHostsNewConfig(server, "*");
    setValidUsersNewConfig(ImmutableMap.of(server, Joiner.on(',').join(client1, client2, client3)));
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNotNull(uwh);

    assertTrue(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertNotEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());

    assertTrue(uwh.getUsers().contains(client1));
    assertTrue(uwh.getUsers().contains(client2));
    assertTrue(uwh.getUsers().contains(client3));
    assertFalse(uwh.getUsers().contains("other_client"));
  }

  @Test
  public void testMultipleExplicitHosts() {
    String server = "server", host1 = "host1", host2 = "host2", host3 = "host3";
    setValidHosts(server, Joiner.on(',').join(host1, host2, host3));
    setValidUsers(server, "*");
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNotNull(uwh);

    assertFalse(uwh.acceptsAllHosts());
    assertTrue(uwh.acceptsAllUsers());

    assertNotEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());

    assertTrue(uwh.getHosts().contains(host1));
    assertTrue(uwh.getHosts().contains(host2));
    assertTrue(uwh.getHosts().contains(host3));
    assertFalse(uwh.getHosts().contains("other_host"));
  }

  @Test
  public void testMultipleExplicitHostsNewConfig() {
    String server = "server", host1 = "host1", host2 = "host2", host3 = "host3";
    setValidHostsNewConfig(server, Joiner.on(',').join(host1, host2, host3));
    setValidUsersNewConfig(ImmutableMap.of(server, "*"));
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNotNull(uwh);

    assertFalse(uwh.acceptsAllHosts());
    assertTrue(uwh.acceptsAllUsers());

    assertNotEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());

    assertTrue(uwh.getHosts().contains(host1));
    assertTrue(uwh.getHosts().contains(host2));
    assertTrue(uwh.getHosts().contains(host3));
    assertFalse(uwh.getHosts().contains("other_host"));
  }

  @Test
  public void testMultipleExplicitUsersHosts() {
    String server = "server", host1 = "host1", host2 = "host2", host3 = "host3", client1 = "client1", client2 = "client2", client3 = "client3";
    setValidHosts(server, Joiner.on(',').join(host1, host2, host3));
    setValidUsers(server, Joiner.on(',').join(client1, client2, client3));
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNotNull(uwh);

    assertFalse(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertNotEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertNotEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());

    assertTrue(uwh.getUsers().contains(client1));
    assertTrue(uwh.getUsers().contains(client2));
    assertTrue(uwh.getUsers().contains(client3));
    assertFalse(uwh.getUsers().contains("other_client"));

    assertTrue(uwh.getHosts().contains(host1));
    assertTrue(uwh.getHosts().contains(host2));
    assertTrue(uwh.getHosts().contains(host3));
    assertFalse(uwh.getHosts().contains("other_host"));
  }

  @Test
  public void testMultipleExplicitUsersHostsNewConfig() {
    String server = "server", host1 = "host1", host2 = "host2", host3 = "host3", client1 = "client1", client2 = "client2", client3 = "client3";
    setValidHostsNewConfig(server, Joiner.on(',').join(host1, host2, host3));
    setValidUsersNewConfig(ImmutableMap.of(server, Joiner.on(',').join(client1, client2, client3)));
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);
    assertNotNull(uwh);

    assertFalse(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertNotEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertNotEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());

    assertTrue(uwh.getUsers().contains(client1));
    assertTrue(uwh.getUsers().contains(client2));
    assertTrue(uwh.getUsers().contains(client3));
    assertFalse(uwh.getUsers().contains("other_client"));

    assertTrue(uwh.getHosts().contains(host1));
    assertTrue(uwh.getHosts().contains(host2));
    assertTrue(uwh.getHosts().contains(host3));
    assertFalse(uwh.getHosts().contains("other_host"));
  }

  @Test
  public void testMultipleAllowedImpersonators() {
    String server1 = "server1", server2 = "server2", host1 = "host1", host2 = "host2", host3 = "host3", client1 = "client1", client2 = "client2", client3 = "client3";
    // server1 can impersonate client1 and client2 from host1 or host2
    setValidHosts(server1, Joiner.on(',').join(host1, host2));
    setValidUsers(server1, Joiner.on(',').join(client1, client2));
    // server2 can impersonate only client3 from host3
    setValidHosts(server2, host3);
    setValidUsers(server2, client3);
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server1);
    assertNotNull(uwh);

    assertFalse(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertNotEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertNotEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());

    assertTrue(uwh.getUsers().contains(client1));
    assertTrue(uwh.getUsers().contains(client2));
    assertFalse(uwh.getUsers().contains(client3));
    assertFalse(uwh.getUsers().contains("other_client"));

    assertTrue(uwh.getHosts().contains(host1));
    assertTrue(uwh.getHosts().contains(host2));
    assertFalse(uwh.getHosts().contains(host3));
    assertFalse(uwh.getHosts().contains("other_host"));

    uwh = impersonation.get(server2);
    assertNotNull(uwh);

    assertFalse(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertNotEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertNotEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());

    assertFalse(uwh.getUsers().contains(client1));
    assertFalse(uwh.getUsers().contains(client2));
    assertTrue(uwh.getUsers().contains(client3));
    assertFalse(uwh.getUsers().contains("other_client"));

    assertFalse(uwh.getHosts().contains(host1));
    assertFalse(uwh.getHosts().contains(host2));
    assertTrue(uwh.getHosts().contains(host3));
    assertFalse(uwh.getHosts().contains("other_host"));

    // client3 is not allowed to impersonate anyone
    assertNull(impersonation.get(client3));
  }

  @Test
  public void testMultipleAllowedImpersonatorsNewConfig() {
    String server1 = "server1", server2 = "server2", host1 = "host1", host2 = "host2", host3 = "host3", client1 = "client1", client2 = "client2", client3 = "client3";
    // server1 can impersonate client1 and client2 from host1 or host2
    // server2 can impersonate only client3 from host3
    setValidHostsNewConfig(server1, Joiner.on(',').join(host1, host2), host3);
    setValidUsersNewConfig(ImmutableMap.of(server1, Joiner.on(',').join(client1, client2), server2, client3));
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server1);
    assertNotNull(uwh);

    assertFalse(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertNotEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertNotEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());

    assertTrue(uwh.getUsers().contains(client1));
    assertTrue(uwh.getUsers().contains(client2));
    assertFalse(uwh.getUsers().contains(client3));
    assertFalse(uwh.getUsers().contains("other_client"));

    assertTrue(uwh.getHosts().contains(host1));
    assertTrue(uwh.getHosts().contains(host2));
    assertFalse(uwh.getHosts().contains(host3));
    assertFalse(uwh.getHosts().contains("other_host"));

    uwh = impersonation.get(server2);
    assertNotNull(uwh);

    assertFalse(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertNotEquals(AlwaysTrueSet.class, uwh.getHosts().getClass());
    assertNotEquals(AlwaysTrueSet.class, uwh.getUsers().getClass());

    assertFalse(uwh.getUsers().contains(client1));
    assertFalse(uwh.getUsers().contains(client2));
    assertTrue(uwh.getUsers().contains(client3));
    assertFalse(uwh.getUsers().contains("other_client"));

    assertFalse(uwh.getHosts().contains(host1));
    assertFalse(uwh.getHosts().contains(host2));
    assertTrue(uwh.getHosts().contains(host3));
    assertFalse(uwh.getHosts().contains("other_host"));

    // client3 is not allowed to impersonate anyone
    assertNull(impersonation.get(client3));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSingleUser() throws Exception {
    final String server = "server/hostname@EXAMPLE.COM", client = "client@EXAMPLE.COM";
    cc.set(Property.INSTANCE_RPC_SASL_PROXYUSERS.getKey() + server + ".users", client);
    cc.set(Property.INSTANCE_RPC_SASL_PROXYUSERS.getKey() + server + ".hosts", "*");
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);

    assertNotNull(uwh);

    assertTrue(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertTrue(uwh.getUsers().contains(client));
  }

  @Test
  public void testSingleUserNewConfig() throws Exception {
    final String server = "server/hostname@EXAMPLE.COM", client = "client@EXAMPLE.COM";
    cc.set(Property.INSTANCE_RPC_SASL_ALLOWED_USER_IMPERSONATION, server + ":" + client);
    cc.set(Property.INSTANCE_RPC_SASL_ALLOWED_HOST_IMPERSONATION, "*");
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);

    assertNotNull(uwh);

    assertTrue(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertTrue(uwh.getUsers().contains(client));
  }
}
