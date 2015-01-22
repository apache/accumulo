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

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.security.UserImpersonation.AlwaysTrueSet;
import org.apache.accumulo.server.security.UserImpersonation.UsersWithHosts;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;

/**
 *
 */
public class UserImpersonationTest {

  private ConfigurationCopy conf;

  @Before
  public void setup() {
    conf = new ConfigurationCopy(new HashMap<String,String>());
  }

  void setValidHosts(String user, String hosts) {
    setUsersOrHosts(user, ".hosts", hosts);
  }

  void setValidUsers(String user, String users) {
    setUsersOrHosts(user, ".users", users);
  }

  void setUsersOrHosts(String user, String suffix, String value) {
    conf.set(Property.INSTANCE_RPC_SASL_PROXYUSERS.getKey() + user + suffix, value);
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
  public void testSingleUser() throws Exception {
    final String server = "server/hostname@EXAMPLE.COM", client = "client@EXAMPLE.COM";
    conf.set(Property.INSTANCE_RPC_SASL_PROXYUSERS.getKey() + server + ".users", client);
    conf.set(Property.INSTANCE_RPC_SASL_PROXYUSERS.getKey() + server + ".hosts", "*");
    UserImpersonation impersonation = new UserImpersonation(conf);

    UsersWithHosts uwh = impersonation.get(server);

    assertNotNull(uwh);

    assertTrue(uwh.acceptsAllHosts());
    assertFalse(uwh.acceptsAllUsers());

    assertTrue(uwh.getUsers().contains(client));
  }
}
