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
package org.apache.accumulo.core.security;

import static org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier.createTAuthIdentifier;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.securityImpl.thrift.TAuthenticationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

public class AuthenticationTokenIdentifierTest {

  @Test
  public void testUgi() {
    String principal = "user";
    var token = new AuthenticationTokenIdentifier(new TAuthenticationTokenIdentifier(principal));
    UserGroupInformation actual = token.getUser();
    UserGroupInformation expected = UserGroupInformation.createRemoteUser(principal);
    assertEquals(expected.getAuthenticationMethod(), actual.getAuthenticationMethod());
    assertEquals(expected.getUserName(), expected.getUserName());
  }

  @Test
  public void testEquality() {
    String principal = "user";
    var token = new AuthenticationTokenIdentifier(new TAuthenticationTokenIdentifier(principal));
    assertEquals(token, token);
    var newToken = new AuthenticationTokenIdentifier(new TAuthenticationTokenIdentifier(principal));
    assertEquals(token, newToken);
    assertEquals(token.hashCode(), newToken.hashCode());
  }

  @Test
  public void testExtendedEquality() {
    String principal = "user";
    var token = new AuthenticationTokenIdentifier(new TAuthenticationTokenIdentifier(principal));
    assertEquals(token, token);
    var newToken =
        new AuthenticationTokenIdentifier(createTAuthIdentifier(principal, 1, 5L, 10L, "uuid"));
    assertNotEquals(token, newToken);
    assertNotEquals(token.hashCode(), newToken.hashCode());
    var dblNewToken =
        new AuthenticationTokenIdentifier(new TAuthenticationTokenIdentifier(principal));
    dblNewToken.setKeyId(1);
    dblNewToken.setIssueDate(5L);
    dblNewToken.setExpirationDate(10L);
    dblNewToken.setInstanceId(InstanceId.of("uuid"));
  }

  @Test
  public void testToString() {
    String principal = "my_special_principal";
    var token = new AuthenticationTokenIdentifier(new TAuthenticationTokenIdentifier(principal));
    assertTrue(token.toString().contains(principal));
  }

  @Test
  public void testSerialization() throws IOException {
    String principal = "my_special_principal";
    var token = new AuthenticationTokenIdentifier(new TAuthenticationTokenIdentifier(principal));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    token.write(out);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    AuthenticationTokenIdentifier deserializedToken = new AuthenticationTokenIdentifier();
    deserializedToken.readFields(in);
    assertEquals(token, deserializedToken);
    assertEquals(token.hashCode(), deserializedToken.hashCode());
    assertEquals(token.toString(), deserializedToken.toString());
  }

  @Test
  public void testTokenKind() {
    String principal = "my_special_principal";
    var token = new AuthenticationTokenIdentifier(new TAuthenticationTokenIdentifier(principal));
    assertEquals(AuthenticationTokenIdentifier.TOKEN_KIND, token.getKind());
  }

  @Test
  public void testNullMsg() throws IOException {
    AuthenticationTokenIdentifier token = new AuthenticationTokenIdentifier();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    token.write(out);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    AuthenticationTokenIdentifier deserializedToken = new AuthenticationTokenIdentifier();
    deserializedToken.readFields(in);
    assertEquals(token, deserializedToken);
    assertEquals(token.hashCode(), deserializedToken.hashCode());
    assertEquals(token.toString(), deserializedToken.toString());
  }
}
