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
package org.apache.accumulo.core.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.client.impl.AuthenticationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

public class AuthenticationTokenIdentifierTest {

  @Test
  public void testUgi() {
    String principal = "user";
    AuthenticationTokenIdentifier token = new AuthenticationTokenIdentifier(principal);
    UserGroupInformation actual = token.getUser(), expected = UserGroupInformation.createRemoteUser(principal);
    assertEquals(expected.getAuthenticationMethod(), actual.getAuthenticationMethod());
    assertEquals(expected.getUserName(), expected.getUserName());
  }

  @Test
  public void testEquality() {
    String principal = "user";
    AuthenticationTokenIdentifier token = new AuthenticationTokenIdentifier(principal);
    assertEquals(token, token);
    AuthenticationTokenIdentifier newToken = new AuthenticationTokenIdentifier(principal);
    assertEquals(token, newToken);
    assertEquals(token.hashCode(), newToken.hashCode());
  }

  @Test
  public void testExtendedEquality() {
    String principal = "user";
    AuthenticationTokenIdentifier token = new AuthenticationTokenIdentifier(principal);
    assertEquals(token, token);
    AuthenticationTokenIdentifier newToken = new AuthenticationTokenIdentifier(principal, 1, 5l, 10l, "uuid");
    assertNotEquals(token, newToken);
    assertNotEquals(token.hashCode(), newToken.hashCode());
    AuthenticationTokenIdentifier dblNewToken = new AuthenticationTokenIdentifier(principal);
    dblNewToken.setKeyId(1);
    dblNewToken.setIssueDate(5l);
    dblNewToken.setExpirationDate(10l);
    dblNewToken.setInstanceId("uuid");
  }

  @Test
  public void testToString() {
    String principal = "my_special_principal";
    AuthenticationTokenIdentifier token = new AuthenticationTokenIdentifier(principal);
    assertTrue(token.toString().contains(principal));
  }

  @Test
  public void testSerialization() throws IOException {
    String principal = "my_special_principal";
    AuthenticationTokenIdentifier token = new AuthenticationTokenIdentifier(principal);
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
    AuthenticationTokenIdentifier token = new AuthenticationTokenIdentifier(principal);
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
