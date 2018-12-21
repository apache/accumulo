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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.security.auth.DestroyFailedException;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.AuthenticationTokenSerializer;
import org.apache.accumulo.core.client.security.tokens.NullToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class CredentialsTest {

  @Rule
  public TestName test = new TestName();

  private String instanceID = test.getMethodName();

  @Test
  public void testToThrift() throws DestroyFailedException {
    // verify thrift serialization
    Credentials creds = new Credentials("test", new PasswordToken("testing"));
    TCredentials tCreds = creds.toThrift(instanceID);
    assertEquals("test", tCreds.getPrincipal());
    assertEquals(PasswordToken.class.getName(), tCreds.getTokenClassName());
    assertArrayEquals(AuthenticationTokenSerializer.serialize(new PasswordToken("testing")),
        tCreds.getToken());

    // verify that we can't serialize if it's destroyed
    creds.getToken().destroy();
    try {
      creds.toThrift(instanceID);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof RuntimeException);
      assertTrue(e.getCause() instanceof AccumuloSecurityException);
      assertEquals(AccumuloSecurityException.class.cast(e.getCause()).getSecurityErrorCode(),
          SecurityErrorCode.TOKEN_EXPIRED);
    }
  }

  @Test
  public void roundtripThrift() {
    Credentials creds = new Credentials("test", new PasswordToken("testing"));
    TCredentials tCreds = creds.toThrift(instanceID);
    Credentials roundtrip = Credentials.fromThrift(tCreds);
    assertEquals("Roundtrip through thirft changed credentials equality", creds, roundtrip);
  }

  @Test
  public void testEqualsAndHashCode() {
    Credentials nullNullCreds = new Credentials(null, null);
    Credentials abcNullCreds = new Credentials("abc", new NullToken());
    Credentials cbaNullCreds = new Credentials("cba", new NullToken());
    Credentials abcBlahCreds = new Credentials("abc", new PasswordToken("blah"));

    // check hash codes
    assertEquals(0, nullNullCreds.hashCode());
    assertEquals("abc".hashCode(), abcNullCreds.hashCode());
    assertEquals(abcNullCreds.hashCode(), abcBlahCreds.hashCode());
    assertNotEquals(abcNullCreds.hashCode(), cbaNullCreds.hashCode());

    // identity
    assertEquals(abcNullCreds, abcNullCreds);
    assertEquals(new Credentials("abc", new NullToken()), abcNullCreds);
    // equal, but different token constructors
    assertEquals(new Credentials("abc", new PasswordToken("abc".getBytes(UTF_8))),
        new Credentials("abc", new PasswordToken("abc")));
    // test not equals
    assertNotEquals(nullNullCreds, abcBlahCreds);
    assertNotEquals(nullNullCreds, abcNullCreds);
    assertNotEquals(abcNullCreds, abcBlahCreds);
  }

  @Test
  public void testCredentialsSerialization() {
    Credentials creds = new Credentials("a:b-c", new PasswordToken("d-e-f".getBytes(UTF_8)));
    String serialized = creds.serialize();
    Credentials result = Credentials.deserialize(serialized);
    assertEquals(creds, result);
    assertEquals("a:b-c", result.getPrincipal());
    assertEquals(new PasswordToken("d-e-f"), result.getToken());

    Credentials nullNullCreds = new Credentials(null, null);
    serialized = nullNullCreds.serialize();
    result = Credentials.deserialize(serialized);
    assertNull(result.getPrincipal());
    assertNull(result.getToken());
  }

  @Test
  public void testToString() {
    Credentials creds = new Credentials(null, null);
    assertEquals(Credentials.class.getName() + ":null:null:<hidden>", creds.toString());
    creds = new Credentials("", new NullToken());
    assertEquals(Credentials.class.getName() + "::" + NullToken.class.getName() + ":<hidden>",
        creds.toString());
    creds = new Credentials("abc", null);
    assertEquals(Credentials.class.getName() + ":abc:null:<hidden>", creds.toString());
    creds = new Credentials("abc", new PasswordToken(""));
    assertEquals(
        Credentials.class.getName() + ":abc:" + PasswordToken.class.getName() + ":<hidden>",
        creds.toString());
  }
}
