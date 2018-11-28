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
package org.apache.accumulo.core.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ClientPropertyTest {

  @Test
  public void testAuthentication() {
    Properties props = new Properties();
    props.setProperty(ClientProperty.AUTH_PRINCIPAL.getKey(), "user");
    ClientProperty.setPassword(props, "testpass1");
    assertEquals("testpass1", ClientProperty.AUTH_TOKEN.getValue(props));
    AuthenticationToken token = ClientProperty.getAuthenticationToken(props);
    assertTrue(token instanceof PasswordToken);
    assertEquals("testpass1", new String(((PasswordToken) token).getPassword()));

    ClientProperty.setAuthenticationToken(props, new PasswordToken("testpass2"));
    assertEquals("AAAAHR+LCAAAAAAAAAArSS0uKUgsLjYCANxwRH4JAAAA",
        ClientProperty.AUTH_TOKEN.getValue(props));
    token = ClientProperty.getAuthenticationToken(props);
    assertTrue(token instanceof PasswordToken);
    assertEquals("testpass2", new String(((PasswordToken) token).getPassword()));

    ClientProperty.setAuthenticationToken(props, new PasswordToken("testpass3"));
    assertEquals("AAAAHR+LCAAAAAAAAAArSS0uKUgsLjYGAEpAQwkJAAAA",
        ClientProperty.AUTH_TOKEN.getValue(props));
    token = ClientProperty.getAuthenticationToken(props);
    assertTrue(token instanceof PasswordToken);
    assertEquals("testpass3", new String(((PasswordToken) token).getPassword()));

    ClientProperty.setKerberosKeytab(props, "/path/to/keytab");
    assertEquals("/path/to/keytab", ClientProperty.AUTH_TOKEN.getValue(props));
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testTypes() {
    Properties props = new Properties();
    props.setProperty(ClientProperty.BATCH_WRITER_LATENCY_MAX.getKey(), "10s");
    Long value = ClientProperty.BATCH_WRITER_LATENCY_MAX.getTimeInMillis(props);
    assertEquals(10000L, value.longValue());

    props.setProperty(ClientProperty.BATCH_WRITER_MEMORY_MAX.getKey(), "555M");
    value = ClientProperty.BATCH_WRITER_MEMORY_MAX.getBytes(props);
    assertEquals(581959680L, value.longValue());

    ClientProperty.BATCH_WRITER_MEMORY_MAX.setBytes(props, 5819L);
    value = ClientProperty.BATCH_WRITER_MEMORY_MAX.getBytes(props);
    assertEquals(5819L, value.longValue());

    ClientProperty.BATCH_WRITER_LATENCY_MAX.setTimeInMillis(props, 1234L);
    value = ClientProperty.BATCH_WRITER_LATENCY_MAX.getTimeInMillis(props);
    assertEquals(1234L, value.longValue());

    exception.expect(IllegalStateException.class);
    ClientProperty.BATCH_WRITER_LATENCY_MAX.getBytes(props);
  }
}
