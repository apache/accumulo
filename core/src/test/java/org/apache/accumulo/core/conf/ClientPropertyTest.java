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

import java.util.Properties;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Assert;
import org.junit.Test;

public class ClientPropertyTest {

  @Test
  public void testAuthentication() {
    Properties props = new Properties();
    props.setProperty(ClientProperty.AUTH_PRINCIPAL.getKey(), "user");
    ClientProperty.setPassword(props, "testpass1");
    Assert.assertEquals("testpass1", ClientProperty.AUTH_TOKEN.getValue(props));
    AuthenticationToken token = ClientProperty.getAuthenticationToken(props);
    Assert.assertTrue(token instanceof PasswordToken);
    Assert.assertEquals("testpass1", new String(((PasswordToken) token).getPassword()));

    ClientProperty.setAuthenticationToken(props, new PasswordToken("testpass2"));
    Assert.assertEquals("AAAAHR+LCAAAAAAAAAArSS0uKUgsLjYCANxwRH4JAAAA",
        ClientProperty.AUTH_TOKEN.getValue(props));
    token = ClientProperty.getAuthenticationToken(props);
    Assert.assertTrue(token instanceof PasswordToken);
    Assert.assertEquals("testpass2", new String(((PasswordToken) token).getPassword()));

    ClientProperty.setAuthenticationToken(props, new PasswordToken("testpass3"));
    Assert.assertEquals("AAAAHR+LCAAAAAAAAAArSS0uKUgsLjYGAEpAQwkJAAAA",
        ClientProperty.AUTH_TOKEN.getValue(props));
    token = ClientProperty.getAuthenticationToken(props);
    Assert.assertTrue(token instanceof PasswordToken);
    Assert.assertEquals("testpass3", new String(((PasswordToken) token).getPassword()));

    ClientProperty.setKerberosKeytab(props, "/path/to/keytab");
    Assert.assertEquals("/path/to/keytab", ClientProperty.AUTH_TOKEN.getValue(props));
  }
}
