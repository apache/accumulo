/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.client.security.tokens;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.Test;

/**
 * Test class for {@link KerberosToken}.
 */
public class KerberosTokenTest {

  @Test
  public void testAuthMethodAcceptance() {
    // There is also KERBEROS_SSL but that appears to be deprecated/OBE
    Set<AuthenticationMethod> allowedMethods = new HashSet<>(Arrays.asList(AuthenticationMethod.KERBEROS, AuthenticationMethod.PROXY));
    for (AuthenticationMethod authMethod : AuthenticationMethod.values()) {
      final boolean allowable = allowedMethods.contains(authMethod);
      try {
        KerberosToken.validateAuthMethod(authMethod);
        if (!allowable) {
          fail(authMethod + " should have triggered a thrown exception but it did not");
        }
      } catch (IllegalArgumentException e) {
        if (allowable) {
          fail(authMethod + " should not have triggered a thrown exception");
        }
      }
    }
  }
}
