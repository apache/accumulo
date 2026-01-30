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
package org.apache.accumulo.core.clientImpl.access;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.accumulo.access.Access;
import org.apache.accumulo.access.AccessEvaluator;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.security.AuthorizationContainer;
import org.apache.accumulo.core.security.Authorizations;

/**
 * All Accumulo Access APIs are String based. Accumulo's legacy APIs for access control are all
 * based on byte[]. This class maps those legacy byte[] based APIs to use the Accumulo Access string
 * based APIs.
 *
 * <p>
 * This mapping is done by using ISO_8859_1 to convert byte[] to String and vice versa. ISO_8859_1
 * is used because it maps each unsigned integer in a byte array directly to the same unsigned
 * integer in the strings char array. This preserves the behavior of Accumulo legacy APIs by making
 * Accumulo Access operate on the exact same data as the legacy APIs did.
 *
 * <p>
 * All transformations using ISO_8859_1 should remain completely within this class and never escape.
 * For example if a string escaped this class it could lead to an overall situation where
 * {@code new String(bytes,ISO_8859_1).getBytes(UTF_8)} accidentally happens.
 *
 * <p>
 * In addition to using ISO_8859_1, an instance of Accumulo Access is created that does no
 * validation of authorizations. By default, Accumulo Access would reject some chars which
 * conceptually would reject some bytes. The legacy Accumulo APIs accepted any bytes.
 *
 * <p>
 * This class is meant to serve as a bridge between legacy behavior that accepted any byte in an
 * access expression and newer default behavior in Accumulo Access that only accepts valid Unicode.
 */
public class BytesAccess {

  private static final Access ACCESS =
      Access.builder().authorizationValidator((auth, authChars) -> true).build();

  public static void validate(byte[] expression) {
    ACCESS.validateExpression(new String(expression, ISO_8859_1));
  }

  public static byte[] quote(byte[] auth) {
    return ACCESS.quote(new String(auth, ISO_8859_1)).getBytes(ISO_8859_1);
  }

  public static String quote(String auth) {
    // TODO is this safe? does not go through a byte array
    return ACCESS.quote(auth);
  }

  public static void findAuthorizations(byte[] expression, Consumer<byte[]> consumer) {
    ACCESS.findAuthorizations(new String(expression, ISO_8859_1),
        authString -> consumer.accept(authString.getBytes(ISO_8859_1)));
  }

  public static class BytesEvaluator {

    private final AccessEvaluator evaluator;

    private BytesEvaluator(AccessEvaluator evaluator) {
      this.evaluator = evaluator;
    }

    public boolean canAccess(byte[] expression) {
      return evaluator.canAccess(new String(expression, ISO_8859_1));
    }
  }

  public static BytesEvaluator newEvaluator(Authorizations auths) {
    List<byte[]> bytesAuths = auths.getAuthorizations();
    Set<String> stringAuths = new HashSet<>(bytesAuths.size());
    for (var auth : bytesAuths) {
      stringAuths.add(new String(auth, ISO_8859_1));
    }
    return new BytesEvaluator(ACCESS.newEvaluator(ACCESS.newAuthorizations(stringAuths)));
  }

  public static BytesEvaluator newEvaluator(AuthorizationContainer authContainer) {
    AccessEvaluator.Authorizer authorizer = authString -> authContainer
        .contains(new ArrayByteSequence(authString.getBytes(ISO_8859_1)));
    return new BytesEvaluator(ACCESS.newEvaluator(authorizer));
  }
}
