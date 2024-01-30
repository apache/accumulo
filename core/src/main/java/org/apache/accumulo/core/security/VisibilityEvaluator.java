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

import java.util.ArrayList;

import org.apache.accumulo.access.AccessEvaluator;

/**
 * A class which evaluates visibility expressions against a set of authorizations.
 */
public class VisibilityEvaluator {
  private final AccessEvaluator evaluator;

  /**
   * Creates a new evaluator for the authorizations found in the given container.
   *
   * @since 1.7.0
   */
  public VisibilityEvaluator(AuthorizationContainer authsContainer) {
    this.evaluator = AccessEvaluator.of(authsContainer.getAuthorizations());
  }

  /**
   * Creates a new evaluator for the given collection of authorizations. Each authorization string
   * is escaped before handling, and the original strings are unchanged.
   *
   * @param authorizations authorizations object
   */
  public VisibilityEvaluator(Authorizations authorizations) {
    this.evaluator = AccessEvaluator.of(authorizations.getAuthorizations());
  }

  /**
   * Evaluates the given column visibility against the authorizations provided to this evaluator. A
   * visibility passes evaluation if all authorizations in it are contained in those known to the
   * evaluator, and all AND and OR subexpressions have at least two children.
   *
   * @param visibility column visibility to evaluate
   * @return true if visibility passes evaluation
   * @throws VisibilityParseException if an AND or OR subexpression has less than two children, or a
   *         subexpression is of an unknown type
   */
  public boolean evaluate(ColumnVisibility visibility) throws VisibilityParseException {
    return this.evaluator.canAccess(visibility.getExpression());
  }

  /**
   * Creates a new {@link Authorizations} object with escaped forms of the authorizations in the
   * given object.
   *
   * @param auths original authorizations
   * @return authorizations object with escaped authorization strings
   * @see #escape(byte[], boolean)
   */
  static Authorizations escape(Authorizations auths) {
    ArrayList<byte[]> retAuths = new ArrayList<>(auths.getAuthorizations().size());

    for (byte[] auth : auths.getAuthorizations()) {
      retAuths.add(escape(auth, false));
    }
    return new Authorizations(retAuths);
  }

  /**
   * Properly escapes an authorization string. The string can be quoted if desired.
   *
   * @param auth authorization string, as UTF-8 encoded bytes
   * @param quote true to wrap escaped authorization in quotes
   * @return escaped authorization string
   */
  public static byte[] escape(byte[] auth, boolean quote) {
    int escapeCount = 0;

    for (byte value : auth) {
      if (value == '"' || value == '\\') {
        escapeCount++;
      }
    }

    if (escapeCount > 0 || quote) {
      byte[] escapedAuth = new byte[auth.length + escapeCount + (quote ? 2 : 0)];
      int index = quote ? 1 : 0;
      for (byte b : auth) {
        if (b == '"' || b == '\\') {
          escapedAuth[index++] = '\\';
        }
        escapedAuth[index++] = b;
      }

      if (quote) {
        escapedAuth[0] = '"';
        escapedAuth[escapedAuth.length - 1] = '"';
      }

      auth = escapedAuth;
    }
    return auth;
  }

}
