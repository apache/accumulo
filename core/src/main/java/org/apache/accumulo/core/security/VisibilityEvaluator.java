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

import org.apache.accumulo.access.AccessEvaluator;
import org.apache.accumulo.access.IllegalAccessExpressionException;
import org.apache.accumulo.core.data.ArrayByteSequence;

/**
 * A class which evaluates visibility expressions against a set of authorizations.
 *
 * @deprecated since 3.1.0 Use Accumulo Access library instead
 */
@Deprecated(since = "3.1.0")
public class VisibilityEvaluator {
  private final AccessEvaluator accessEvaluator;

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

  /**
   * Creates a new evaluator for the authorizations found in the given container.
   *
   * @since 1.7.0
   */
  public VisibilityEvaluator(AuthorizationContainer authsContainer) {
    // TODO need to look into efficiency and correctness of this
    this.accessEvaluator =
        AccessEvaluator.of(auth -> authsContainer.contains(new ArrayByteSequence(auth)));
  }

  /**
   * Creates a new evaluator for the given collection of authorizations. Each authorization string
   * is escaped before handling, and the original strings are unchanged.
   *
   * @param authorizations authorizations object
   */
  public VisibilityEvaluator(Authorizations authorizations) {
    this.accessEvaluator = AccessEvaluator.of(authorizations.toAccessAuthorizations());
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
    try {
      return accessEvaluator.canAccess(visibility.getExpression());
    } catch (IllegalAccessExpressionException e) {
      // This is thrown for compatability with the exception this class used to evaluate expressions
      // itself.
      throw new VisibilityParseException(e);
    }
  }
}
