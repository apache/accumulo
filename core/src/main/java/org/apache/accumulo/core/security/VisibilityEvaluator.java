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

import java.util.ArrayList;

import org.apache.accumulo.core.constraints.Constraint.Environment;
import org.apache.accumulo.core.security.ColumnVisibility.Node;

/**
 * A class which evaluates visibility expressions against a set of authorizations.
 */
public class VisibilityEvaluator {
  private AuthorizationContainer auths;

  /**
   * Creates a new {@link Authorizations} object with escaped forms of the authorizations in the given object.
   *
   * @param auths
   *          original authorizations
   * @return authorizations object with escaped authorization strings
   * @see #escape(byte[], boolean)
   */
  static Authorizations escape(Authorizations auths) {
    ArrayList<byte[]> retAuths = new ArrayList<byte[]>(auths.getAuthorizations().size());

    for (byte[] auth : auths.getAuthorizations())
      retAuths.add(escape(auth, false));

    return new Authorizations(retAuths);
  }

  /**
   * Properly escapes an authorization string. The string can be quoted if desired.
   *
   * @param auth
   *          authorization string, as UTF-8 encoded bytes
   * @param quote
   *          true to wrap escaped authorization in quotes
   * @return escaped authorization string
   */
  public static byte[] escape(byte[] auth, boolean quote) {
    int escapeCount = 0;

    for (int i = 0; i < auth.length; i++)
      if (auth[i] == '"' || auth[i] == '\\')
        escapeCount++;

    if (escapeCount > 0 || quote) {
      byte[] escapedAuth = new byte[auth.length + escapeCount + (quote ? 2 : 0)];
      int index = quote ? 1 : 0;
      for (int i = 0; i < auth.length; i++) {
        if (auth[i] == '"' || auth[i] == '\\')
          escapedAuth[index++] = '\\';
        escapedAuth[index++] = auth[i];
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
   * Creates a new evaluator for the authorizations found in the given environment.
   *
   * @param env
   *          environment containing authorizations
   */
  VisibilityEvaluator(Environment env) {
    this.auths = env.getAuthorizationsContainer();
  }

  /**
   * Creates a new evaluator for the given collection of authorizations. Each authorization string is escaped before handling, and the original strings are
   * unchanged.
   *
   * @param authorizations
   *          authorizations object
   */
  public VisibilityEvaluator(Authorizations authorizations) {
    this.auths = escape((Authorizations) authorizations);
  }

  /**
   * Evaluates the given column visibility against the authorizations provided to this evaluator. A visibility passes evaluation if all authorizations in it are
   * contained in those known to the evaluator, and all AND and OR subexpressions have at least two children.
   *
   * @param visibility
   *          column visibility to evaluate
   * @return true if visibility passes evaluation
   * @throws VisibilityParseException
   *           if an AND or OR subexpression has less than two children, or a subexpression is of an unknown type
   */
  public boolean evaluate(ColumnVisibility visibility) throws VisibilityParseException {
    // The VisibilityEvaluator computes a trie from the given Authorizations, that ColumnVisibility expressions can be evaluated against.
    return evaluate(visibility.getExpression(), visibility.getParseTree());
  }

  private final boolean evaluate(final byte[] expression, final Node root) throws VisibilityParseException {
    if (expression.length == 0)
      return true;
    switch (root.type) {
      case TERM:
        return auths.contains(root.getTerm(expression));
      case AND:
        if (root.children == null || root.children.size() < 2)
          throw new VisibilityParseException("AND has less than 2 children", expression, root.start);
        for (Node child : root.children) {
          if (!evaluate(expression, child))
            return false;
        }
        return true;
      case OR:
        if (root.children == null || root.children.size() < 2)
          throw new VisibilityParseException("OR has less than 2 children", expression, root.start);
        for (Node child : root.children) {
          if (evaluate(expression, child))
            return true;
        }
        return false;
      default:
        throw new VisibilityParseException("No such node type", expression, root.start);
    }
  }
}
