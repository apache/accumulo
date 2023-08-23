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
package org.apache.accumulo.visibility;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableList;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

//this class is intentionally package private and should never be made public
class VisibilityArbiterImpl implements VisibilityArbiter {
  private final Predicate<BytesWrapper> authorizedPredicate;

  private VisibilityArbiterImpl(AuthorizationChecker authorizationChecker) {
    this.authorizedPredicate = auth -> authorizationChecker.isAuthorized(unescape(auth));
  }

  public VisibilityArbiterImpl(List<byte[]> authorizations) {
    var escapedAuths =
        authorizations.stream().map(auth -> VisibilityArbiterImpl.escape(auth, false))
            .map(BytesWrapper::new).collect(toSet());
    this.authorizedPredicate = escapedAuths::contains;
  }

  static byte[] unescape(BytesWrapper auth) {
    int escapeCharCount = 0;
    for (int i = 0; i < auth.length(); i++) {
      byte b = auth.byteAt(i);
      if (b == '"' || b == '\\') {
        escapeCharCount++;
      }
    }

    if (escapeCharCount > 0) {
      if (escapeCharCount % 2 == 1) {
        throw new IllegalArgumentException("Illegal escape sequence in auth : " + auth);
      }

      byte[] unescapedCopy = new byte[auth.length() - escapeCharCount / 2];
      int pos = 0;
      for (int i = 0; i < auth.length(); i++) {
        byte b = auth.byteAt(i);
        if (b == '\\') {
          i++;
          b = auth.byteAt(i);
          if (b != '"' && b != '\\') {
            throw new IllegalArgumentException("Illegal escape sequence in auth : " + auth);
          }
        } else if (b == '"') {
          // should only see quote after a slash
          throw new IllegalArgumentException("Illegal escape sequence in auth : " + auth);
        }

        unescapedCopy[pos++] = b;
      }

      return unescapedCopy;
    } else {
      return auth.toArray();
    }
  }

  /**
   * Properly escapes an authorization string. The string can be quoted if desired.
   *
   * @param auth authorization string, as UTF-8 encoded bytes
   * @param quote true to wrap escaped authorization in quotes
   * @return escaped authorization string
   */
  static byte[] escape(byte[] auth, boolean quote) {
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

  @Override
  public boolean isVisible(String expression) throws IllegalArgumentException {

    return evaluate(new VisibilityExpressionImpl(expression));

  }

  @Override
  public boolean isVisible(byte[] expression) throws IllegalArgumentException {

    return evaluate(new VisibilityExpressionImpl(expression));

  }

  @Override
  public boolean isVisible(VisibilityExpression expression) throws IllegalArgumentException {
    if (expression instanceof VisibilityExpressionImpl) {

      return evaluate((VisibilityExpressionImpl) expression);

    } else {
      return isVisible(expression.getExpression());
    }
  }

  public boolean evaluate(VisibilityExpressionImpl visibility) throws IllegalVisibilityException {
    // The VisibilityEvaluator computes a trie from the given Authorizations, that ColumnVisibility
    // expressions can be evaluated against.
    return evaluate(visibility.getExpressionBytes(), visibility.getParseTree());
  }

  private boolean evaluate(final byte[] expression, final VisibilityExpressionImpl.Node root)
      throws IllegalVisibilityException {
    if (expression.length == 0) {
      return true;
    }
    switch (root.type) {
      case TERM:
        return authorizedPredicate.test(root.getTerm(expression));
      case AND:
        if (root.children == null || root.children.size() < 2) {
          throw new IllegalVisibilityException("AND has less than 2 children",
              root.getTerm(expression).toString(), root.start);
        }
        for (VisibilityExpressionImpl.Node child : root.children) {
          if (!evaluate(expression, child)) {
            return false;
          }
        }
        return true;
      case OR:
        if (root.children == null || root.children.size() < 2) {
          throw new IllegalVisibilityException("OR has less than 2 children",
              root.getTerm(expression).toString(), root.start);
        }
        for (VisibilityExpressionImpl.Node child : root.children) {
          if (evaluate(expression, child)) {
            return true;
          }
        }
        return false;
      default:
        throw new IllegalVisibilityException("No such node type",
            root.getTerm(expression).toString(), root.start);
    }
  }

  private static class BuilderImpl
      implements AuthorizationsBuilder, FinalBuilder, ExecutionBuilder {

    private AuthorizationChecker authorizationsChecker;

    private List<byte[]> authorizations;
    private int cacheSize = 0;

    private void setAuthorizations(List<byte[]> auths) {
      if (authorizationsChecker != null) {
        throw new IllegalStateException("Cannot set checker and authorizations");
      }

      for (byte[] auth : auths) {
        if (auth.length == 0) {
          throw new IllegalArgumentException("Empty authorization");
        }
      }

      this.authorizations = auths;
    }

    @Override
    public ExecutionBuilder authorizations(List<byte[]> authorizations) {
      // copy the passed in byte arrays because a the caller could change them after this returns
      setAuthorizations(authorizations.stream().map(auth -> Arrays.copyOf(auth, auth.length))
          .collect(toUnmodifiableList()));
      return this;
    }

    @Override
    public ExecutionBuilder authorizations(Set<String> authorizations) {
      setAuthorizations(
          authorizations.stream().map(auth -> auth.getBytes(UTF_8)).collect(toUnmodifiableList()));
      return this;
    }

    @Override
    public ExecutionBuilder authorizations(String... authorizations) {
      setAuthorizations(Stream.of(authorizations).map(auth -> auth.getBytes(UTF_8))
          .collect(toUnmodifiableList()));
      return this;
    }

    @Override
    public ExecutionBuilder authorizations(AuthorizationChecker authorizationChecker) {
      if (authorizations != null) {
        throw new IllegalStateException("Cannot set checker and authorizations");
      }
      this.authorizationsChecker = authorizationChecker;
      return this;
    }

    @Override
    public ExecutionBuilder cacheSize(int cacheSize) {
      if (cacheSize < 0) {
        throw new IllegalArgumentException();
      }
      this.cacheSize = cacheSize;
      return this;
    }

    @Override
    public VisibilityArbiter build() {
      if (authorizations != null ^ authorizationsChecker == null) {
        throw new IllegalStateException();
      }

      VisibilityArbiter visibilityArbiter;
      if (authorizationsChecker != null) {
        visibilityArbiter = new VisibilityArbiterImpl(authorizationsChecker);
      } else {
        visibilityArbiter = new VisibilityArbiterImpl(authorizations);
      }

      if (cacheSize > 0) {
        visibilityArbiter = new CachingVisibilityArbiter(visibilityArbiter, cacheSize);
      }
      return visibilityArbiter;
    }

  }

  public static AuthorizationsBuilder builder() {
    return new BuilderImpl();
  }
}
