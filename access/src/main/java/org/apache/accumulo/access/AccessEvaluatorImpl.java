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
package org.apache.accumulo.access;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Collectors.toUnmodifiableList;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//this class is intentionally package private and should never be made public
class AccessEvaluatorImpl implements AccessEvaluator {
  private final Collection<Predicate<BytesWrapper>> authorizedPredicates;

  private AccessEvaluatorImpl(Authorizer authorizationChecker) {
    this.authorizedPredicates = List.of(auth -> authorizationChecker.isAuthorized(unescape(auth)));
  }

  public AccessEvaluatorImpl(Collection<List<byte[]>> authorizationSets) {
    authorizedPredicates = authorizationSets.stream()
        .map(authorizations -> authorizations.stream()
            .map(auth -> AccessEvaluatorImpl.escape(auth, false)).map(BytesWrapper::new)
            .collect(toSet()))
        .map(escapedAuths -> (Predicate<BytesWrapper>) escapedAuths::contains)
        .collect(Collectors.toList());
  }

  static String unescape(BytesWrapper auth) {
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

      return new String(unescapedCopy, UTF_8);
    } else {
      return auth.toString();
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
  public boolean canAccess(String expression) throws IllegalArgumentException {

    return evaluate(new AccessExpressionImpl(expression));

  }

  @Override
  public boolean canAccess(byte[] expression) throws IllegalArgumentException {

    return evaluate(new AccessExpressionImpl(expression));

  }

  @Override
  public boolean canAccess(AccessExpression expression) throws IllegalArgumentException {
    if (expression instanceof AccessExpressionImpl) {

      return evaluate((AccessExpressionImpl) expression);

    } else {
      return canAccess(expression.getExpression());
    }
  }

  public boolean evaluate(AccessExpressionImpl visibility) throws IllegalAccessExpressionException {
    // The VisibilityEvaluator computes a trie from the given Authorizations, that ColumnVisibility
    // expressions can be evaluated against.
    return authorizedPredicates.stream()
        .allMatch(ap -> evaluate(ap, visibility.getExpressionBytes(), visibility.getParseTree()));
  }

  private static boolean evaluate(Predicate<BytesWrapper> authorizedPredicate,
      final byte[] expression, final AccessExpressionImpl.Node root)
      throws IllegalAccessExpressionException {
    if (expression.length == 0) {
      return true;
    }
    switch (root.type) {
      case TERM:
        return authorizedPredicate.test(root.getTerm(expression));
      case AND:
        if (root.children == null || root.children.size() < 2) {
          throw new IllegalAccessExpressionException("AND has less than 2 children",
              root.getTerm(expression).toString(), root.start);
        }
        for (AccessExpressionImpl.Node child : root.children) {
          if (!evaluate(authorizedPredicate, expression, child)) {
            return false;
          }
        }
        return true;
      case OR:
        if (root.children == null || root.children.size() < 2) {
          throw new IllegalAccessExpressionException("OR has less than 2 children",
              root.getTerm(expression).toString(), root.start);
        }
        for (AccessExpressionImpl.Node child : root.children) {
          if (evaluate(authorizedPredicate, expression, child)) {
            return true;
          }
        }
        return false;
      default:
        throw new IllegalAccessExpressionException("No such node type",
            root.getTerm(expression).toString(), root.start);
    }
  }

  private static class BuilderImpl
      implements AuthorizationsBuilder, FinalBuilder, ExecutionBuilder {

    private Authorizer authorizationsChecker;

    private Collection<List<byte[]>> authorizationSets;
    private int cacheSize = 0;

    private void setAuthorizations(List<byte[]> auths) {
      setAuthorizations(Collections.singletonList(auths));
    }

    private void setAuthorizations(Collection<List<byte[]>> authSets) {
      if (authorizationsChecker != null) {
        throw new IllegalStateException("Cannot set checker and authorizations");
      }

      for (List<byte[]> auths : authSets) {
        for (byte[] auth : auths) {
          if (auth.length == 0) {
            throw new IllegalArgumentException("Empty authorization");
          }
        }
      }
      this.authorizationSets = authSets;
    }

    @Override
    public ExecutionBuilder authorizations(Authorizations authorizations) {
      setAuthorizations(authorizations.asSet().stream().map(auth -> auth.getBytes(UTF_8))
          .collect(toUnmodifiableList()));
      return this;
    }

    @Override
    public ExecutionBuilder authorizations(Collection<Authorizations> authorizationSets) {
      setAuthorizations(authorizationSets
          .stream().map(authorizations -> authorizations.asSet().stream()
              .map(auth -> auth.getBytes(UTF_8)).collect(toUnmodifiableList()))
          .collect(Collectors.toList()));
      return this;
    }

    @Override
    public ExecutionBuilder authorizations(String... authorizations) {
      setAuthorizations(Stream.of(authorizations).map(auth -> auth.getBytes(UTF_8))
          .collect(toUnmodifiableList()));
      return this;
    }

    @Override
    public ExecutionBuilder authorizations(Authorizer authorizationChecker) {
      if (authorizationSets != null) {
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
    public AccessEvaluator build() {
      if (authorizationSets != null ^ authorizationsChecker == null) {
        throw new IllegalStateException();
      }

      AccessEvaluator accessEvaluator;
      if (authorizationsChecker != null) {
        accessEvaluator = new AccessEvaluatorImpl(authorizationsChecker);
      } else {
        accessEvaluator = new AccessEvaluatorImpl(authorizationSets);
      }

      if (cacheSize > 0) {
        accessEvaluator = new CachingAccessEvaluator(accessEvaluator, cacheSize);
      }
      return accessEvaluator;
    }

  }

  public static AuthorizationsBuilder builder() {
    return new BuilderImpl();
  }
}
