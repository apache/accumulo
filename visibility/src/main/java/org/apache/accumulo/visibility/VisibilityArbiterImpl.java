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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//this class is intentionally package private and should never be made public
class VisibilityArbiterImpl implements VisibilityArbiter {
  private final VisibilityEvaluator visibilityEvaluator;

  private VisibilityArbiterImpl(AuthorizationChecker authorizations) {
    // TODO will probably be more efficient to pass in a set as that would avoid the unescaping
    this.visibilityEvaluator = new VisibilityEvaluator(new AuthorizationContainer() {
      @Override
      public boolean contains(ByteSequence auth) {
        return authorizations.isAuthorized(auth.toArray());
      }
    });
  }

  @Override
  public boolean isVisible(String expression) throws IllegalArgumentException {
    try {
      return visibilityEvaluator.evaluate(new VisibilityExpressionImpl(expression));
    } catch (VisibilityParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public boolean isVisible(byte[] expression) throws IllegalArgumentException {
    try {
      return visibilityEvaluator.evaluate(new VisibilityExpressionImpl(expression));
    } catch (VisibilityParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public boolean isVisible(VisibilityExpression expression) throws IllegalArgumentException {
    if (expression instanceof VisibilityExpressionImpl) {
      try {
        return visibilityEvaluator.evaluate((VisibilityExpressionImpl) expression);
      } catch (VisibilityParseException e) {
        throw new IllegalArgumentException(e);
      }
    } else {
      return isVisible(expression.getExpression());
    }
  }

  private static class BuilderImpl
      implements AuthorizationsBuilder, FinalBuilder, ExecutionBuilder {

    private AuthorizationChecker authorizations;
    private int cacheSize = 0;

    @Override
    public ExecutionBuilder authorizations(List<byte[]> authorizations) {
      // TODO maybe keep as byte array?

      // TODO may want to copy byte arrays
      var authsSet =
          authorizations.stream().map(ArrayByteSequence::new).collect(Collectors.toSet());
      this.authorizations = auth -> authsSet.contains(new ArrayByteSequence(auth));
      return this;
    }

    @Override
    public ExecutionBuilder authorizations(Set<String> authorizations) {
      var authsSet =
          authorizations.stream().map(ArrayByteSequence::new).collect(Collectors.toSet());
      this.authorizations = auth -> authsSet.contains(new ArrayByteSequence(auth));
      return this;
    }

    @Override
    public ExecutionBuilder authorizations(String... authorizations) {
      var authsSet =
          Stream.of(authorizations).map(ArrayByteSequence::new).collect(Collectors.toSet());
      this.authorizations = auth -> authsSet.contains(new ArrayByteSequence(auth));
      return this;
    }

    @Override
    public ExecutionBuilder authorizations(byte[]... authorizations) {
      return authorizations(Arrays.asList(authorizations));
    }

    @Override
    public ExecutionBuilder authorizations(AuthorizationChecker authorizationChecker) {
      this.authorizations = authorizationChecker;
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
      VisibilityArbiter visibilityArbiter = new VisibilityArbiterImpl(authorizations);
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
