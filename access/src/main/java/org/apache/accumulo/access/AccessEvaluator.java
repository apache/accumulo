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

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * <p>
 * An implementation of the Accumulo visibility standard as specified in this document (TODO write
 * the document based on current Accumulo implementation and post somewhere).
 *
 * <p>
 * Below is an example that should print false and then print true.
 *
 * <pre>
 * {@code
 * var evaluator = VisibilityArbiter.builder().authorizations("ALPHA", "OMEGA").build();
 *
 * System.out.println(evaluator.isAccessible("ALPHA&BETA"));
 * System.out.println(evaluator.isAccessible("(ALPHA|BETA)&(OMEGA|EPSILON)"));
 * }
 * </pre>
 *
 *
 * @since ???
 */
public interface AccessEvaluator {
  /**
   * @return true if the expression is visible using the authorizations supplied at creation, false
   *         otherwise
   * @throws IllegalArgumentException when the expression is not valid
   */
  boolean isAccessible(String accessExpression) throws IllegalAccessExpressionException;

  boolean isAccessible(byte[] accessExpression) throws IllegalAccessExpressionException;

  /**
   * TODO documnet that may be more efficient
   */
  boolean isAccessible(AccessExpression accessExpression) throws IllegalAccessExpressionException;

  // TODO decide if Charsequence should be used in API or not

  /**
   * @since ???
   */
  interface AuthorizationChecker {
    boolean isAuthorized(byte[] auth);
  }

  interface AuthorizationsBuilder {

    // TODO document utf-8 expected
    ExecutionBuilder authorizations(List<byte[]> authorizations);

    ExecutionBuilder authorizations(Set<String> authorizations);

    ExecutionBuilder authorizations(Collection<Set<String>> authorizations);

    ExecutionBuilder authorizations(String... authorizations);

    ExecutionBuilder authorizations(AuthorizationChecker authorizationChecker);
  }

  interface ExecutionBuilder extends FinalBuilder {
    ExecutionBuilder cacheSize(int cacheSize);
  }

  interface FinalBuilder {
    AccessEvaluator build();
  }

  static AuthorizationsBuilder builder() {
    return AccessEvaluatorImpl.builder();
  }
}
