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

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

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
 * System.out.println(evaluator.isVisible("ALPHA&BETA"));
 * System.out.println(evaluator.isVisible("(ALPHA|BETA)&(OMEGA|EPSILON)"));
 * }
 * </pre>
 *
 *
 * @since 1.0
 */
// TODO is there a better name? maybe use VisibilityEvaluator even though its a reuse of an existing
// type name in another accumulo package.
public interface VisibilityArbiter {
  /**
   * @return true if the expression is visible using the authorizations supplied at creation, false
   *         otherwise
   * @throws IllegalArgumentException when the expression is not valid
   */
  boolean isVisible(String expression) throws IllegalArgumentException;

  boolean isVisible(byte[] expression) throws IllegalArgumentException;

  boolean isVisible(VisibilityExpression expression) throws IllegalArgumentException;

  // TODO decide if Charsequence should be used in API or not

  interface AuthorizationChecker {
    boolean isAuthorized(byte[] auth);
  }

  interface BytesPredicate extends Predicate<byte[]> {}

  interface AuthorizationsBuilder {

    ExecutionBuilder authorizations(List<byte[]> authorizations);

    ExecutionBuilder authorizations(Set<String> authorizations);

    ExecutionBuilder authorizations(String... authorizations);

    // TODO document utf-8 expected
    ExecutionBuilder authorizations(byte[]... authorizations);

    ExecutionBuilder authorizations(AuthorizationChecker authorizationChecker);
  }

  interface ExecutionBuilder extends FinalBuilder {
    ExecutionBuilder cacheSize(int cacheSize);
  }

  interface FinalBuilder {
    VisibilityArbiter build();
  }

  static AuthorizationsBuilder builder() {
    return VisibilityArbiterImpl.builder();
  }
}
