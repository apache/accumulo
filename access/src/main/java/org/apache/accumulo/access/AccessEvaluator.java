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
 * System.out.println(evaluator.canAccess("ALPHA&BETA"));
 * System.out.println(evaluator.canAccess("(ALPHA|BETA)&(OMEGA|EPSILON)"));
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
  boolean canAccess(String accessExpression) throws IllegalAccessExpressionException;

  boolean canAccess(byte[] accessExpression) throws IllegalAccessExpressionException;

  /**
   * TODO documnet that may be more efficient
   */
  boolean canAccess(AccessExpression accessExpression) throws IllegalAccessExpressionException;

  /**
   * @since ???
   */
  interface Authorizer {
    boolean isAuthorized(String auth);
  }

  interface AuthorizationsBuilder {

    ExecutionBuilder authorizations(Authorizations authorizations);

    /**
     * Allows providing multiple sets of authorizations. Each expression will be evaluated
     * independently against each set of authorizations and will only be deemed accessible if
     * accessible for all. For example the following code would print false, true, and then false.
     *
     * <pre>
     *     {@code
     * Collection<Authorizations> authSets =
     *     List.of(Authorizations.of("A", "B"), Authorizations.of("C", "D"));
     * var evaluator = AccessEvaluator.builder().authorizations(authSets).build();
     *
     * System.out.println(evaluator.canAccess("A"));
     * System.out.println(evaluator.canAccess("A|D"));
     * System.out.println(evaluator.canAccess("A&D"));
     *
     * }
     * </pre>
     *
     * <p>
     * The following table shows how each expression in the example above will evaluate for each
     * authorization set. In order to return true for {@code canAccess()} the expression must
     * evaluate to true for each authorization set.
     *
     * <table>
     * <caption>Evaluations</caption>
     * <tr>
     * <td></td>
     * <td>[A,B]</td>
     * <td>[C,D]</td>
     * </tr>
     * <tr>
     * <td>A</td>
     * <td>True</td>
     * <td>False</td>
     * </tr>
     * <tr>
     * <td>A|D</td>
     * <td>True</td>
     * <td>True</td>
     * </tr>
     * <tr>
     * <td>A&amp;D</td>
     * <td>False</td>
     * <td>False</td>
     * </tr>
     *
     * </table>
     *
     *
     *
     */
    ExecutionBuilder authorizations(Collection<Authorizations> authorizations);

    ExecutionBuilder authorizations(String... authorizations);

    ExecutionBuilder authorizations(Authorizer authorizer);
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
