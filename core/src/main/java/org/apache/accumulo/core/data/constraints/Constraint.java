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
package org.apache.accumulo.core.data.constraints;

import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.security.AuthorizationContainer;

/**
 * Constraint objects are used to determine if mutations will be applied to a table.
 *
 * <p>
 * This interface expects implementers to return violation codes. The reason codes are returned
 * instead of arbitrary strings to encourage conciseness. Conciseness is needed because violations
 * are aggregated. If a user sends a batch of 10,000 mutations to Accumulo, only aggregated counts
 * about which violations occurred are returned. If the constraint implementer were allowed to
 * return arbitrary violation strings like the following:
 *
 * <p>
 * Value "abc" is not a number<br>
 * Value "vbg" is not a number
 *
 * <p>
 * This would not aggregate very well, because the same violation is represented with two different
 * strings.
 *
 * @since 2.1.0 Replaces interface in org.apache.accumulo.core.constraints package
 */
public interface Constraint {

  /**
   * The environment within which a constraint exists.
   *
   * @since 2.1.0
   */
  interface Environment {

    /**
     * Gets the tablet Id of the environment.
     *
     * @return TabletId
     */
    TabletId getTablet();

    /**
     * Gets the user within the environment.
     *
     * @return user
     */
    String getUser();

    /**
     * Gets the authorizations in the environment.
     *
     * @return authorizations
     */
    AuthorizationContainer getAuthorizationsContainer();
  }

  /**
   * Gets a short, one-sentence description of what a given violation code means.
   *
   * @param violationCode numeric violation code
   * @return matching violation description
   */
  String getViolationDescription(short violationCode);

  /**
   * Checks a mutation for constraint violations. If the mutation contains no violations, returns
   * null. Otherwise, returns a list of violation codes.
   *
   * Violation codes must be non-negative. Negative violation codes are reserved for system use.
   *
   * @param env constraint environment
   * @param mutation mutation to check
   * @return list of violation codes, or null if none
   */
  List<Short> check(Environment env, Mutation mutation);
}
