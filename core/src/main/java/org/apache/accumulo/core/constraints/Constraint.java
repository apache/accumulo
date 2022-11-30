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
package org.apache.accumulo.core.constraints;

import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.security.AuthorizationContainer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class is replaced by {@link org.apache.accumulo.core.data.constraints.Constraint}
 *
 * @deprecated since 2.1.0 Use {@link org.apache.accumulo.core.data.constraints.Constraint}
 */
@Deprecated(since = "2.1.0")
@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_INTERFACE",
    justification = "Same name used for compatibility during deprecation cycle")
public interface Constraint extends org.apache.accumulo.core.data.constraints.Constraint {

  /**
   * The environment within which a constraint exists.
   */
  interface Environment extends org.apache.accumulo.core.data.constraints.Constraint.Environment {
    /**
     * Gets the key extent of the environment.
     *
     * @return key extent
     */
    KeyExtent getExtent();

    /**
     * Gets the user within the environment.
     *
     * @return user
     */
    @Override
    String getUser();

    /**
     * Gets the authorizations in the environment.
     *
     * @return authorizations
     */
    @Override
    AuthorizationContainer getAuthorizationsContainer();
  }

  /**
   * Gets a short, one-sentence description of what a given violation code means.
   *
   * @param violationCode numeric violation code
   * @return matching violation description
   */
  @Override
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

  /**
   * Implemented for backwards compatibility.
   *
   * @since 2.1.0
   */
  @Override
  default List<Short> check(org.apache.accumulo.core.data.constraints.Constraint.Environment env,
      Mutation mutation) {
    return check((Environment) env, mutation);
  }
}
