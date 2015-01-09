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
package org.apache.accumulo.core.constraints;

import java.util.List;

import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.AuthorizationContainer;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Accumulo uses Constraint objects to determine if mutations will be applied to a table.
 *
 * This interface expects implementers to return violation codes. The reason codes are returned instead of arbitrary strings it to encourage conciseness.
 * Conciseness is needed because violations are aggregated. If a user sends a batch of 10,000 mutations to accumulo, only aggregated counts about which
 * violations occurred are returned.
 *
 * If the Constraint implementer was allowed to return arbitrary violation strings like the following :
 *
 * Value "abc" is not a number Value "vbg" is not a number
 *
 * Then this would not aggregate very well, because the same violation is represented with two different strings.
 *
 *
 *
 */

public interface Constraint {

  interface Environment {
    KeyExtent getExtent();

    String getUser();

    @Deprecated
    Authorizations getAuthorizations();

    AuthorizationContainer getAuthorizationsContainer();
  }

  /**
   * Implementers of this method should return a short one sentence description of what a given violation code means.
   *
   */

  String getViolationDescription(short violationCode);

  /**
   * Checks a mutation for constrain violations. If the mutation contains no violations, then the implementation should return null. Otherwise it should return
   * a list of violation codes.
   *
   * Violation codes must be non negative. Negative violation codes are reserved for system use.
   *
   */

  List<Short> check(Environment env, Mutation mutation);
}
