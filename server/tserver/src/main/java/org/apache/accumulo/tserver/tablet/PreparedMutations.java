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
package org.apache.accumulo.tserver.tablet;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.Mutation;

import com.google.common.base.Preconditions;

public class PreparedMutations {

  private final Violations violations;
  private final Collection<Mutation> violators;
  private final List<Mutation> nonViolators;
  private final CommitSession commitSession;
  private final boolean tabletClosed;

  /**
   * This constructor is used to communicate the tablet was closed.
   */
  public PreparedMutations() {
    this.tabletClosed = true;
    this.violations = null;
    this.violators = null;
    this.nonViolators = null;
    this.commitSession = null;
  }

  public PreparedMutations(CommitSession cs, List<Mutation> nonViolators, Violations violations,
      Set<Mutation> violators) {
    this.tabletClosed = false;
    this.nonViolators = Objects.requireNonNull(nonViolators);
    this.violators = Objects.requireNonNull(violators);
    this.violations = Objects.requireNonNull(violations);
    if (cs == null) {
      Preconditions.checkArgument(nonViolators.isEmpty());
    }
    this.commitSession = cs;
  }

  /**
   * Return true if the tablet was closed. When this is the case no other methods can be called.
   */
  public boolean tabletClosed() {
    return tabletClosed;
  }

  /**
   * Retrieve the commit session. May be null.
   */
  public CommitSession getCommitSession() {
    Preconditions.checkState(!tabletClosed);
    return commitSession;
  }

  /**
   * Retrieve the constraint violations found across the mutations.
   */
  public Violations getViolations() {
    Preconditions.checkState(!tabletClosed);
    return violations;
  }

  /**
   * Return the list of mutations that violated a constraint.
   */
  public Collection<Mutation> getViolators() {
    Preconditions.checkState(!tabletClosed);
    return violators;
  }

  /**
   * Return the list of mutations that did not violate any constraints.
   */
  public List<Mutation> getNonViolators() {
    Preconditions.checkState(!tabletClosed);
    return nonViolators;
  }
}
