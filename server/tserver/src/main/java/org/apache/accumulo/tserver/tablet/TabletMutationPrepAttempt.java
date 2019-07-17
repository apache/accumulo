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
package org.apache.accumulo.tserver.tablet;

import java.util.List;

import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.Mutation;

public class TabletMutationPrepAttempt {

  private Violations violations;
  private List<Mutation> violators;
  private List<Mutation> nonViolators;
  private CommitSession commitSession;
  private boolean attemptedTabletPrep;

  /**
   * Return true if an attempt was made to update the writes in progress for the tablet and to
   * retrieve the commit session from the tablet's memory, or false otherwise.
   */
  public boolean attemptedTabletPrep() {
    return attemptedTabletPrep;
  }

  /**
   * Return true if this attempt has a non-null commit session. If null, it indicates that either
   * there was never an attempt made to get the commit session from the tablet's memory, or that
   * either the tablet has a closed state or the tablet's memory is unavailable.
   */
  public boolean hasCommitSession() {
    return commitSession != null;
  }

  /**
   * Retrieve the commit session. Possibly null.
   */
  public CommitSession getCommitSession() {
    return commitSession;
  }

  /**
   * Retrieve the constraint violations found across the mutations. Possibly null.
   */
  public Violations getViolations() {
    return violations;
  }

  /**
   * Return true if at least one constraint violation was found, or false otherwise.
   */
  public boolean hasViolations() {
    return violations != null && !violations.isEmpty();
  }

  /**
   * Return the list of mutations that violated a constraint. Possibly null.
   */
  public List<Mutation> getViolators() {
    return violators;
  }

  /**
   * Return the list of mutations that did not violate any constraints. Possibly null.
   */
  public List<Mutation> getNonViolators() {
    return nonViolators;
  }

  /**
   * Return true if at least one mutation that did not violate any constraints was found.
   */
  public boolean hasNonViolators() {
    return nonViolators != null && !nonViolators.isEmpty();
  }

  /**
   * Set the commit session that was retrieved from the tablet's memory, and set
   * {@link #attemptedTabletPrep} to true.
   */
  void setCommitSession(final CommitSession commitSession) {
    this.commitSession = commitSession;
    attemptedTabletPrep = true;
  }

  /**
   * Set the constraint violations that were found.
   */
  void setViolations(final Violations violations) {
    this.violations = violations;
  }

  /**
   * Set the list of mutations that violated constraints.
   */
  void setViolators(final List<Mutation> violators) {
    this.violators = violators;
  }

  /**
   * Set the list of mutations that did not violate any constraints.
   */
  void setNonViolators(final List<Mutation> nonViolators) {
    this.nonViolators = nonViolators;
  }
}
