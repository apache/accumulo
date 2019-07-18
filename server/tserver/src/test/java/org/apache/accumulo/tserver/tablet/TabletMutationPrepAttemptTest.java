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

import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Mutation;
import org.junit.Test;

public class TabletMutationPrepAttemptTest {

  @Test
  public void attemptedTabletPrep_byDefault_returnsFalse() {
    TabletMutationPrepAttempt attempt = new TabletMutationPrepAttempt();
    assertFalse(attempt.attemptedTabletPrep());
  }

  @Test
  public void attemptedTabletPrep_givenNullCommitSessionSet_returnsTrue() {
    TabletMutationPrepAttempt attempt = new TabletMutationPrepAttempt();
    attempt.setCommitSession(null);
    assertTrue(attempt.attemptedTabletPrep());
  }

  @Test
  public void attemptedTabletPrep_givenNonNullCommitSessionSet_returnsTrue() {
    TabletMutationPrepAttempt attempt = new TabletMutationPrepAttempt();
    attempt.setCommitSession(mock(CommitSession.class));
    assertTrue(attempt.attemptedTabletPrep());
  }

  @Test
  public void hasViolations_givenEmptyViolations_returnsTrue() {
    TabletMutationPrepAttempt attempt = new TabletMutationPrepAttempt();
    assertFalse(attempt.hasViolations());
  }

  @Test
  public void hasViolations_givenViolator_returnsTrue() {
    Violations violations = new Violations();
    violations.add(new ConstraintViolationSummary("clazz", (short) 1, "desc", 1));
    Mutation mutation = mock(Mutation.class);
    TabletMutationPrepAttempt attempt = new TabletMutationPrepAttempt();
    attempt.addViolator(mutation, violations);

    assertTrue(attempt.hasViolations());
    assertFalse(attempt.getViolators().isEmpty());
  }

  @Test
  public void hasNonViolators_byDefault_returnsFalse() {
    TabletMutationPrepAttempt attempt = new TabletMutationPrepAttempt();
    assertFalse(attempt.hasNonViolators());
  }

  @Test
  public void hasNonViolator_givenNonViolator_returnsTrue() {
    TabletMutationPrepAttempt attempt = new TabletMutationPrepAttempt();
    attempt.addNonViolator(mock(Mutation.class));
    assertTrue(attempt.hasNonViolators());
    assertFalse(attempt.getNonViolators().isEmpty());

  }
}
