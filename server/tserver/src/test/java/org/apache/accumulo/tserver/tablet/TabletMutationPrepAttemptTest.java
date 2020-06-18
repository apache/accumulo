/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.tablet;

import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.Mutation;
import org.junit.Test;

public class TabletMutationPrepAttemptTest {

  public void ensureTabletClosed() {
    PreparedMutations prepared = new PreparedMutations();
    assertTrue(prepared.tabletClosed());
  }

  @Test(expected = IllegalStateException.class)
  public void callGetSessionWhenClosed() {
    PreparedMutations prepared = new PreparedMutations();
    prepared.getCommitSession();
  }

  @Test(expected = IllegalStateException.class)
  public void callGetNonViolatorsWhenClosed() {
    PreparedMutations prepared = new PreparedMutations();
    prepared.getNonViolators();
  }

  @Test(expected = IllegalStateException.class)
  public void callGetViolatorsWhenClosed() {
    PreparedMutations prepared = new PreparedMutations();
    prepared.getViolators();
  }

  @Test(expected = IllegalStateException.class)
  public void callGetViolationsWhenClosed() {
    PreparedMutations prepared = new PreparedMutations();
    prepared.getViolations();
  }

  public void testTabletOpen() {
    CommitSession cs = mock(CommitSession.class);
    List<Mutation> nonViolators = new ArrayList<>();
    Violations violations = new Violations();
    Set<Mutation> violators = new HashSet<>();

    PreparedMutations prepared = new PreparedMutations(cs, nonViolators, violations, violators);

    assertFalse(prepared.tabletClosed());
    assertSame(cs, prepared.getCommitSession());
    assertSame(nonViolators, prepared.getNonViolators());
    assertSame(violations, prepared.getViolations());
    assertSame(violators, prepared.getNonViolators());
  }
}
