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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.AuthorizationContainer;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class VisibilityConstraintTest {

  VisibilityConstraint vc;
  Constraint.Environment env;
  Mutation mutation;

  static final ColumnVisibility good = new ColumnVisibility("good");
  static final ColumnVisibility bad = new ColumnVisibility("bad");

  static final String D = "don't care";

  static final List<Short> ENOAUTH = Arrays.asList((short) 2);

  @BeforeEach
  public void setUp() {
    vc = new VisibilityConstraint();
    mutation = new Mutation("r");

    ArrayByteSequence bs = new ArrayByteSequence("good".getBytes(UTF_8));

    AuthorizationContainer ac = createNiceMock(AuthorizationContainer.class);
    expect(ac.contains(bs)).andReturn(true);
    replay(ac);

    env = createMock(Constraint.Environment.class);
    expect(env.getAuthorizationsContainer()).andReturn(ac);
    replay(env);
  }

  @Test
  public void testNoVisibility() {
    mutation.put(D, D, D);
    assertNull(vc.check(env, mutation), "authorized");
  }

  @Test
  public void testVisibilityNoAuth() {
    mutation.put(D, D, bad, D);
    assertEquals(ENOAUTH, vc.check(env, mutation), "unauthorized");
  }

  @Test
  public void testGoodVisibilityAuth() {
    mutation.put(D, D, good, D);
    assertNull(vc.check(env, mutation), "authorized");
  }

  @Test
  public void testCachedVisibilities() {
    mutation.put(D, D, good, "v");
    mutation.put(D, D, good, "v2");
    assertNull(vc.check(env, mutation), "authorized");
  }

  @Test
  public void testMixedVisibilities() {
    mutation.put(D, D, bad, D);
    mutation.put(D, D, good, D);
    assertEquals(ENOAUTH, vc.check(env, mutation), "unauthorized");
  }

}
