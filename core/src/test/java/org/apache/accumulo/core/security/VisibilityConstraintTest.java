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
package org.apache.accumulo.core.security;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.constraints.Constraint.Environment;
import org.apache.accumulo.core.constraints.VisibilityConstraint;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.Mutation;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class VisibilityConstraintTest {

  VisibilityConstraint vc;
  Environment env;
  Mutation mutation;

  static final ColumnVisibility good = new ColumnVisibility("good");
  static final ColumnVisibility bad = new ColumnVisibility("bad");

  static final String D = "don't care";

  static final List<Short> ENOAUTH = Arrays.asList((short) 2);

  @Before
  public void setUp() throws Exception {
    vc = new VisibilityConstraint();
    mutation = new Mutation("r");

    ArrayByteSequence bs = new ArrayByteSequence("good".getBytes(UTF_8));

    AuthorizationContainer ac = createNiceMock(AuthorizationContainer.class);
    expect(ac.contains(bs)).andReturn(true);
    replay(ac);

    env = createMock(Environment.class);
    expect(env.getAuthorizationsContainer()).andReturn(ac);
    replay(env);
  }

  @Test
  public void testNoVisibility() {
    mutation.put(D, D, D);
    assertNull("authorized", vc.check(env, mutation));
  }

  @Test
  public void testVisibilityNoAuth() {
    mutation.put(D, D, bad, D);
    assertEquals("unauthorized", ENOAUTH, vc.check(env, mutation));
  }

  @Test
  public void testGoodVisibilityAuth() {
    mutation.put(D, D, good, D);
    assertNull("authorized", vc.check(env, mutation));
  }

  @Test
  public void testCachedVisibilities() {
    mutation.put(D, D, good, "v");
    mutation.put(D, D, good, "v2");
    assertNull("authorized", vc.check(env, mutation));
  }

  @Test
  public void testMixedVisibilities() {
    mutation.put(D, D, bad, D);
    mutation.put(D, D, good, D);
    assertEquals("unauthorized", ENOAUTH, vc.check(env, mutation));
  }

  @Test
  @Ignore
  public void testMalformedVisibility() {
    // TODO: ACCUMULO-1006 Should test for returning error code 1, but not sure how since ColumnVisibility won't let us construct a bad one in the first place
  }
}
