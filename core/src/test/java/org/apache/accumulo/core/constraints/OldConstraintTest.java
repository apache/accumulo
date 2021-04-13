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
package org.apache.accumulo.core.constraints;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.AuthorizationContainer;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * This test is a consolidation of the old Constraint Tests. Tests the deprecated
 * org.apache.accumulo.core.constraints.Constraint
 */
@SuppressWarnings("deprecation")
public class OldConstraintTest {
  private static final List<Short> NO_VIOLATIONS = new ArrayList<>();

  @Test
  public void noDeleteConstraintTest() {
    Mutation m1 = new Mutation("r1");
    m1.putDelete("f1", "q1");

    NoDeleteConstraint ndc = new NoDeleteConstraint();

    List<Short> results = ndc.check(null, m1);
    assertEquals(1, results.size());
    assertEquals(1, results.get(0).intValue());

    Mutation m2 = new Mutation("r1");
    m2.put("f1", "q1", new Value("v1"));

    results = ndc.check(null, m2);
    assertNull(results);
  }

  private static class NoDeleteConstraint implements Constraint {

    @Override
    public String getViolationDescription(short violationCode) {
      if (violationCode == 1) {
        return "Deletes are not allowed";
      }
      return null;
    }

    @Override
    public List<Short> check(Environment env, Mutation mutation) {
      List<ColumnUpdate> updates = mutation.getUpdates();
      for (ColumnUpdate update : updates) {
        if (update.isDeleted()) {
          return Collections.singletonList((short) 1);
        }
      }
      return null;
    }
  }

  @Test
  public void testDefaultKeySizeConstraint() {
    Constraint constraint = new DefaultKeySizeConstraint();
    byte[] oversized = new byte[1048577];
    byte[] large = new byte[419430];

    // pass constraints
    Mutation m = new Mutation("rowId");
    m.put("colf", "colq", new Value(new byte[] {}));
    assertEquals(Collections.emptyList(), constraint.check(null, m));

    // test with row id > 1mb
    m = new Mutation(oversized);
    m.put("colf", "colq", new Value(new byte[] {}));
    assertEquals(
        Collections.singletonList(DefaultKeySizeConstraint.MAX__KEY_SIZE_EXCEEDED_VIOLATION),
        constraint.check(null, m));

    // test with colf > 1mb
    m = new Mutation("rowid");
    m.put(new Text(oversized), new Text("colq"), new Value(new byte[] {}));
    assertEquals(
        Collections.singletonList(DefaultKeySizeConstraint.MAX__KEY_SIZE_EXCEEDED_VIOLATION),
        constraint.check(null, m));

    // test with colf > 1mb
    m = new Mutation("rowid");
    m.put(new Text(oversized), new Text("colq"), new Value(new byte[] {}));
    assertEquals(
        Collections.singletonList(DefaultKeySizeConstraint.MAX__KEY_SIZE_EXCEEDED_VIOLATION),
        constraint.check(null, m));

    // test sum of smaller sizes violates 1mb constraint
    m = new Mutation(large);
    m.put(new Text(large), new Text(large), new Value(new byte[] {}));
    assertEquals(
        Collections.singletonList(DefaultKeySizeConstraint.MAX__KEY_SIZE_EXCEEDED_VIOLATION),
        constraint.check(null, m));
  }

  private static class DefaultKeySizeConstraint implements Constraint {
    protected static final short MAX__KEY_SIZE_EXCEEDED_VIOLATION = 1;
    protected static final long maxSize = 1048576; // 1MB default size

    @Override
    public String getViolationDescription(short violationCode) {
      switch (violationCode) {
        case MAX__KEY_SIZE_EXCEEDED_VIOLATION:
          return "Key was larger than 1MB";
      }
      return null;
    }

    @Override
    public List<Short> check(Environment env, Mutation mutation) {
      // fast size check
      if (mutation.numBytes() < maxSize)
        return NO_VIOLATIONS;

      List<Short> violations = new ArrayList<>();
      for (ColumnUpdate cu : mutation.getUpdates()) {
        int size = mutation.getRow().length;
        size += cu.getColumnFamily().length;
        size += cu.getColumnQualifier().length;
        size += cu.getColumnVisibility().length;

        if (size > maxSize)
          violations.add(MAX__KEY_SIZE_EXCEEDED_VIOLATION);
      }
      return violations;
    }
  }

  VisibilityConstraint vc;
  Constraint.Environment env;
  Mutation mutation;

  static final ColumnVisibility good = new ColumnVisibility("good");
  static final ColumnVisibility bad = new ColumnVisibility("bad");

  static final String D = "don't care";

  static final List<Short> ENOAUTH = Arrays.asList((short) 2);

  private void setUp() {
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
    setUp();
    mutation.put(D, D, D);
    assertNull("authorized", vc.check(env, mutation));
  }

  @Test
  public void testVisibilityNoAuth() {
    setUp();
    mutation.put(D, D, bad, D);
    assertEquals("unauthorized", ENOAUTH, vc.check(env, mutation));
  }

  @Test
  public void testGoodVisibilityAuth() {
    setUp();
    mutation.put(D, D, good, D);
    assertNull("authorized", vc.check(env, mutation));
  }

  @Test
  public void testCachedVisibilities() {
    setUp();
    mutation.put(D, D, good, "v");
    mutation.put(D, D, good, "v2");
    assertNull("authorized", vc.check(env, mutation));
  }

  @Test
  public void testMixedVisibilities() {
    setUp();
    mutation.put(D, D, bad, D);
    mutation.put(D, D, good, D);
    assertEquals("unauthorized", ENOAUTH, vc.check(env, mutation));
  }

  private static class VisibilityConstraint implements Constraint {

    @Override
    public String getViolationDescription(short violationCode) {
      switch (violationCode) {
        case 1:
          return "Malformed column visibility";
        case 2:
          return "User does not have authorization on column visibility";
      }
      return null;
    }

    @Override
    public List<Short> check(Environment env, Mutation mutation) {
      List<ColumnUpdate> updates = mutation.getUpdates();
      HashSet<String> ok = null;
      if (updates.size() > 1)
        ok = new HashSet<>();

      VisibilityEvaluator ve = null;
      for (ColumnUpdate update : updates) {
        byte[] cv = update.getColumnVisibility();
        if (cv.length > 0) {
          String key = null;
          if (ok != null && ok.contains(key = new String(cv, UTF_8)))
            continue;
          try {
            if (ve == null)
              ve = new VisibilityEvaluator(env.getAuthorizationsContainer());
            if (!ve.evaluate(new ColumnVisibility(cv)))
              return Collections.singletonList((short) 2);

          } catch (BadArgumentException | VisibilityParseException bae) {
            return Collections.singletonList((short) 1);
          }

          if (ok != null)
            ok.add(key);
        }
      }
      return null;
    }
  }
}
