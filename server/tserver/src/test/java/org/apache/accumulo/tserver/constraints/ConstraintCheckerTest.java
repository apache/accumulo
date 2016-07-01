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
package org.apache.accumulo.tserver.constraints;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createMockBuilder;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.constraints.Constraint.Environment;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.hadoop.io.BinaryComparable;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class ConstraintCheckerTest {

  private ConstraintChecker cc;
  private ArrayList<Constraint> constraints;
  private Environment env;
  private KeyExtent extent;
  private Mutation m;

  @Before
  public void setup() throws NoSuchMethodException, SecurityException {
    cc = createMockBuilder(ConstraintChecker.class).addMockedMethod("getConstraints").createMock();
    constraints = new ArrayList<>();
    expect(cc.getConstraints()).andReturn(constraints);

    env = createMock(Environment.class);
    extent = createMock(KeyExtent.class);
    expect(env.getExtent()).andReturn(extent);

    m = createMock(Mutation.class);
  }

  private Constraint makeSuccessConstraint() {
    Constraint c = createMock(Constraint.class);
    expect(c.check(env, m)).andReturn(null); // no violations
    replay(c);
    return c;
  }

  private Constraint makeFailureConstraint() {
    Constraint c = createMock(Constraint.class);
    short code1 = 2;
    short code2 = 4;
    List<Short> vCodes = ImmutableList.of(code1, code2);
    expect(c.getViolationDescription(code1)).andReturn("ViolationCode1");
    expect(c.getViolationDescription(code2)).andReturn("ViolationCode2");
    expect(c.check(env, m)).andReturn(vCodes);
    replay(c);
    return c;
  }

  private void replayAll() {
    replay(extent);
    replay(env);
    replay(cc);
  }

  private Constraint makeExceptionConstraint() {
    Constraint c = createMock(Constraint.class);
    expect(c.check(env, m)).andThrow(new RuntimeException("some exception"));
    replay(c);
    return c;
  }

  @Test
  public void testCheckAllOK() {
    expect(extent.contains(anyObject(BinaryComparable.class))).andReturn(true);
    replayAll();
    constraints.add(makeSuccessConstraint());
    assertNull(cc.check(env, m));
  }

  @Test
  public void testCheckMutationOutsideKeyExtent() {
    expect(extent.contains(anyObject(BinaryComparable.class))).andReturn(false);
    replayAll();
    ConstraintViolationSummary cvs = Iterables.getOnlyElement(cc.check(env, m).asList());
    assertEquals(SystemConstraint.class.getName(), cvs.getConstrainClass());
  }

  @Test
  public void testCheckFailure() {
    expect(extent.contains(anyObject(BinaryComparable.class))).andReturn(true);
    replayAll();
    constraints.add(makeFailureConstraint());
    List<ConstraintViolationSummary> cvsList = cc.check(env, m).asList();
    assertEquals(2, cvsList.size());
    Set<String> violationDescs = new HashSet<>();
    for (ConstraintViolationSummary cvs : cvsList) {
      violationDescs.add(cvs.getViolationDescription());
    }
    assertEquals(ImmutableSet.of("ViolationCode1", "ViolationCode2"), violationDescs);
  }

  @Test
  public void testCheckException() {
    expect(extent.contains(anyObject(BinaryComparable.class))).andReturn(true);
    replayAll();
    constraints.add(makeExceptionConstraint());
    ConstraintViolationSummary cvs = Iterables.getOnlyElement(cc.check(env, m).asList());
    assertEquals("CONSTRAINT FAILED : threw some Exception", cvs.getViolationDescription());
  }
}
