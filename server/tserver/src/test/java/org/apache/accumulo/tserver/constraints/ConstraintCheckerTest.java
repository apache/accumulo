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
package org.apache.accumulo.tserver.constraints;

import static java.nio.charset.StandardCharsets.UTF_8;
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

import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.data.constraints.Constraint.Environment;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class ConstraintCheckerTest {

  private ConstraintChecker cc;
  private ArrayList<Constraint> constraints;
  private Environment env;
  private TabletId tabletId;
  private Mutation m;
  private Mutation m2;

  @Before
  public void setup() throws SecurityException {
    cc = createMockBuilder(ConstraintChecker.class).addMockedMethod("getConstraints").createMock();
    constraints = new ArrayList<>();
    expect(cc.getConstraints()).andReturn(constraints);

    env = createMock(Environment.class);
    tabletId = createMock(TabletId.class);
    expect(env.getTablet()).andReturn(tabletId);

    m = createMock(Mutation.class);
    m2 = createMock(Mutation.class);
    expect(tabletId.getEndRow()).andReturn(new Text("d")).anyTimes();
    expect(tabletId.getPrevEndRow()).andReturn(new Text("a")).anyTimes();
    expect(m.getRow()).andReturn("b".getBytes(UTF_8)).anyTimes();
    expect(m2.getRow()).andReturn("z".getBytes(UTF_8)).anyTimes();
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
    List<Short> vCodes = List.of(code1, code2);
    expect(c.getViolationDescription(code1)).andReturn("ViolationCode1");
    expect(c.getViolationDescription(code2)).andReturn("ViolationCode2");
    expect(c.check(env, m)).andReturn(vCodes);
    replay(c);
    return c;
  }

  private void replayAll() {
    replay(tabletId);
    replay(env);
    replay(cc);
    replay(m);
  }

  private Constraint makeExceptionConstraint() {
    Constraint c = createMock(Constraint.class);
    expect(c.check(env, m)).andThrow(new RuntimeException("some exception"));
    replay(c);
    return c;
  }

  @Test
  public void testCheckAllOK() {
    replayAll();
    constraints.add(makeSuccessConstraint());
    assertNull(cc.check(env, m));
  }

  @Test
  public void testCheckMutationOutsideTablet() {
    replayAll();
    replay(m2);
    ConstraintViolationSummary cvs = Iterables.getOnlyElement(cc.check(env, m2).asList());
    assertEquals(SystemConstraint.class.getName(), cvs.getConstrainClass());
  }

  @Test
  public void testCheckFailure() {
    replayAll();
    constraints.add(makeFailureConstraint());
    List<ConstraintViolationSummary> cvsList = cc.check(env, m).asList();
    assertEquals(2, cvsList.size());
    Set<String> violationDescs = new HashSet<>();
    for (ConstraintViolationSummary cvs : cvsList) {
      violationDescs.add(cvs.getViolationDescription());
    }
    assertEquals(Set.of("ViolationCode1", "ViolationCode2"), violationDescs);
  }

  @Test
  public void testCheckException() {
    replayAll();
    constraints.add(makeExceptionConstraint());
    ConstraintViolationSummary cvs = Iterables.getOnlyElement(cc.check(env, m).asList());
    assertEquals("CONSTRAINT FAILED : threw some Exception", cvs.getViolationDescription());
  }
}
