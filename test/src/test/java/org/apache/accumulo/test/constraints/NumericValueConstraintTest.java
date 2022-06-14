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
package org.apache.accumulo.test.constraints;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class NumericValueConstraintTest {

  private NumericValueConstraint nvc = new NumericValueConstraint();

  @Test
  public void testCheck() {
    Mutation goodMutation = new Mutation(new Text("r"));
    goodMutation.put("cf", "cq", "1234");
    assertNull(nvc.check(null, goodMutation));

    // Check that multiple bad mutations result in one violation only
    Mutation badMutation = new Mutation(new Text("r"));
    badMutation.put("cf", "cq", "foo1234");
    badMutation.put("cf2", "cq2", "foo1234");
    assertEquals(NumericValueConstraint.NON_NUMERIC_VALUE,
        nvc.check(null, badMutation).stream().collect(onlyElement()).shortValue());
  }

  @Test
  public void testGetViolationDescription() {
    assertEquals(NumericValueConstraint.VIOLATION_MESSAGE,
        nvc.getViolationDescription(NumericValueConstraint.NON_NUMERIC_VALUE));
    assertNull(nvc.getViolationDescription((short) 2));
  }
}
