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
package org.apache.accumulo.test.constraints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class AlphaNumKeyConstraintTest {

  private AlphaNumKeyConstraint ankc = new AlphaNumKeyConstraint();

  @Test
  public void test() {
    Mutation goodMutation = new Mutation(new Text("Row1"));
    goodMutation.put(new Text("Colf2"), new Text("ColQ3"), new Value("value".getBytes()));
    assertNull(ankc.check(null, goodMutation));

    // Check that violations are in row, cf, cq order
    Mutation badMutation = new Mutation(new Text("Row#1"));
    badMutation.put(new Text("Colf$2"), new Text("Colq%3"), new Value("value".getBytes()));
    assertEquals(ImmutableList.of(AlphaNumKeyConstraint.NON_ALPHA_NUM_ROW, AlphaNumKeyConstraint.NON_ALPHA_NUM_COLF, AlphaNumKeyConstraint.NON_ALPHA_NUM_COLQ),
        ankc.check(null, badMutation));
  }

  @Test
  public void testGetViolationDescription() {
    assertEquals(AlphaNumKeyConstraint.ROW_VIOLATION_MESSAGE, ankc.getViolationDescription(AlphaNumKeyConstraint.NON_ALPHA_NUM_ROW));
    assertEquals(AlphaNumKeyConstraint.COLF_VIOLATION_MESSAGE, ankc.getViolationDescription(AlphaNumKeyConstraint.NON_ALPHA_NUM_COLF));
    assertEquals(AlphaNumKeyConstraint.COLQ_VIOLATION_MESSAGE, ankc.getViolationDescription(AlphaNumKeyConstraint.NON_ALPHA_NUM_COLQ));
    assertNull(ankc.getViolationDescription((short) 4));
  }
}
