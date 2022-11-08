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
package org.apache.accumulo.core.data.constraints;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class DefaultKeySizeConstraintTest {

  Constraint constraint = new DefaultKeySizeConstraint();

  final private byte[] oversized = new byte[1048577];
  final private byte[] large = new byte[419430];

  @Test
  public void testConstraint() {
    // pass constraints
    Mutation m = new Mutation("rowId");
    m.put("colf", "colq", new Value());
    assertEquals(Collections.emptyList(), constraint.check(null, m));

    // test with row id > 1mb
    m = new Mutation(oversized);
    m.put("colf", "colq", new Value());
    assertEquals(
        Collections.singletonList(DefaultKeySizeConstraint.MAX__KEY_SIZE_EXCEEDED_VIOLATION),
        constraint.check(null, m));

    // test with colf > 1mb
    m = new Mutation("rowid");
    m.put(new Text(oversized), new Text("colq"), new Value());
    assertEquals(
        Collections.singletonList(DefaultKeySizeConstraint.MAX__KEY_SIZE_EXCEEDED_VIOLATION),
        constraint.check(null, m));

    // test with colf > 1mb
    m = new Mutation("rowid");
    m.put(new Text(oversized), new Text("colq"), new Value());
    assertEquals(
        Collections.singletonList(DefaultKeySizeConstraint.MAX__KEY_SIZE_EXCEEDED_VIOLATION),
        constraint.check(null, m));

    // test sum of smaller sizes violates 1mb constraint
    m = new Mutation(large);
    m.put(new Text(large), new Text(large), new Value());
    assertEquals(
        Collections.singletonList(DefaultKeySizeConstraint.MAX__KEY_SIZE_EXCEEDED_VIOLATION),
        constraint.check(null, m));
  }
}
