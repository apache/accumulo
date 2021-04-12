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
package org.apache.accumulo.core.data.constraints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

public class NoDeleteConstraintTest {

  @Test
  public void testConstraint() {
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
}
