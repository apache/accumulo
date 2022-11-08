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
package org.apache.accumulo.core.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ConstraintViolationSummaryTest {

  @Test
  public void testToString() {
    ConstraintViolationSummary cvs =
        new ConstraintViolationSummary("fooClass", (short) 1, "fooDescription", 100L);
    assertEquals("ConstraintViolationSummary(constrainClass:fooClass,"
        + " violationCode:1, violationDescription:fooDescription,"
        + " numberOfViolatingMutations:100)", cvs.toString());

    cvs = new ConstraintViolationSummary(null, (short) 2, null, 101L);
    assertEquals(
        "ConstraintViolationSummary(constrainClass:null,"
            + " violationCode:2, violationDescription:null, numberOfViolatingMutations:101)",
        cvs.toString());
  }
}
