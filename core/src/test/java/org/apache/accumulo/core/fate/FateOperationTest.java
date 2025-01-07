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
package org.apache.accumulo.core.fate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.manager.thrift.TFateOperation;
import org.junit.jupiter.api.Test;

public class FateOperationTest {

  @Test
  public void testFateOperation() {
    // ensures that all TFateOperation have an equivalent FateOperation
    assertTrue(TFateOperation.values().length > 0);
    for (var top : TFateOperation.values()) {
      assertEquals(top, Fate.FateOperation.fromThrift(top).toThrift());
    }
    // ensures that all FateOperation are valid: either specified not to have an equivalent thrift
    // form or do have an equivalent thrift form
    assertTrue(Fate.FateOperation.values().length > 0);
    for (var op : Fate.FateOperation.values()) {
      if (Fate.FateOperation.getNonThriftOps().contains(op)) {
        assertThrows(IllegalStateException.class, op::toThrift);
      } else {
        assertEquals(op, Fate.FateOperation.fromThrift(op.toThrift()));
      }
    }
  }
}
