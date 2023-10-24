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
package org.apache.accumulo.manager.tableOps.merge;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.util.Pair;
import org.junit.jupiter.api.Test;

public class DeleteRowsTest {

  @Test
  public void testComputeNewDfvEven() {
    DataFileValue original = new DataFileValue(20, 10, 100);
    Pair<DataFileValue,DataFileValue> newValues = DeleteRows.computeNewDfv(original);

    assertEquals(10, newValues.getFirst().getSize());
    assertEquals(5, newValues.getFirst().getNumEntries());
    assertEquals(original.getTime(), newValues.getFirst().getTime());
    assertEquals(10, newValues.getSecond().getSize());
    assertEquals(5, newValues.getSecond().getNumEntries());
    assertEquals(original.getTime(), newValues.getSecond().getTime());
  }

  @Test
  public void testComputeNewDfvOdd() {
    DataFileValue original = new DataFileValue(21, 11, 100);
    Pair<DataFileValue,DataFileValue> newValues = DeleteRows.computeNewDfv(original);

    assertEquals(10, newValues.getFirst().getSize());
    assertEquals(5, newValues.getFirst().getNumEntries());
    assertEquals(original.getTime(), newValues.getFirst().getTime());
    assertEquals(11, newValues.getSecond().getSize());
    assertEquals(6, newValues.getSecond().getNumEntries());
    assertEquals(original.getTime(), newValues.getSecond().getTime());
  }

  @Test
  public void testComputeNewDfvSmall() {
    DataFileValue original = new DataFileValue(1, 2, 100);
    Pair<DataFileValue,DataFileValue> newValues = DeleteRows.computeNewDfv(original);

    assertEquals(1, newValues.getFirst().getSize());
    assertEquals(1, newValues.getFirst().getNumEntries());
    assertEquals(original.getTime(), newValues.getFirst().getTime());
    assertEquals(1, newValues.getSecond().getSize());
    assertEquals(1, newValues.getSecond().getNumEntries());
    assertEquals(original.getTime(), newValues.getSecond().getTime());
  }
}
