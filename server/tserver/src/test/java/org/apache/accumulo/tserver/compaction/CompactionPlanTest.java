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
package org.apache.accumulo.tserver.compaction;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.junit.jupiter.api.Test;

@SuppressWarnings("removal")
public class CompactionPlanTest {

  @Test
  public void testOverlappingInputAndDelete() {
    CompactionPlan cp1 = new CompactionPlan();

    StoredTabletFile fr1 = new StoredTabletFile("hdfs://nn1/accumulo/tables/1/t-1/1.rf");
    StoredTabletFile fr2 = new StoredTabletFile("hdfs://nn1/accumulo/tables/1/t-1/2.rf");

    cp1.inputFiles.add(fr1);

    cp1.deleteFiles.add(fr1);
    cp1.deleteFiles.add(fr2);

    Set<StoredTabletFile> allFiles = Set.of(fr1, fr2);

    assertThrows(IllegalStateException.class, () -> cp1.validate(allFiles));
  }

  @Test
  public void testInputNotInAllFiles() {
    CompactionPlan cp1 = new CompactionPlan();

    StoredTabletFile fr1 = new StoredTabletFile("hdfs://nn1/accumulo/tables/1/t-1/1.rf");
    StoredTabletFile fr2 = new StoredTabletFile("hdfs://nn1/accumulo/tables/1/t-1/2.rf");
    StoredTabletFile fr3 = new StoredTabletFile("hdfs://nn1/accumulo/tables/1/t-2/3.rf");

    cp1.inputFiles.add(fr1);
    cp1.inputFiles.add(fr2);
    cp1.inputFiles.add(fr3);

    Set<StoredTabletFile> allFiles = Set.of(fr1, fr2);

    assertThrows(IllegalStateException.class, () -> cp1.validate(allFiles));
  }

  @Test
  public void testDeleteNotInAllFiles() {
    CompactionPlan cp1 = new CompactionPlan();

    StoredTabletFile fr1 = new StoredTabletFile("hdfs://nn1/accumulo/tables/1/t-1/1.rf");
    StoredTabletFile fr2 = new StoredTabletFile("hdfs://nn1/accumulo/tables/1/t-1/2.rf");
    StoredTabletFile fr3 = new StoredTabletFile("hdfs://nn1/accumulo/tables/1/t-2/3.rf");

    cp1.deleteFiles.add(fr1);
    cp1.deleteFiles.add(fr2);
    cp1.deleteFiles.add(fr3);

    Set<StoredTabletFile> allFiles = Set.of(fr1, fr2);

    assertThrows(IllegalStateException.class, () -> cp1.validate(allFiles));
  }

}
