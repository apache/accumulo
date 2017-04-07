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

package org.apache.accumulo.tserver.compaction;

import java.util.Set;

import org.apache.accumulo.server.fs.FileRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableSet;

public class CompactionPlanTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testOverlappingInputAndDelete() {
    CompactionPlan cp1 = new CompactionPlan();

    FileRef fr1 = new FileRef("hdfs://nn1/accumulo/tables/1/t-1/1.rf");
    FileRef fr2 = new FileRef("hdfs://nn1/accumulo/tables/1/t-1/2.rf");

    cp1.inputFiles.add(fr1);

    cp1.deleteFiles.add(fr1);
    cp1.deleteFiles.add(fr2);

    Set<FileRef> allFiles = ImmutableSet.of(fr1, fr2);

    exception.expect(IllegalStateException.class);
    cp1.validate(allFiles);
  }

  @Test
  public void testInputNotInAllFiles() {
    CompactionPlan cp1 = new CompactionPlan();

    FileRef fr1 = new FileRef("hdfs://nn1/accumulo/tables/1/t-1/1.rf");
    FileRef fr2 = new FileRef("hdfs://nn1/accumulo/tables/1/t-1/2.rf");
    FileRef fr3 = new FileRef("hdfs://nn1/accumulo/tables/1/t-2/3.rf");

    cp1.inputFiles.add(fr1);
    cp1.inputFiles.add(fr2);
    cp1.inputFiles.add(fr3);

    Set<FileRef> allFiles = ImmutableSet.of(fr1, fr2);

    exception.expect(IllegalStateException.class);
    cp1.validate(allFiles);
  }

  @Test
  public void testDeleteNotInAllFiles() {
    CompactionPlan cp1 = new CompactionPlan();

    FileRef fr1 = new FileRef("hdfs://nn1/accumulo/tables/1/t-1/1.rf");
    FileRef fr2 = new FileRef("hdfs://nn1/accumulo/tables/1/t-1/2.rf");
    FileRef fr3 = new FileRef("hdfs://nn1/accumulo/tables/1/t-2/3.rf");

    cp1.deleteFiles.add(fr1);
    cp1.deleteFiles.add(fr2);
    cp1.deleteFiles.add(fr3);

    Set<FileRef> allFiles = ImmutableSet.of(fr1, fr2);

    exception.expect(IllegalStateException.class);
    cp1.validate(allFiles);
  }

}
