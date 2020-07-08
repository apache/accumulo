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

package org.apache.accumulo.core.util.compaction;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.junit.Test;

public class CompactionPrioritizerTest {

  public CompactionJob createJob(CompactionKind kind, String tablet, int numFiles, int totalFiles) {

    Collection<CompactableFile> files = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      files.add(CompactableFile
          .create(URI.create("hdfs://foonn/accumulo/tables/5/" + tablet + "/" + i + ".rf"), 4, 4));
    }
    return new CompactionJobImpl(CompactionJobPrioritizer.createPriority(kind, totalFiles),
        CompactionExecutorId.of("test"), files, kind, false);
  }

  @Test
  public void testCompactionJobComparator() {
    var j1 = createJob(CompactionKind.USER, "t-009", 10, 20);
    var j2 = createJob(CompactionKind.USER, "t-010", 11, 25);
    var j3 = createJob(CompactionKind.USER, "t-011", 11, 20);
    var j4 = createJob(CompactionKind.SYSTEM, "t-012", 11, 30);
    var j5 = createJob(CompactionKind.SYSTEM, "t-013", 5, 10);
    var j6 = createJob(CompactionKind.CHOP, "t-014", 5, 40);
    var j7 = createJob(CompactionKind.CHOP, "t-015", 5, 7);
    var j8 = createJob(CompactionKind.SELECTOR, "t-014", 5, 21);
    var j9 = createJob(CompactionKind.SELECTOR, "t-015", 7, 21);

    var expected = List.of(j2, j3, j1, j6, j7, j9, j8, j4, j5);

    var shuffled = new ArrayList<>(expected);
    Collections.shuffle(shuffled);
    Collections.sort(shuffled, CompactionJobPrioritizer.JOB_COMPARATOR);

    assertEquals(expected, shuffled);
  }
}
