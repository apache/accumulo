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
package org.apache.accumulo.server.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.stream.Stream;

import org.apache.accumulo.core.gc.GcCandidate;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.server.gc.GcVolumeUtil;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class RootGcCandidatesTest {

  @Test
  public void externalCandidateRoundTrip() {
    StoredTabletFile external = StoredTabletFile.ofExternal(new Path("file:///external/F.rf"));
    StoredTabletFile standard =
        StoredTabletFile.of(new Path("file:///accumulo/tables/!0/t-0/F.rf"));

    RootGcCandidates candidates = new RootGcCandidates();
    candidates.add(Stream.of(external, standard));

    RootGcCandidates reloaded = new RootGcCandidates(candidates.toJson());
    Set<String> paths = reloaded.sortedStream().map(GcCandidate::getPath)
        .collect(java.util.stream.Collectors.toSet());
    assertEquals(Set.of(GcVolumeUtil.encodeExternalCandidatePath(external.getPath().toString()),
        standard.getPath().toString()), paths);

    String externalCandidate =
        GcVolumeUtil.encodeExternalCandidatePath(external.getPath().toString());
    reloaded.remove(Stream.of(new GcCandidate(externalCandidate, 1)));
    assertEquals(Set.of(standard.getPath().toString()), reloaded.sortedStream()
        .map(GcCandidate::getPath).collect(java.util.stream.Collectors.toSet()));
  }
}
