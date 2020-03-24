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
package org.apache.accumulo.monitor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.accumulo.tracer.thrift.RemoteSpan;
import org.junit.Test;

public class ShowTraceLinkTypeTest {
  private static RemoteSpan rs(long start, long stop, String description) {
    return new RemoteSpan("sender", "svc", 0L, 0L, Collections.singletonList(0L), start, stop,
        description, Collections.emptyMap(), Collections.emptyList());
  }

  @Test
  public void testTraceSortingForMonitor() {
    ArrayList<RemoteSpan> spans = new ArrayList<>(10), expectedOrdering = new ArrayList<>(10);

    // "Random" ordering
    spans.add(rs(55L, 75L, "desc5"));
    spans.add(rs(25L, 30L, "desc2"));
    spans.add(rs(85L, 90L, "desc8"));
    spans.add(rs(45L, 60L, "desc4"));
    spans.add(rs(35L, 55L, "desc3"));
    spans.add(rs(95L, 110L, "desc9"));
    spans.add(rs(65L, 80L, "desc6"));
    spans.add(rs(100L, 120L, "desc10"));
    spans.add(rs(15L, 25L, "desc1"));
    spans.add(rs(75L, 100L, "desc7"));

    // We expect them to be sorted by 'start'
    expectedOrdering.add(rs(15L, 25L, "desc1"));
    expectedOrdering.add(rs(25L, 30L, "desc2"));
    expectedOrdering.add(rs(35L, 55L, "desc3"));
    expectedOrdering.add(rs(45L, 60L, "desc4"));
    expectedOrdering.add(rs(55L, 75L, "desc5"));
    expectedOrdering.add(rs(65L, 80L, "desc6"));
    expectedOrdering.add(rs(75L, 100L, "desc7"));
    expectedOrdering.add(rs(85L, 90L, "desc8"));
    expectedOrdering.add(rs(95L, 110L, "desc9"));
    expectedOrdering.add(rs(100L, 120L, "desc10"));

    Collections.sort(spans);

    assertEquals(expectedOrdering, spans);
  }
}
