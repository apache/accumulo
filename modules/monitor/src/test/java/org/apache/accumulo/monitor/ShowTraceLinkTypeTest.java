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
package org.apache.accumulo.monitor;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.accumulo.tracer.thrift.Annotation;
import org.apache.accumulo.tracer.thrift.RemoteSpan;
import org.junit.Assert;
import org.junit.Test;

public class ShowTraceLinkTypeTest {
  private static RemoteSpan rs(long start, long stop, String description) {
    return new RemoteSpan("sender", "svc", 0l, 0l, 0l, start, stop, description, Collections.<String,String> emptyMap(), Collections.<Annotation> emptyList());
  }

  @Test
  public void testTraceSortingForMonitor() {
    ArrayList<RemoteSpan> spans = new ArrayList<>(10), expectedOrdering = new ArrayList<>(10);

    // "Random" ordering
    spans.add(rs(55l, 75l, "desc5"));
    spans.add(rs(25l, 30l, "desc2"));
    spans.add(rs(85l, 90l, "desc8"));
    spans.add(rs(45l, 60l, "desc4"));
    spans.add(rs(35l, 55l, "desc3"));
    spans.add(rs(95l, 110l, "desc9"));
    spans.add(rs(65l, 80l, "desc6"));
    spans.add(rs(100l, 120l, "desc10"));
    spans.add(rs(15l, 25l, "desc1"));
    spans.add(rs(75l, 100l, "desc7"));

    // We expect them to be sorted by 'start'
    expectedOrdering.add(rs(15l, 25l, "desc1"));
    expectedOrdering.add(rs(25l, 30l, "desc2"));
    expectedOrdering.add(rs(35l, 55l, "desc3"));
    expectedOrdering.add(rs(45l, 60l, "desc4"));
    expectedOrdering.add(rs(55l, 75l, "desc5"));
    expectedOrdering.add(rs(65l, 80l, "desc6"));
    expectedOrdering.add(rs(75l, 100l, "desc7"));
    expectedOrdering.add(rs(85l, 90l, "desc8"));
    expectedOrdering.add(rs(95l, 110l, "desc9"));
    expectedOrdering.add(rs(100l, 120l, "desc10"));

    Collections.sort(spans);

    Assert.assertEquals(expectedOrdering, spans);
  }
}
