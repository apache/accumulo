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

import org.apache.accumulo.trace.thrift.RemoteSpan;
import org.junit.Assert;
import org.junit.Test;

public class ShowTraceLinkTypeTest {

  @Test
  public void testTraceSortingForMonitor() {
    /*
     * public RemoteSpan(String sender, String svc, long traceId, long spanId, long parentId, long start, long stop, String description, Map<String,String>
     * data)
     */
    ArrayList<RemoteSpan> spans = new ArrayList<RemoteSpan>(10), expectedOrdering = new ArrayList<RemoteSpan>(10);

    // "Random" ordering
    spans.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 55l, 75l, "desc5", Collections.<String,String> emptyMap()));
    spans.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 25l, 30l, "desc2", Collections.<String,String> emptyMap()));
    spans.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 85l, 90l, "desc8", Collections.<String,String> emptyMap()));
    spans.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 45l, 60l, "desc4", Collections.<String,String> emptyMap()));
    spans.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 35l, 55l, "desc3", Collections.<String,String> emptyMap()));
    spans.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 95l, 110l, "desc9", Collections.<String,String> emptyMap()));
    spans.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 65l, 80l, "desc6", Collections.<String,String> emptyMap()));
    spans.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 100l, 120l, "desc10", Collections.<String,String> emptyMap()));
    spans.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 15l, 25l, "desc1", Collections.<String,String> emptyMap()));
    spans.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 75l, 100l, "desc7", Collections.<String,String> emptyMap()));

    // We expect them to be sorted by 'start'
    expectedOrdering.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 15l, 25l, "desc1", Collections.<String,String> emptyMap()));
    expectedOrdering.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 25l, 30l, "desc2", Collections.<String,String> emptyMap()));
    expectedOrdering.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 35l, 55l, "desc3", Collections.<String,String> emptyMap()));
    expectedOrdering.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 45l, 60l, "desc4", Collections.<String,String> emptyMap()));
    expectedOrdering.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 55l, 75l, "desc5", Collections.<String,String> emptyMap()));
    expectedOrdering.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 65l, 80l, "desc6", Collections.<String,String> emptyMap()));
    expectedOrdering.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 75l, 100l, "desc7", Collections.<String,String> emptyMap()));
    expectedOrdering.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 85l, 90l, "desc8", Collections.<String,String> emptyMap()));
    expectedOrdering.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 95l, 110l, "desc9", Collections.<String,String> emptyMap()));
    expectedOrdering.add(new RemoteSpan("sender", "svc", 0l, 0l, 0l, 100l, 120l, "desc10", Collections.<String,String> emptyMap()));

    Collections.sort(spans);

    Assert.assertEquals(expectedOrdering, spans);
  }
}
