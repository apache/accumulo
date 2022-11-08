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
package org.apache.accumulo.core.client.summary.summarizers;

import static org.apache.accumulo.core.client.summary.CountingSummarizer.COUNTER_STAT_PREFIX;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.DELETES_IGNORED_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.EMITTED_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.SEEN_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.TOO_LONG_STAT;
import static org.apache.accumulo.core.client.summary.CountingSummarizer.TOO_MANY_STAT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;

import org.apache.accumulo.core.client.summary.Summarizer.Collector;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Test;

public class AuthorizationSummarizerTest {

  private static final Value EV = new Value();

  @Test
  public void testBasic() {
    SummarizerConfiguration sc =
        SummarizerConfiguration.builder(AuthorizationSummarizer.class).build();
    AuthorizationSummarizer authSummarizer = new AuthorizationSummarizer();

    Collector collector = authSummarizer.collector(sc);

    collector.accept(new Key("r", "f", "q", ""), EV);
    collector.accept(new Key("r", "f", "q", "A"), EV);
    collector.accept(new Key("r", "f", "q", "B"), EV);
    collector.accept(new Key("r", "f", "q", "A&B"), EV);
    collector.accept(new Key("r", "f", "q", "(C|D)&(A|B)"), EV);
    collector.accept(new Key("r", "f", "q", "(C|D)&(A|B)"), EV);
    collector.accept(new Key("r", "f", "q", "(D&E)|(D&C&F)"), EV);

    HashMap<String,Long> actual = new HashMap<>();
    collector.summarize(actual::put);

    String p = COUNTER_STAT_PREFIX;

    HashMap<String,Long> expected = new HashMap<>();
    expected.put(p + "A", 4L);
    expected.put(p + "B", 4L);
    expected.put(p + "C", 3L);
    expected.put(p + "D", 3L);
    expected.put(p + "E", 1L);
    expected.put(p + "F", 1L);
    expected.put(TOO_LONG_STAT, 0L);
    expected.put(TOO_MANY_STAT, 0L);
    expected.put(SEEN_STAT, 7L);
    expected.put(EMITTED_STAT, 16L);
    expected.put(DELETES_IGNORED_STAT, 0L);

    assertEquals(expected, actual);
  }
}
