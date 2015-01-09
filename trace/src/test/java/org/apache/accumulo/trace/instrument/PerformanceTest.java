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
package org.apache.accumulo.trace.instrument;

import org.junit.Test;

public class PerformanceTest {

  @Test
  public void test() {

  }

  public static void main(String[] args) {

    long now = System.currentTimeMillis();
    for (long i = 0; i < 1000 * 1000; i++) {
      @SuppressWarnings("unused")
      Long x = new Long(i);
    }
    System.out.println(String.format("Trivial took %d millis", System.currentTimeMillis() - now));
    now = System.currentTimeMillis();
    for (long i = 0; i < 1000 * 1000; i++) {
      Span s = Trace.start("perf");
      s.stop();
    }
    System.out.println(String.format("Span Loop took %d millis", System.currentTimeMillis() - now));
    now = System.currentTimeMillis();
    Trace.on("test");
    for (long i = 0; i < 1000 * 1000; i++) {
      Span s = Trace.start("perf");
      s.stop();
    }
    Trace.off();
    System.out.println(String.format("Trace took %d millis", System.currentTimeMillis() - now));
  }
}
