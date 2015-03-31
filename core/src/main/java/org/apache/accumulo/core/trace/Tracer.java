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
package org.apache.accumulo.core.trace;

import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.htrace.Span;

public class Tracer {
  private static final TInfo DONT_TRACE = new TInfo(0, 0);

  /**
   * Obtain {@link org.apache.accumulo.core.trace.thrift.TInfo} for the current span.
   */
  public static TInfo traceInfo() {
    Span span = org.apache.htrace.Trace.currentSpan();
    if (span != null) {
      return new TInfo(span.getTraceId(), span.getSpanId());
    }
    return DONT_TRACE;
  }
}
