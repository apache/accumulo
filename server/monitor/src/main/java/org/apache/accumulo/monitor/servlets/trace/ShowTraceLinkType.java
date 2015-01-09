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
package org.apache.accumulo.monitor.servlets.trace;

import java.util.Date;

import org.apache.accumulo.core.trace.TraceFormatter;
import org.apache.accumulo.monitor.util.celltypes.StringType;
import org.apache.accumulo.trace.thrift.RemoteSpan;

/**
 *
 */
public class ShowTraceLinkType extends StringType<RemoteSpan> {
  @Override
  public String format(Object obj) {
    if (obj == null)
      return "-";
    RemoteSpan span = (RemoteSpan) obj;
    return String.format("<a href='/trace/show?id=%s'>%s</a>", Long.toHexString(span.traceId), TraceFormatter.formatDate(new Date(span.start)));
  }

  @Override
  public int compare(RemoteSpan o1, RemoteSpan o2) {
    if (o1 == null && o2 == null)
      return 0;
    else if (o1 == null)
      return -1;
    else if (o2 == null)
      return 1;
    return o1.start < o2.start ? -1 : (o1.start == o2.start ? 0 : 1);
  }
}
