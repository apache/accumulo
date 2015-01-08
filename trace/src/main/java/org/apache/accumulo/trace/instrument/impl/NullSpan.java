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
package org.apache.accumulo.trace.instrument.impl;

import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.trace.instrument.Span;

/**
 * A Span that does nothing. Used to avoid returning and checking for nulls when we are not tracing.
 *
 */
public class NullSpan implements Span {

  public NullSpan() {}

  @Override
  public long accumulatedMillis() {
    return 0;
  }

  @Override
  public String description() {
    return "NullSpan";
  }

  @Override
  public long getStartTimeMillis() {
    return 0;
  }

  @Override
  public long getStopTimeMillis() {
    return 0;
  }

  @Override
  public Span parent() {
    return null;
  }

  @Override
  public long parentId() {
    return -1;
  }

  @Override
  public boolean running() {
    return false;
  }

  @Override
  public long spanId() {
    return -1;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public long traceId() {
    return -1;
  }

  @Override
  public Span child(String description) {
    return this;
  }

  @Override
  public void data(String key, String value) {}

  @Override
  public String toString() {
    return "Not Tracing";
  }

  @Override
  public Map<String,String> getData() {
    return Collections.emptyMap();
  }

}
