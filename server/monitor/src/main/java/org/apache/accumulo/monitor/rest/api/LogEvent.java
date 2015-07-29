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
package org.apache.accumulo.monitor.rest.api;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A single message logged
 */
public class LogEvent {

  protected long timestamp;
  protected Object application;
  protected int count;
  protected String level, message;

  public LogEvent() {}

  public LogEvent(long timestamp, Object application, int count, String level, String message) {
    this.timestamp = timestamp;
    this.application = application;
    this.count = count;
    this.level = level;
    this.message = message;
  }

  @JsonProperty("timestamp")
  public long getTimestamp() {
    return timestamp;
  }

  @JsonProperty("timestamp")
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @JsonProperty("application")
  public Object getApplication() {
    return application;
  }

  @JsonProperty("application")
  public void setApplication(Object application) {
    this.application = application;
  }

  @JsonProperty("count")
  public int getCount() {
    return count;
  }

  @JsonProperty("count")
  public void setCount(int count) {
    this.count = count;
  }

  @JsonProperty("level")
  public String getLevel() {
    return level;
  }

  @JsonProperty("level")
  public void setLevel(String level) {
    this.level = level;
  }

  @JsonProperty("message")
  public String getMessage() {
    return message;
  }

  @JsonProperty("message")
  public void setMessage(String message) {
    this.message = message;
  }
}
