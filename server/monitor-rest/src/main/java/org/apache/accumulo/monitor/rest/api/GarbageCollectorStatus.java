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

import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * 
 */
public class GarbageCollectorStatus {
  public static final GarbageCollectorStatus EMPTY = new GarbageCollectorStatus();

  protected GarbageCollection fileCollection = new GarbageCollection(), logCollection = new GarbageCollection();

  public GarbageCollectorStatus() {}

  public GarbageCollectorStatus(GCStatus status) {
    if (null != status) {
      fileCollection = new GarbageCollection(status.last, status.current);
      logCollection = new GarbageCollection(status.lastLog, status.currentLog);
    }
  }

  @JsonProperty("files")
  public GarbageCollection getFiles() {
    return fileCollection;
  }

  @JsonProperty("files")
  public void setFiles(GarbageCollection fileCollection) {
    this.fileCollection = fileCollection;
  }

  @JsonProperty("wals")
  public GarbageCollection getWals() {
    return logCollection;
  }

  @JsonProperty("wals")
  public void setWals(GarbageCollection logCollection) {
    this.logCollection = logCollection;
  }
}
