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
package org.apache.accumulo.examples.wikisearch.logic;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.StatusReporter;

public class StandaloneStatusReporter extends StatusReporter {
  
  private Counters c = new Counters();
  
  private long filesProcessed = 0;
  private long recordsProcessed = 0;
  
  public Counters getCounters() {
    return c;
  }
  
  @Override
  public Counter getCounter(Enum<?> name) {
    return c.findCounter(name);
  }
  
  @Override
  public Counter getCounter(String group, String name) {
    return c.findCounter(group, name);
  }
  
  @Override
  public void progress() {
    // do nothing
  }
  
  @Override
  public void setStatus(String status) {
    // do nothing
  }
  
  public long getFilesProcessed() {
    return filesProcessed;
  }
  
  public long getRecordsProcessed() {
    return recordsProcessed;
  }
  
  public void incrementFilesProcessed() {
    filesProcessed++;
    recordsProcessed = 0;
  }
  
  public void incrementRecordsProcessed() {
    recordsProcessed++;
  }
  
  public float getProgress() {
    return 0;
  }
}
