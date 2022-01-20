/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.memory;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.tserver.tablet.Tablet;

public class TabletMemoryReport implements Cloneable {

  private final Tablet tablet;
  private final long lastCommitTime;
  private final long memTableSize;
  private final long minorCompactingMemTableSize;

  public TabletMemoryReport(Tablet tablet, long lastCommitTime, long memTableSize,
      long minorCompactingMemTableSize) {
    this.tablet = tablet;
    this.lastCommitTime = lastCommitTime;
    this.memTableSize = memTableSize;
    this.minorCompactingMemTableSize = minorCompactingMemTableSize;
  }

  public KeyExtent getExtent() {
    return tablet.getExtent();
  }

  public Tablet getTablet() {
    return tablet;
  }

  public long getLastCommitTime() {
    return lastCommitTime;
  }

  public long getMemTableSize() {
    return memTableSize;
  }

  public long getMinorCompactingMemTableSize() {
    return minorCompactingMemTableSize;
  }

  @Override
  public TabletMemoryReport clone() throws CloneNotSupportedException {
    return (TabletMemoryReport) super.clone();
  }
}
