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
package org.apache.accumulo.tserver.tablet;

import java.util.List;

import org.apache.accumulo.core.data.Key;

final class Batch {
  private final boolean skipContinueKey;
  private final List<KVEntry> results;
  private final Key continueKey;
  private final long numBytes;

  Batch(boolean skipContinueKey, List<KVEntry> results, Key continueKey, long numBytes) {
    this.skipContinueKey = skipContinueKey;
    this.results = results;
    this.continueKey = continueKey;
    this.numBytes = numBytes;
  }

  public boolean isSkipContinueKey() {
    return skipContinueKey;
  }

  public List<KVEntry> getResults() {
    return results;
  }

  public Key getContinueKey() {
    return continueKey;
  }

  public long getNumBytes() {
    return numBytes;
  }
}
