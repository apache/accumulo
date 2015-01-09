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
package org.apache.accumulo.tserver;

public class CompactionStats {
  private long entriesRead;
  private long entriesWritten;
  private long fileSize;

  CompactionStats(long er, long ew) {
    this.setEntriesRead(er);
    this.setEntriesWritten(ew);
  }

  public CompactionStats() {}

  private void setEntriesRead(long entriesRead) {
    this.entriesRead = entriesRead;
  }

  public long getEntriesRead() {
    return entriesRead;
  }

  private void setEntriesWritten(long entriesWritten) {
    this.entriesWritten = entriesWritten;
  }

  public long getEntriesWritten() {
    return entriesWritten;
  }

  public void add(CompactionStats mcs) {
    this.entriesRead += mcs.entriesRead;
    this.entriesWritten += mcs.entriesWritten;
  }

  public void setFileSize(long fileSize) {
    this.fileSize = fileSize;
  }

  public long getFileSize() {
    return this.fileSize;
  }
}
