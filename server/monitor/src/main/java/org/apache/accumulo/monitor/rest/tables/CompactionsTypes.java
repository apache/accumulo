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
package org.apache.accumulo.monitor.rest.tables;

/**
 * Generates a list of compactions based on type
 *
 * @since 2.0.0
 */
public class CompactionsTypes {

  // Variable names become JSON objects
  public CompactionsList scans = new CompactionsList();
  public CompactionsList major = new CompactionsList();
  public CompactionsList minor = new CompactionsList();

  public CompactionsTypes() {}

  /**
   * Create a new compaction list based on types
   *
   * @param scans Scan compaction list
   * @param major Major compaction list
   * @param minor Minor compaction list
   */
  public CompactionsTypes(CompactionsList scans, CompactionsList major, CompactionsList minor) {
    this.scans = scans;
    this.major = major;
    this.minor = minor;
  }
}
