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
package org.apache.accumulo.tserver.metrics;

/**
 * Keys for general tablet server metrics
 */
public interface TabletServerMetricsKeys {

  String ENTRIES = "entries";
  String ENTRIES_IN_MEM = "entriesInMem";
  String HOLD_TIME = "holdTime";
  String FILES_PER_TABLET = "filesPerTablet";
  String ACTIVE_MAJCS = "activeMajCs";
  String QUEUED_MAJCS = "queuedMajCs";
  String ACTIVE_MINCS = "activeMinCs";
  String QUEUED_MINCS = "queuedMinCs";
  String ONLINE_TABLETS = "onlineTablets";
  String OPENING_TABLETS = "openingTablets";
  String UNOPENED_TABLETS = "unopenedTablets";
  String QUERIES = "queries";
  String TOTAL_MINCS = "totalMinCs";

}
