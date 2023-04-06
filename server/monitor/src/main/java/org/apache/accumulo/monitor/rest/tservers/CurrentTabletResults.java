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
package org.apache.accumulo.monitor.rest.tservers;

/**
 * Generates the current results of the tablet
 *
 * @since 2.0.0
 */
public class CurrentTabletResults {

  // Variable names become JSON keys
  public Double currentMinorAvg;
  public Double currentMinorStdDev;
  public Double currentMajorAvg;
  public Double currentMajorStdDev;

  public CurrentTabletResults() {}

  /**
   * Stores new current results for the tablet
   *
   * @param currentMinorAvg minor compaction average
   * @param currentMinorStdDev minor compaction standard deviation
   * @param currentMajorAvg major compaction average
   * @param currentMajorStdDev major compaction standard deviation
   */
  public CurrentTabletResults(Double currentMinorAvg, Double currentMinorStdDev,
      Double currentMajorAvg, Double currentMajorStdDev) {
    this.currentMinorAvg = currentMinorAvg;
    this.currentMinorStdDev = currentMinorStdDev;
    this.currentMajorAvg = currentMajorAvg;
    this.currentMajorStdDev = currentMajorStdDev;
  }
}
