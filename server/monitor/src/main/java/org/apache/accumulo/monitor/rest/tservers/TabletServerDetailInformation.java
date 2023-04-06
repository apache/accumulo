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
 * Generates tserver detail as JSON object
 *
 * @since 2.0.0
 */
public class TabletServerDetailInformation {

  // Variable names become JSON keys
  public int hostedTablets;
  public int minors;
  public int majors;
  public int splits;
  public long entries;

  public TabletServerDetailInformation() {}

  /**
   * Store new tserver details
   *
   * @param hostedTablets Number of hosted tablets
   * @param entries Number of entries
   * @param minors Number of minor compactions
   * @param majors Number of major compactions
   * @param splits Number of splits
   */
  public TabletServerDetailInformation(int hostedTablets, long entries, int minors, int majors,
      int splits) {
    this.hostedTablets = hostedTablets;
    this.entries = entries;
    this.minors = minors;
    this.majors = majors;
    this.splits = splits;
  }
}
