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
package org.apache.accumulo.monitor.rest;

/**
 * Generates the totals for XML summary
 *
 * @since 2.0.0
 */
public class Totals {

  // Variable names become JSON keys
  public double ingestrate = 0.0;
  public double queryrate = 0.0;
  public double diskrate = 0.0;
  public long numentries = 0L;

  public Totals() {}

  /**
   * Initializes totals
   *
   * @param ingestrate Total ingest rate
   * @param queryrate Total query rate
   * @param numentries Total number of entries
   */
  public Totals(double ingestrate, double queryrate, long numentries) {
    this.ingestrate = ingestrate;
    this.queryrate = queryrate;
    this.numentries = numentries;
  }
}
