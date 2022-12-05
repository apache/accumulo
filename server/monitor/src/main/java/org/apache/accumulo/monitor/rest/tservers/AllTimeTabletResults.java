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
 * Generates a tablets all time results
 *
 * @since 2.0.0
 */
public class AllTimeTabletResults {

  // Variable names become JSON keys
  public String operation;
  public int success;
  public int failure;
  public Double queueStdDev;
  public Double avgQueueTime;
  public Double avgTime;
  public double timeSpent;
  public double stdDev;

  public AllTimeTabletResults() {}

  /**
   * Stores all time results of a tablet
   *
   * @param operation Type of operation
   * @param success Number of successes
   * @param failure Number of failures
   * @param avgQueueTime Average queue time
   * @param queueStdDev Standard deviation of queue
   * @param avgTime Average time
   * @param stdDev Standard deviation
   * @param timeSpent Time spent in operation
   */
  public AllTimeTabletResults(String operation, int success, int failure, Double avgQueueTime,
      Double queueStdDev, Double avgTime, double stdDev, double timeSpent) {
    this.operation = operation;
    this.success = success;
    this.failure = failure;
    this.avgQueueTime = avgQueueTime;
    this.avgTime = avgTime;
    this.queueStdDev = queueStdDev;
    this.stdDev = stdDev;
    this.timeSpent = timeSpent;
  }
}
