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
package org.apache.accumulo.monitor.rest.api;

public class AllTimeTabletResults {

  public String operation;
  public int success, failure;
  public Double queueStdDev, avgQueueTime, avgTime;
  public double timeSpent, stdDev;

  public AllTimeTabletResults() {}

  public AllTimeTabletResults(String operation, int success, int failure, Double avgQueueTime, Double majorQueueStdDev, Double queueStdDev, double stdDev,
      double timeSpent) {
    this.operation = operation;
    this.success = success;
    this.failure = failure;
    this.avgQueueTime = avgQueueTime;
    this.avgTime = majorQueueStdDev;
    this.queueStdDev = queueStdDev;
    this.stdDev = stdDev;
    this.timeSpent = timeSpent;
  }
}
