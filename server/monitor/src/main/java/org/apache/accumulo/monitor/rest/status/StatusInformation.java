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
package org.apache.accumulo.monitor.rest.status;

/**
 * Generates status of manager, gc, and tservers, as well as log and problem report
 *
 * @since 2.0.0
 */
public class StatusInformation {

  // Variable names become JSON keys
  public String managerStatus = null;
  public String gcStatus = null;
  public String tServerStatus = null;

  public Integer logNumber = 0;
  public boolean logsHaveError = false;
  public Integer problemNumber = 0;

  public StatusInformation() {}

  /**
   * Generate the status report for the services
   *
   * @param managerStatus Status for the manager
   * @param gcStatus Status for the GC
   * @param tServerStatus Status for the tserver
   * @param logNumber Number of log reports
   * @param logsHaveError Check if log reports include errors
   * @param problemNumber Number of problems per table
   */
  public StatusInformation(String managerStatus, String gcStatus, String tServerStatus,
      Integer logNumber, boolean logsHaveError, Integer problemNumber) {
    this.managerStatus = managerStatus;
    this.gcStatus = gcStatus;
    this.tServerStatus = tServerStatus;
    this.logNumber = logNumber;
    this.logsHaveError = logsHaveError;
    this.problemNumber = problemNumber;

  }
}
