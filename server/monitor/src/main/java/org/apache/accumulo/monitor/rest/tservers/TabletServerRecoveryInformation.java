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
 * Generates a tserver recovery information
 *
 * @since 2.0.0
 */
public class TabletServerRecoveryInformation {

  // Variable names become JSON keys
  public String server = "";
  public String log = "";
  public int time = 0;
  public double progress = 0d;

  public TabletServerRecoveryInformation() {}

  /**
   * Stores a tserver recovery
   *
   * @param server Name of the tserver
   * @param log Log of the tserver
   * @param time Recovery runtime
   * @param progress Recovery progress
   */
  public TabletServerRecoveryInformation(String server, String log, int time, double progress) {
    this.server = server;
    this.log = log;
    this.time = time;
    this.progress = progress;
  }
}
