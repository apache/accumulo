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
package org.apache.accumulo.monitor.rest.tservers;

/**
 *
 * Stores the server stats
 *
 * @since 2.0.0
 *
 */
public class ServerStat {

  // Variable names become JSON keys
  public int max;
  public boolean adjustMax;
  public float significance;
  public String description, name;
  public boolean derived;

  public ServerStat() {}

  public ServerStat(int max, boolean adjustMax, float significance, String description, String name) {
    this.max = max;
    this.adjustMax = adjustMax;
    this.significance = significance;
    this.description = description;
    this.derived = false;
    this.name = name;
  }

  public ServerStat(int max, boolean adjustMax, float significance, String description, boolean derived, String name) {
    this.max = max;
    this.adjustMax = adjustMax;
    this.significance = significance;
    this.description = description;
    this.derived = derived;
    this.name = name;
  }
}
