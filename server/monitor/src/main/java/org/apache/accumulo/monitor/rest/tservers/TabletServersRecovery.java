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

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Generates a list of tservers recoveries
 *
 * @since 2.0.0
 *
 */
public class TabletServersRecovery {

  // Variable names become JSON keys
  public List<TabletServerRecoveryInformation> recoveryList = new ArrayList<>();

  /**
   * Initializes tserver recovery list
   */
  public TabletServersRecovery() {}

  /**
   * Adds a new tserver recovery to the list
   *
   * @param tabletServerRecovery
   *          TServer recovery to add
   */
  public void addRecovery(TabletServerRecoveryInformation tabletServerRecovery) {
    this.recoveryList.add(tabletServerRecovery);
  }

}
