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
 * Generates a list of bad tservers
 *
 * @since 2.0.0
 *
 */
public class BadTabletServers {

  // Variable names become JSON keys
  public List<BadTabletServerInformation> badTabletServer = new ArrayList<>();

  /**
   * Initializes bad tserver list
   */
  public BadTabletServers() {}

  /**
   * Adds a new bad tserver to the list
   *
   * @param badTabletServer
   *          Bad tserver to add
   */
  public void addBadServer(BadTabletServerInformation badTabletServer) {
    this.badTabletServer.add(badTabletServer);
  }

}
