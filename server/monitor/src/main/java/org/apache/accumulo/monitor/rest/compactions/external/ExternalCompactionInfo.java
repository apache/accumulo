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
package org.apache.accumulo.monitor.rest.compactions.external;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.accumulo.core.util.HostAndPort;

/**
 * Bag of everything going on with external compactions.
 */
public class ExternalCompactionInfo {

  private Optional<HostAndPort> coordinatorHost;
  private Map<String,List<HostAndPort>> compactors = new HashMap<>();
  private long fetchedTimeMillis;

  public void setCoordinatorHost(Optional<HostAndPort> coordinatorHost) {
    this.coordinatorHost = coordinatorHost;
  }

  public Optional<HostAndPort> getCoordinatorHost() {
    return coordinatorHost;
  }

  public Map<String,List<HostAndPort>> getCompactors() {
    return compactors;
  }

  public void setCompactors(Map<String,List<HostAndPort>> compactors) {
    this.compactors = compactors;
  }

  public long getFetchedTimeMillis() {
    return fetchedTimeMillis;
  }

  public void setFetchedTimeMillis(long fetchedTimeMillis) {
    this.fetchedTimeMillis = fetchedTimeMillis;
  }
}
