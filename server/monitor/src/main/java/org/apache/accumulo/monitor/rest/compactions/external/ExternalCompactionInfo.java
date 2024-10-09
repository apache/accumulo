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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.client.admin.servers.ServerId;

import com.google.common.net.HostAndPort;

/**
 * Bag of everything going on with external compactions.
 */
public class ExternalCompactionInfo {

  private Optional<HostAndPort> coordinatorHost;
  private Set<ServerId> compactors = new HashSet<>();
  private long fetchedTimeMillis;

  public void setCoordinatorHost(Optional<HostAndPort> coordinatorHost) {
    this.coordinatorHost = coordinatorHost;
  }

  public Optional<HostAndPort> getCoordinatorHost() {
    return coordinatorHost;
  }

  public Set<ServerId> getCompactors() {
    return compactors;
  }

  public void setCompactors(Set<ServerId> compactors) {
    this.compactors = compactors;
  }

  public long getFetchedTimeMillis() {
    return fetchedTimeMillis;
  }

  public void setFetchedTimeMillis(long fetchedTimeMillis) {
    this.fetchedTimeMillis = fetchedTimeMillis;
  }
}
