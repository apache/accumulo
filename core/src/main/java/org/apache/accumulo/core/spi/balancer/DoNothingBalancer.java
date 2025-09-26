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
package org.apache.accumulo.core.spi.balancer;

import org.apache.accumulo.core.data.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A balancer that will do nothing and warn about doing nothing. This purpose of this balancer is as
 * a fallback when attempts to create a balancer fail.
 *
 * @since 2.1.4
 */
public class DoNothingBalancer implements TabletBalancer {

  private static final Logger log = LoggerFactory.getLogger(DoNothingBalancer.class);

  private final TableId tableId;

  public DoNothingBalancer() {
    this.tableId = null;
  }

  @Override
  public void init(BalancerEnvironment balancerEnvironment) {}

  @Override
  public void getAssignments(AssignmentParameters params) {
    if (tableId != null) {
      log.warn("Balancer creation failed. Ignoring {} assignment request for tableId {}",
          params.unassignedTablets().size(), tableId);
    } else {
      log.warn("Balancer creation failed. Ignoring {} assignment request ",
          params.unassignedTablets().size());
    }
  }

  @Override
  public long balance(BalanceParameters params) {
    if (tableId != null) {
      log.warn("Balancer creation failed. Ignoring request to balance tablets for tableId:{}",
          tableId);
    } else {
      log.warn("Balancer creation failed. Ignoring request to balance tablets");
    }
    return 30_000;
  }
}
