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
package org.apache.accumulo.server.util;

import org.apache.accumulo.core.tabletserver.thrift.ActionStats;

public class ActionStatsUpdator {

  public static void update(ActionStats summary, ActionStats td) {
    summary.setStatus(summary.getStatus() + td.getStatus());
    summary.setElapsed(summary.getElapsed() + td.getElapsed());
    summary.setNum(summary.getNum() + td.getNum());
    summary.setCount(summary.getCount() + td.getCount());
    summary.setSumDev(summary.getSumDev() + td.getSumDev());
    summary.setQueueTime(summary.getQueueTime() + td.getQueueTime());
    summary.setQueueSumDev(summary.getQueueSumDev() + td.getQueueSumDev());
    summary.setFail(summary.getFail() + td.getFail());
  }

}
