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
package org.apache.accumulo.core.spi.scan;

import java.util.Comparator;

import com.google.common.base.Preconditions;

/**
 * Prioritize scans based on the ratio of runTime/idleTime. Scans with a lower ratio have a higher
 * priority. When the ratio is equal, the scan with the oldest last run time has the highest
 * priority. If neither have run, then the oldest gets priority.
 *
 * @since 2.0.0
 */
public class IdleRatioScanPrioritizer implements ScanPrioritizer {
  private static double idleRatio(long currTime, ScanInfo si) {
    double totalRunTime = si.getRunTimeStats().sum();
    double totalIdleTime = Math.max(1, si.getIdleTimeStats(currTime).sum());
    return totalRunTime / totalIdleTime;
  }

  @Override
  public Comparator<ScanInfo> createComparator(CreateParameters params) {
    Preconditions.checkArgument(params.getOptions().isEmpty());

    Comparator<ScanInfo> c1 = (si1, si2) -> {
      long currTime = System.currentTimeMillis();
      return Double.compare(idleRatio(currTime, si1), idleRatio(currTime, si2));
    };

    return c1.thenComparingLong(si -> si.getLastRunTime().orElse(0))
        .thenComparingLong(ScanInfo::getCreationTime);
  }
}
