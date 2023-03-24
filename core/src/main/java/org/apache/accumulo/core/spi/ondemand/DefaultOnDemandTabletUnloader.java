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
package org.apache.accumulo.core.spi.ondemand;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.stream.Collectors;

import org.apache.accumulo.core.data.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultOnDemandTabletUnloader implements OnDemandTabletUnloader {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultOnDemandTabletUnloader.class);
  public static final String INACTIVITY_THRESHOLD =
      "table.custom.ondemand.unloader.inactivity.threshold.seconds";
  private static final String TEN_MINUTES = Long.toString(MINUTES.toSeconds(10));

  @Override
  public void evaluate(UnloaderParams params) {
    String inactivitySeconds = params.getServiceEnvironment()
        .getConfiguration(TableId.of(params.getTableId())).get(INACTIVITY_THRESHOLD);
    if (inactivitySeconds == null) {
      inactivitySeconds = TEN_MINUTES;
    }
    // access times are stored in nanos
    final long threshold = SECONDS.toNanos(Long.parseLong(inactivitySeconds));
    final long currentTime = getCurrentTime();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Current time: {}", currentTime);
      LOG.trace("Inactivity Threshold: {}", threshold);
      params.getLastAccessTimes().forEach((k, v) -> {
        LOG.trace("Tablet: {}, LastAccessTime: {}, should unload: {}", k, v,
            (currentTime - v) > threshold);
      });
    }
    params.setOnDemandTabletsToUnload(params.getLastAccessTimes().entrySet().stream()
        .filter(e -> (currentTime - e.getValue()) > threshold)
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())).keySet());
  }

  protected long getCurrentTime() {
    return System.nanoTime();
  }

}
