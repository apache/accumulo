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
package org.apache.accumulo.core.spi.balancer.util;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.TabletId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Throttles logging of balancer problems by storing the last log time for each problem and limiting
 * reports to once per minute. The logger is generated internally from the supplied class in order
 * to adhere to the requirements for the SPI package (which prohibits having the Logger class in the
 * API).
 */
public class ThrottledBalancerProblemReporter {
  public interface Problem {
    void report();
  }

  public interface OutstandingMigrationsProblem extends Problem {
    void setMigrations(Set<TabletId> migrations);
  }

  private static final long TIME_BETWEEN_WARNINGS = TimeUnit.SECONDS.toMillis(60);
  private final WeakHashMap<Problem,Long> problemReportTimes = new WeakHashMap<>();
  private final Logger log;

  public ThrottledBalancerProblemReporter(Class<?> loggerClass) {
    log = LoggerFactory.getLogger(loggerClass);
  }

  /**
   * Create a new problem reporter to indicate there are no tablet servers available and balancing
   * could not be performed. Balancers should only create a single instance of this problem, and
   * reuse each time the problem is reported.
   */
  public Problem createNoTabletServersProblem() {
    return () -> log.warn("Not balancing because we don't have any tservers.");
  }

  /**
   * Create a new problem reporter to indicate that balancing could not be performed due to the
   * existence of outstanding migrations. Balancers should only create a single instance of this
   * problem and update its migrations list before each report.
   */
  public OutstandingMigrationsProblem createOutstandingMigrationsProblem() {
    return new OutstandingMigrationsProblem() {
      private Set<TabletId> migrations = Collections.emptySet();

      @Override
      public void setMigrations(Set<TabletId> migrations) {
        this.migrations = migrations;
      }

      @Override
      public void report() {
        log.warn("Not balancing due to {} outstanding migrations.", migrations.size());
        /*
         * TODO ACCUMULO-2938 redact key extents in this output to avoid leaking protected
         * information.
         */
        if (log.isDebugEnabled()) {
          log.debug("Sample up to 10 outstanding migrations: {}",
              migrations.stream().limit(10).map(String::valueOf).collect(Collectors.joining(", ")));
        }
        // Now that we've reported, clear out the migrations list so we don't hold it in memory.
        migrations = Collections.emptySet();
      }
    };
  }

  /**
   * Reports a balance problem. The {@link Problem#report()} will only be called up to once a minute
   * for each problem that is reported repeatedly.
   */
  public void reportProblem(Problem problem) {
    long reportTime = problemReportTimes.getOrDefault(problem, -1L);
    if ((System.currentTimeMillis() - reportTime) > TIME_BETWEEN_WARNINGS) {
      problem.report();
      problemReportTimes.put(problem, System.currentTimeMillis());
    }
  }

  /**
   * Clears reported problems so that a problem report will be logged immediately the next time
   * {@link #reportProblem(Problem)} is invoked.
   */
  public void clearProblemReportTimes() {
    problemReportTimes.clear();
  }
}
