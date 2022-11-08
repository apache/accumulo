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
package org.apache.accumulo.manager.metrics;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.metrics.fate.FateMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagerMetrics {

  private final static Logger log = LoggerFactory.getLogger(ManagerMetrics.class);

  public static void init(AccumuloConfiguration conf, Manager m) {
    requireNonNull(conf, "AccumuloConfiguration must not be null");
    @SuppressWarnings("deprecation")
    ReplicationMetrics replMetrics = new ReplicationMetrics(m);
    MetricsUtil.initializeProducers(replMetrics);
    log.info("Registered replication metrics module");
    MetricsUtil.initializeProducers(new FateMetrics(m.getContext(),
        conf.getTimeInMillis(Property.MANAGER_FATE_METRICS_MIN_UPDATE_INTERVAL)));
    log.info("Registered FATE metrics module");
  }

}
