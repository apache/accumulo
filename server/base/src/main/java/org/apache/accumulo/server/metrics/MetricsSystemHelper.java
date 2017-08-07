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
package org.apache.accumulo.server.metrics;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.source.JvmMetricsInfo;

/**
 *
 */
public class MetricsSystemHelper {

  private static Map<String,String> serviceNameMap = new HashMap<>();

  static {
    serviceNameMap.put("master", "Master");
    serviceNameMap.put("tserver", "TabletServer");
    serviceNameMap.put("monitor", "Monitor");
    serviceNameMap.put("gc", "GarbageCollector");
    serviceNameMap.put("tracer", "Tracer");
    serviceNameMap.put("shell", "Shell");
  }

  public static String getProcessName() {
    String serviceName = System.getProperty("app", "Unknown");
    if (MetricsSystemHelper.serviceNameMap.containsKey(serviceName)) {
      serviceName = MetricsSystemHelper.serviceNameMap.get(serviceName);
    }
    // this system property is used if more than one TabletServer is started on a host
    String serviceInstance = System.getProperty("accumulo.service.instance", "");
    if (StringUtils.isNotBlank(serviceInstance)) {
      // property ends with an underscore because it's used in log filenames
      serviceInstance = "-" + serviceInstance.replaceAll("_", "");
    }
    return serviceName + serviceInstance;
  }

  private static class MetricsSystemHolder {
    // Singleton, rely on JVM to initialize the MetricsSystem only when it is accessed.
    private static final MetricsSystem metricsSystem = DefaultMetricsSystem.initialize(Metrics.PREFIX);
  }

  public static MetricsSystem getInstance() {
    if (MetricsSystemHolder.metricsSystem.getSource(JvmMetricsInfo.JvmMetrics.name()) == null) {
      JvmMetrics.create(getProcessName(), "", MetricsSystemHolder.metricsSystem);
    }
    return MetricsSystemHolder.metricsSystem;
  }

}
