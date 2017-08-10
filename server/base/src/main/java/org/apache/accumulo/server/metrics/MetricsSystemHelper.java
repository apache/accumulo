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

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.metrics2.source.JvmMetricsInfo;

/**
 *
 */
public class MetricsSystemHelper {

  private static Map<String,String> serviceNameMap = new HashMap<>();
  private static String processName = "Unknown";

  static {
    serviceNameMap.put("master", "Master");
    serviceNameMap.put("tserver", "TabletServer");
    serviceNameMap.put("monitor", "Monitor");
    serviceNameMap.put("gc", "GarbageCollector");
    serviceNameMap.put("tracer", "Tracer");
    serviceNameMap.put("shell", "Shell");
  }

  public static void configure(String application) {
    String serviceName = application;
    if (MetricsSystemHelper.serviceNameMap.containsKey(application)) {
      serviceName = MetricsSystemHelper.serviceNameMap.get(application);
    }

    // a system property containing 'instance' can be used if more than one TabletServer is started on a host
    String serviceInstance = "";
    if (serviceName.equals("TabletServer")) {
      for (Map.Entry<Object,Object> p : System.getProperties().entrySet()) {
        if (((String) p.getKey()).contains("instance")) {
          // get rid of all non-alphanumeric characters and then prefix with a -
          serviceInstance = "-" + ((String) p.getValue()).replaceAll("[^a-zA-Z0-9]", "");
          break;
        }
      }
    }
    MetricsSystemHelper.processName = serviceName + serviceInstance;
  }

  public static String getProcessName() {
    return MetricsSystemHelper.processName;
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
