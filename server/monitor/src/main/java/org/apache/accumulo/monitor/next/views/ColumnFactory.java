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
package org.apache.accumulo.monitor.next.views;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.metrics.MonitorMeterRegistry;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.monitor.next.SystemInformation;
import org.apache.accumulo.monitor.next.views.TableData.Column;

public interface ColumnFactory {

  default Number computeRate(Number sum) {
    if (sum == null) {
      return null;
    }
    return sum.doubleValue() / MonitorMeterRegistry.STEP.toSeconds();
  }

  default Number add(Number n1, Number n2) {
    if (n1 == null && n2 == null) {
      return null;
    } else if (n1 == null) {
      return n2;
    } else if (n2 == null) {
      return n1;
    } else if (n1 instanceof Double || n2 instanceof Double) {
      return n1.doubleValue() + n2.doubleValue();
    } else if (n1 instanceof Long || n2 instanceof Long) {
      return n1.longValue() + n2.longValue();
    } else if (n1 instanceof Integer || n2 instanceof Integer) {
      return n1.intValue() + n2.intValue();
    } else {
      throw new IllegalArgumentException(
          "Unexpected value type: " + n1.getClass().getName() + " " + n2.getClass().getName());
    }
  }

  default Number sum(List<FMetric> metrics) {
    Number sum = null;
    for (var metric : metrics) {
      sum = add(sum, SystemInformation.getMetricValue(metric));
    }
    return sum;
  }

  Column getColumn();

  Object getRowData(ServerId sid, MetricResponse mr, Map<String,List<FMetric>> serverMetrics);

}
