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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.monitor.next.views.TableData.Column;

public class MetricColumnFactory implements ColumnFactory {

  private final Column column;
  private final boolean computeRate;

  MetricColumnFactory(Metric metric) {
    String classes;
    if (metric.getType() == Metric.MetricType.FUNCTION_COUNTER) {
      if (Arrays.asList(metric.getColumnClasses()).contains(Metric.MonitorCssClass.BYTES)) {
        classes = Metric.MonitorCssClass.BYTES_RATE.getCssClass();
      } else {
        classes = Metric.MonitorCssClass.RATE.getCssClass();
      }
      computeRate = true;
    } else {
      classes = Arrays.stream(metric.getColumnClasses()).map(Metric.MonitorCssClass::getCssClass)
          .collect(Collectors.joining(" "));
      computeRate = false;
    }
    this.column = new Column(metric.getName(), metric.getColumnHeader(),
        metric.getColumnDescription(), classes);
  }

  @Override
  public Column getColumn() {
    return column;
  }

  @Override
  public Object getRowData(ServerId sid, MetricResponse mr,
      Map<String,List<FMetric>> serverMetrics) {
    var sum = sum(serverMetrics.getOrDefault(column.key(), List.of()));
    if (computeRate) {
      return computeRate(sum);
    } else {
      return sum;
    }
  }
}
