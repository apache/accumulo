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
import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.monitor.next.SystemInformation;
import org.apache.accumulo.monitor.next.views.TableDataFactory.StatType;

import com.google.common.base.Preconditions;

public class TimerColumnFactory implements ColumnFactory {

  private final TableData.Column column;

  public TimerColumnFactory(Metric metric) {
    Preconditions.checkArgument(metric.getType() == Metric.MetricType.TIMER);

    this.column = new TableData.Column(metric.getName(), metric.getColumnHeader(),
        metric.getColumnDescription(), Metric.MonitorCssClass.DURATION.getCssClass());

  }

  @Override
  public TableData.Column getColumn() {
    return column;
  }

  @Override
  public Object getRowData(ServerId sid, MetricResponse mr,
      Map<String,List<FMetric>> serverMetrics) {
    var metrics = serverMetrics.getOrDefault(column.key(), List.of());
    FTag ftag = new FTag();
    for (var metric : metrics) {
      for (int i = 0; i < metric.tagsLength(); i++) {
        metric.tags(ftag, i);
      }
      String statVal = TableDataFactory.extractStatistic(metric, ftag);
      if (StatType.AVERAGE.equals(statVal)) {
        // The average time in in seconds, convert it to millis for the monitor front end code.
        return SystemInformation.getMetricValue(metric).doubleValue() * 1000;
      }
    }
    return null;
  }
}
