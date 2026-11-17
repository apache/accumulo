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
import java.util.function.Predicate;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.monitor.next.SystemInformation;
import org.apache.accumulo.monitor.next.views.TableData.Column;
import org.apache.accumulo.monitor.next.views.TableDataFactory.StatType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutorColumnFactory implements ColumnFactory {

  private static final Logger log = LoggerFactory.getLogger(ExecutorColumnFactory.class);

  private final Column column;
  private final String metricName;
  private final Predicate<String> tagPredicate;
  private final Type type;

  enum Type {
    COMPLETED, QUEUED
  }

  String getMetricName(Type t) {
    return switch (t) {
      case COMPLETED -> Metric.EXECUTOR_COMPLETED.getName();
      case QUEUED -> Metric.EXECUTOR_QUEUED.getName();
    };
  }

  String getCssClass(Type t) {
    return switch (t) {
      case COMPLETED -> Metric.MonitorCssClass.RATE.getCssClass();
      case QUEUED -> Metric.MonitorCssClass.NUMBER.getCssClass();
    };
  }

  ExecutorColumnFactory(Type type, String theadPoolPrefix, String label, String description) {
    this.type = type;
    this.metricName = getMetricName(type);
    this.tagPredicate = s -> s.startsWith(theadPoolPrefix);
    this.column =
        new Column(metricName + "-" + theadPoolPrefix, label, description, getCssClass(type));
  }

  ExecutorColumnFactory(Type type, String keySuffix, Predicate<String> tagPredicate, String label,
      String description) {
    this.type = type;
    this.metricName = getMetricName(type);
    this.tagPredicate = tagPredicate;
    this.column = new Column(metricName + "-" + keySuffix, label, description, getCssClass(type));
  }

  @Override
  public Column getColumn() {
    return column;
  }

  @Override
  public Object getRowData(ServerId sid, MetricResponse mr,
      Map<String,List<FMetric>> serverMetrics) {

    var metrics = serverMetrics.getOrDefault(metricName, List.of());

    Number sum = null;

    FTag ftag = new FTag();
    FTag statTag = new FTag();
    for (var metric : metrics) {
      boolean foundTag = false;
      String tag = null;
      for (int i = 0; i < metric.tagsLength(); i++) {
        metric.tags(ftag, i);
        var key = ftag.key();
        var value = ftag.value();
        if (key != null && value != null && key.equals("name") && tagPredicate.test(value)) {
          foundTag = true;
          tag = value;
          break;
        }
      }

      var metricStatistic = TableDataFactory.extractStatistic(metric, statTag);
      if (foundTag && (metricStatistic == null || metricStatistic.equals(StatType.VALUE)
          || metricStatistic.equals(StatType.COUNT))) {
        var val = SystemInformation.getMetricValue(metric);
        log.trace("adding {}+{} for {} {} {} {}", sum, val, metric.name(), tag,
            sid.toHostPortString(), sid.getResourceGroup());
        sum = add(sum, SystemInformation.getMetricValue(metric));

      }
    }

    if (type == Type.COMPLETED) {
      // Convert to a rate
      return computeRate(sum);
    }

    return sum;
  }

}
