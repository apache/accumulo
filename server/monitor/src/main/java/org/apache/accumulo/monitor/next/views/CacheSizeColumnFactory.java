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

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.monitor.next.SystemInformation;

public class CacheSizeColumnFactory implements ColumnFactory {

  private final String cacheName;
  private final TableData.Column column;

  CacheSizeColumnFactory(String cacheName, String colHeader, String description) {
    this.cacheName = cacheName;
    Preconditions.checkState(Metric.CACHE_SIZE.getColumnClasses().length == 1);
    this.column = new TableData.Column(Metric.CACHE_SIZE.getName() + "_" + cacheName, colHeader,
        description, Metric.CACHE_SIZE.getColumnClasses()[0].getCssClass());
  }

  @Override
  public TableData.Column getColumn() {
    return column;
  }

  @Override
  public Object getRowData(ServerId sid, MetricResponse mr,
      Map<String,List<FMetric>> serverMetrics) {
    var tag = new FTag();
    for (var metric : serverMetrics.getOrDefault(Metric.CACHE_SIZE.getName(), List.of())) {
      for (int i = 0; i < metric.tagsLength(); i++) {
        metric.tags(tag, i);
        if ("cache".equals(tag.key()) && cacheName.equals(tag.value())) {
          return SystemInformation.getMetricValue(metric);
        }
      }
    }
    return null;
  }
}
