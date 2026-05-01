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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.process.thrift.MetricResponse;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;

/**
 * Sums multiple metrics of the same type for a column value
 */
public class MultiSumColumnFactory implements ColumnFactory {

  private final List<MetricColumnFactory> colFactories;
  private final TableData.Column column;

  MultiSumColumnFactory(String label, Metric... metrics) {
    Preconditions.checkArgument(metrics.length > 1);
    // ensure all metrics are of the same type
    Preconditions
        .checkArgument(Arrays.stream(metrics).allMatch(m -> m.getType() == metrics[0].getType()));
    // ensure all metrics have the same display type
    Preconditions.checkArgument(
        Arrays.stream(metrics).allMatch(m -> Set.of(Arrays.asList(m.getColumnClasses()))
            .equals(Set.of(Arrays.asList(metrics[0].getColumnClasses())))));

    this.colFactories = Arrays.stream(metrics).map(MetricColumnFactory::new).toList();

    StringBuilder description = new StringBuilder("A sum of the following metrics :");
    var hasher = Hashing.sha256().newHasher();
    for (int i = 0; i < metrics.length; i++) {
      description.append(" ");
      description.append((i + 1) + ") " + metrics[i].getDescription());
      if (!metrics[i].getDescription().endsWith(".")) {
        description.append(".");
      }
      hasher.putString(metrics[i].getName(), UTF_8);
    }

    var key = hasher.hash().toString();

    this.column = new TableData.Column(key, label, description.toString(),
        colFactories.get(0).getColumn().uiClass());
  }

  @Override
  public TableData.Column getColumn() {
    return column;
  }

  @Override
  public Object getRowData(ServerId sid, MetricResponse mr,
      Map<String,List<FMetric>> serverMetrics) {
    Number sum = null;
    for (var colf : colFactories) {
      sum = add(sum, (Number) colf.getRowData(sid, mr, serverMetrics));
    }
    return sum;
  }
}
