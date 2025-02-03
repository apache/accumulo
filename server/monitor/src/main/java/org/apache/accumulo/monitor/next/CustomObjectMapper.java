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
package org.apache.accumulo.monitor.next;

import jakarta.ws.rs.ext.ContextResolver;
import jakarta.ws.rs.ext.Provider;

import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.monitor.next.serializers.CumulativeDistributionSummarySerializer;
import org.apache.accumulo.monitor.next.serializers.FMetricSerializer;
import org.apache.accumulo.monitor.next.serializers.IdSerializer;
import org.apache.accumulo.monitor.next.serializers.MetricResponseSerializer;
import org.apache.accumulo.monitor.next.serializers.TabletIdSerializer;
import org.apache.accumulo.monitor.next.serializers.ThriftSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;

@Provider
public class CustomObjectMapper implements ContextResolver<ObjectMapper> {

  private final ObjectMapper mapper;

  public CustomObjectMapper() {
    // Configure the ObjectMapper
    mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addKeySerializer(Id.class, new IdSerializer());
    module.addSerializer(FMetric.class, new FMetricSerializer());
    module.addSerializer(MetricResponse.class, new MetricResponseSerializer());
    module.addSerializer(TExternalCompaction.class, new ThriftSerializer());
    module.addSerializer(TExternalCompactionJob.class, new ThriftSerializer());
    module.addSerializer(CumulativeDistributionSummary.class,
        new CumulativeDistributionSummarySerializer());
    module.addSerializer(TabletId.class, new TabletIdSerializer());
    mapper.registerModule(module);
    mapper.registerModule(new Jdk8Module());

  }

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return mapper;
  }

}
