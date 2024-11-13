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
package org.apache.accumulo.server.metrics;

import static org.apache.accumulo.core.metrics.MetricsInfo.HOST_TAG_KEY;
import static org.apache.accumulo.core.metrics.MetricsInfo.INSTANCE_NAME_TAG_KEY;
import static org.apache.accumulo.core.metrics.MetricsInfo.PORT_TAG_KEY;
import static org.apache.accumulo.core.metrics.MetricsInfo.PROCESS_NAME_TAG_KEY;
import static org.apache.accumulo.core.metrics.MetricsInfo.RESOURCE_GROUP_TAG_KEY;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.apache.accumulo.core.util.ByteBufferUtil;

import com.google.flatbuffers.FlatBufferBuilder;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;

/**
 * This class is not thread-safe
 */
public class MetricResponseWrapper extends MetricResponse {

  private class CommonRefs {
    int nameRef;
    int typeRef;
    int tagsRef;

    private void reset(int nameRef, int typeRef, int tagsRef) {
      this.nameRef = nameRef;
      this.typeRef = typeRef;
      this.tagsRef = tagsRef;
    }

  }

  private static final long serialVersionUID = 1L;
  private static final TimeUnit UNIT = TimeUnit.SECONDS;
  public static final String STATISTIC_TAG = "statistic";

  private transient final FlatBufferBuilder builder;
  private transient final CommonRefs common = new CommonRefs();

  public MetricResponseWrapper(FlatBufferBuilder builder) {
    this.builder = builder;
  }

  /**
   * Remove tags from the Metric that duplicate other information found in the MetricResponse
   */
  private List<Tag> reduceTags(List<Tag> tags, List<Tag> extraTags) {
    return Stream.concat(tags.stream(), extraTags.stream()).filter(t -> {
      return !t.getKey().equals(INSTANCE_NAME_TAG_KEY) && !t.getKey().equals(PROCESS_NAME_TAG_KEY)
          && !t.getKey().equals(RESOURCE_GROUP_TAG_KEY) && !t.getKey().equals(HOST_TAG_KEY)
          && !t.getKey().equals(PORT_TAG_KEY);
    }).collect(Collectors.toList());
  }

  private void parseAndCreateCommonInfo(Meter.Id id, List<Tag> extraTags) {
    builder.clear();
    final String type = id.getType().name();
    final String name = id.getName();
    final List<Tag> tags = id.getTags();

    final int nameRef = builder.createString(name);
    final int typeRef = builder.createString(type);
    final List<Tag> tagList = reduceTags(tags, extraTags);
    final int[] tagRefs = new int[tagList.size()];
    for (int idx = 0; idx < tagList.size(); idx++) {
      Tag t = tagList.get(idx);
      final int k = builder.createString(t.getKey());
      final int v = builder.createString(t.getValue());
      tagRefs[idx] = FTag.createFTag(builder, k, v);
    }
    final int tagsRef = FMetric.createTagsVector(builder, tagRefs);
    common.reset(nameRef, typeRef, tagsRef);
  }

  private void addMetric(Meter.Id id, List<Tag> extraTags, int value) {
    builder.clear();

    parseAndCreateCommonInfo(id, extraTags);

    FMetric.startFMetric(builder);
    FMetric.addName(builder, common.nameRef);
    FMetric.addType(builder, common.typeRef);
    FMetric.addTags(builder, common.tagsRef);
    FMetric.addIvalue(builder, value);
    final int metricRef = FMetric.endFMetric(builder);
    builder.finish(metricRef);
    ByteBuffer buf = builder.dataBuffer();
    // We want to copy buf as the above returns a reference
    // to a ByteBuffer that is reused by the FlatBufferBuilder
    this.addToMetrics(ByteBuffer.wrap(ByteBufferUtil.toBytes(buf)));
  }

  private void addMetric(Meter.Id id, List<Tag> extraTags, long value) {
    builder.clear();

    parseAndCreateCommonInfo(id, extraTags);

    FMetric.startFMetric(builder);
    FMetric.addName(builder, common.nameRef);
    FMetric.addType(builder, common.typeRef);
    FMetric.addTags(builder, common.tagsRef);
    FMetric.addLvalue(builder, value);
    final int metricRef = FMetric.endFMetric(builder);
    builder.finish(metricRef);
    ByteBuffer buf = builder.dataBuffer();
    // We want to copy buf as the above returns a reference
    // to a ByteBuffer that is reused by the FlatBufferBuilder
    this.addToMetrics(ByteBuffer.wrap(ByteBufferUtil.toBytes(buf)));
  }

  private void addMetric(Meter.Id id, List<Tag> extraTags, double value) {
    builder.clear();

    parseAndCreateCommonInfo(id, extraTags);

    FMetric.startFMetric(builder);
    FMetric.addName(builder, common.nameRef);
    FMetric.addType(builder, common.typeRef);
    FMetric.addTags(builder, common.tagsRef);
    FMetric.addDvalue(builder, value);
    final int metricRef = FMetric.endFMetric(builder);
    builder.finish(metricRef);
    ByteBuffer buf = builder.dataBuffer();
    // We want to copy buf as the above returns a reference
    // to a ByteBuffer that is reused by the FlatBufferBuilder
    this.addToMetrics(ByteBuffer.wrap(ByteBufferUtil.toBytes(buf)));
  }

  public Consumer<Meter> writeMeter(Meter meter) {
    for (Measurement m : meter.measure()) {
      addMetric(meter.getId().withTag(m.getStatistic()), List.of(), m.getValue());
    }
    return null;
  }

  public Consumer<FunctionTimer> writeFunctionTimer(FunctionTimer ft) {
    addMetric(ft.getId(), List.of(Tag.of(STATISTIC_TAG, "count")), ft.count());
    addMetric(ft.getId(), List.of(Tag.of(STATISTIC_TAG, "average")), ft.mean(UNIT));
    addMetric(ft.getId(), List.of(Tag.of(STATISTIC_TAG, "sum")), ft.totalTime(UNIT));
    return null;
  }

  public Consumer<Timer> writeTimer(Timer t) {
    addMetric(t.getId(), List.of(Tag.of(STATISTIC_TAG, "count")), t.count());
    addMetric(t.getId(), List.of(Tag.of(STATISTIC_TAG, "avg")), t.mean(UNIT));
    addMetric(t.getId(), List.of(Tag.of(STATISTIC_TAG, "max")), t.max(UNIT));
    addMetric(t.getId(), List.of(Tag.of(STATISTIC_TAG, "sum")), t.totalTime(UNIT));
    return null;
  }

  public Consumer<LongTaskTimer> writeLongTaskTimer(LongTaskTimer t) {
    addMetric(t.getId(), List.of(Tag.of(STATISTIC_TAG, "avg")), t.mean(UNIT));
    addMetric(t.getId(), List.of(Tag.of(STATISTIC_TAG, "max")), t.max(UNIT));
    addMetric(t.getId(), List.of(Tag.of(STATISTIC_TAG, "duration")), t.duration(UNIT));
    addMetric(t.getId(), List.of(Tag.of(STATISTIC_TAG, "active")), t.activeTasks());
    return null;
  }

  public Consumer<DistributionSummary> writeDistributionSummary(DistributionSummary d) {
    HistogramSnapshot h = d.takeSnapshot();
    ValueAtPercentile[] percentiles = h.percentileValues();
    for (ValueAtPercentile p : percentiles) {
      addMetric(d.getId(), List.of(Tag.of("percentile", Double.toString(p.percentile()))),
          p.value());
    }
    addMetric(d.getId(), List.of(Tag.of(STATISTIC_TAG, "count")), d.count());
    addMetric(d.getId(), List.of(Tag.of(STATISTIC_TAG, "avg")), d.mean());
    addMetric(d.getId(), List.of(Tag.of(STATISTIC_TAG, "max")), d.max());
    addMetric(d.getId(), List.of(Tag.of(STATISTIC_TAG, "sum")), d.totalAmount());
    return null;
  }

}
