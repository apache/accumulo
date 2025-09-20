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
package org.apache.accumulo.core.clientImpl;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.accumulo.core.client.admin.TabletMergeabilityInfo;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl.SplitMergeability;
import org.apache.accumulo.core.manager.thrift.TTabletMergeability;
import org.apache.accumulo.core.metadata.schema.TabletMergeabilityMetadata;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.json.ByteArrayToBase64TypeAdapter;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.gson.Gson;

public class TabletMergeabilityUtil {

  private static final Gson gson = ByteArrayToBase64TypeAdapter.createBase64Gson();

  public static SortedMap<Text,TabletMergeability> userDefaultSplits(SortedSet<Text> splits) {
    return splitsWithDefault(splits, TabletMergeability.never());
  }

  public static SortedMap<Text,TabletMergeability> systemDefaultSplits(SortedSet<Text> splits) {
    return splitsWithDefault(splits, TabletMergeability.always());
  }

  @SuppressWarnings("unchecked")
  public static SortedMap<Text,TabletMergeability> splitsWithDefault(SortedSet<Text> splits,
      TabletMergeability tabletMergeability) {
    Objects.requireNonNull(tabletMergeability);
    return splits.stream()
        .collect(ImmutableSortedMap.toImmutableSortedMap(Optional
            .ofNullable((Comparator<Text>) splits.comparator()).orElse(Comparator.naturalOrder()),
            Function.identity(), t -> tabletMergeability));
  }

  public static ByteBuffer encodeAsBuffer(Text split, TabletMergeability tabletMergeability) {
    return ByteBuffer.wrap(encode(split, tabletMergeability).getBytes(UTF_8));
  }

  // Encode A split and TabletMergeability as json. The split will be Base64 encoded
  public static String encode(Text split, TabletMergeability tabletMergeability) {
    GSonData jData = new GSonData();
    jData.split = TextUtil.getBytes(split);
    jData.never = tabletMergeability.isNever();
    jData.delay = tabletMergeability.getDelay().map(Duration::toNanos).orElse(null);
    return gson.toJson(jData);
  }

  public static ByteBuffer encodeAsBuffer(SplitMergeability sm) {
    return ByteBuffer.wrap(encode(sm.split(), sm.mergeability()).getBytes(UTF_8));
  }

  public static SplitMergeability decode(ByteBuffer data) {
    return decode(ByteBufferUtil.toString(data));
  }

  public static SplitMergeability decode(String data) {
    GSonData jData = gson.fromJson(data, GSonData.class);
    Preconditions.checkArgument(jData.never == (jData.delay == null),
        "delay should both be set if and only if mergeability 'never' is false");
    return new SplitMergeability(new Text(jData.split), jData.never ? TabletMergeability.never()
        : TabletMergeability.after(Duration.ofNanos(jData.delay)));
  }

  public static TabletMergeability fromThrift(TTabletMergeability thriftTm) {
    if (thriftTm.never) {
      return TabletMergeability.never();
    }
    return TabletMergeability.after(Duration.ofNanos(thriftTm.delay));
  }

  public static TTabletMergeability toThrift(TabletMergeability tabletMergeability) {
    if (tabletMergeability.isNever()) {
      return new TTabletMergeability(true, -1L);
    }
    return new TTabletMergeability(false, tabletMergeability.getDelay().orElseThrow().toNanos());
  }

  public static TabletMergeabilityInfo toInfo(TabletMergeabilityMetadata metadata,
      Supplier<Duration> currentTime) {
    return new TabletMergeabilityInfo(metadata.getTabletMergeability(),
        metadata.getSteadyTime().map(SteadyTime::getDuration), currentTime);
  }

  public static boolean isMergeable(Duration createdTime, Duration delay, Duration currentTime) {
    var totalDelay = createdTime.plus(delay);
    return currentTime.compareTo(totalDelay) >= 0;
  }

  private static class GSonData {
    byte[] split;
    boolean never;
    Long delay;
  }

}
