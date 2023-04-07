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
package org.apache.accumulo.core.metadata.schema;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;
import org.apache.accumulo.core.iteratorsImpl.system.TimeSettingIterator;
import org.apache.accumulo.core.util.json.RangeAdapter;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class DataFileValue {

  private static final Gson gson = RangeAdapter.createRangeGson();

  private final long size;
  private final long numEntries;
  private long time = -1;
  private final List<Range> ranges;

  public DataFileValue(final long size, final long numEntries, final long time) {
    this(size, numEntries, time, null);
  }

  public DataFileValue(final long size, final long numEntries) {
    this(size, numEntries, null);
  }

  public DataFileValue(final long size, final long numEntries, final long time,
      final Collection<Range> ranges) {
    this.size = size;
    this.numEntries = numEntries;
    this.time = time;
    // If ranges is null then just set to empty list and also merge overlapping to reduce
    // the data stored if possible.
    this.ranges = Optional.ofNullable(ranges).map(Range::mergeOverlapping)
        .map(Collections::unmodifiableList).orElse(List.of());
  }

  public DataFileValue(long size, long numEntries, Collection<Range> ranges) {
    this(size, numEntries, -1, ranges);
  }

  public static DataFileValue decode(String encodedDFV) {
    try {
      // Optimistically try and decode from Json and fall back to decoding the legacy format
      // if json parsing fails. Over time the old format will be replaced with the new format
      // as new values are written or updated.
      return gson.fromJson(encodedDFV, DataFileValue.class);
    } catch (JsonSyntaxException e) {
      return decodeLegacy(encodedDFV);
    }
  }

  public static DataFileValue decode(byte[] encodedDFV) {
    return decode(new String(encodedDFV, UTF_8));
  }

  /**
   * Decodes the original CSV format for DataFileValue
   */
  private static DataFileValue decodeLegacy(final String encodedDFV) {
    String[] ba = encodedDFV.split(",");
    long size = Long.parseLong(ba[0]);
    long numEntries = Long.parseLong(ba[1]);
    long time = ba.length == 3 ? Long.parseLong(ba[2]) : -1;
    return new DataFileValue(size, numEntries, time);
  }

  public long getSize() {
    return size;
  }

  public long getNumEntries() {
    return numEntries;
  }

  public List<Range> getRanges() {
    return ranges;
  }

  public boolean isTimeSet() {
    return time >= 0;
  }

  public long getTime() {
    return time;
  }

  public byte[] encode() {
    return encodeAsString().getBytes(UTF_8);
  }

  public String encodeAsString() {
    return gson.toJson(this);
  }

  public Value encodeAsValue() {
    return new Value(encode());
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof DataFileValue) {
      DataFileValue odfv = (DataFileValue) o;

      return size == odfv.size && numEntries == odfv.numEntries;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(size + numEntries).hashCode();
  }

  @Override
  public String toString() {
    return size + " " + numEntries;
  }

  public void setTime(long time) {
    if (time < 0) {
      throw new IllegalArgumentException();
    }
    this.time = time;
  }

  /**
   * @return true if {@link #wrapFileIterator} would wrap a given iterator, false otherwise.
   */
  public boolean willWrapIterator() {
    return isTimeSet();
  }

  /**
   * Use per file information from the metadata table to wrap the raw iterator over a file with
   * iterators that may take action based on data set in the metadata table.
   */
  public InterruptibleIterator wrapFileIterator(InterruptibleIterator iter) {
    if (isTimeSet()) {
      return new TimeSettingIterator(iter, getTime());
    } else {
      return iter;
    }
  }

}
