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

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;
import org.apache.accumulo.core.iteratorsImpl.system.TimeSettingIterator;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "CT_CONSTRUCTOR_THROW",
    justification = "Constructor validation is required for proper initialization")
public class DataFileValue {
  private final long size;
  private final long numEntries;
  private long time = -1;
  private boolean shared;

  public DataFileValue(long size, long numEntries, long time, boolean shared) {
    this.size = size;
    this.numEntries = numEntries;
    this.time = time;
    this.shared = shared;
  }

  public DataFileValue(long size, long numEntries, boolean shared) {
    this.size = size;
    this.numEntries = numEntries;
    this.time = -1;
    this.shared = shared;
  }

  public DataFileValue(long size, long numEntries, long time) {
    this.size = size;
    this.numEntries = numEntries;
    this.time = time;
    this.shared = false;
  }

  public DataFileValue(long size, long numEntries) {
    this.size = size;
    this.numEntries = numEntries;
    this.time = -1;
    this.shared = false;
  }

  public DataFileValue(String encodedDFV) {
    String[] ba = encodedDFV.split(",");

    size = Long.parseLong(ba[0]);
    numEntries = Long.parseLong(ba[1]);
    time = -1;
    shared = false;

    if (ba.length == 3) {
      // Could be old format with time, or new format with shared
      try {
        time = Long.parseLong(ba[2]);
      } catch (NumberFormatException e) {
        shared = Boolean.parseBoolean(ba[2]);
      }
    } else if (ba.length == 4) {
      shared = Boolean.parseBoolean(ba[2]);
      time = Long.parseLong(ba[3]);
    }
  }

  public DataFileValue(byte[] encodedDFV) {
    this(new String(encodedDFV, UTF_8));
  }

  public long getSize() {
    return size;
  }

  public long getNumEntries() {
    return numEntries;
  }

  public boolean isTimeSet() {
    return time >= 0;
  }

  public long getTime() {
    return time;
  }

  public boolean isShared() {
    return shared;
  }

  public byte[] encode() {
    return encodeAsString().getBytes(UTF_8);
  }

  public String encodeAsString() {
    if (time >= 0) {
      return ("" + size + "," + numEntries + "," + shared + "," + time);
    }
    return ("" + size + "," + numEntries + "," + shared);
  }

  public Value encodeAsValue() {
    return new Value(encode());
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof DataFileValue odfv) {

      return size == odfv.size && numEntries == odfv.numEntries && time == odfv.time
          && shared == odfv.shared;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(size + numEntries + (shared ? 1 : 0)).hashCode();
  }

  @Override
  public String toString() {
    return size + " " + numEntries + " " + shared;
  }

  public void setTime(long time) {
    if (time < 0) {
      throw new IllegalArgumentException();
    }
    this.time = time;
  }

  public void setShared(boolean shared) {
    this.shared = shared;
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
