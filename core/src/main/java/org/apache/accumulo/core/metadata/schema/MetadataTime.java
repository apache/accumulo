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

import java.util.Objects;

import org.apache.accumulo.core.client.admin.TimeType;

/**
 * Immutable metadata time object
 */
public final class MetadataTime implements Comparable<MetadataTime> {
  private final long time;
  private final TimeType type;

  public MetadataTime(long time, TimeType type) {
    this.time = time;
    this.type = type;
  }

  /**
   * Creates a MetadataTime object from a string
   *
   * @param timestr string representation of a metatdata time, ex. "M12345678"
   * @return a MetadataTime object represented by string
   * @throws IllegalArgumentException if {@code timesstr == null} or {@code timestr.length() <= 1)}
   */

  public static MetadataTime parse(String timestr) throws IllegalArgumentException {

    if (timestr != null && timestr.length() > 1) {
      return new MetadataTime(Long.parseLong(timestr.substring(1)), getType(timestr.charAt(0)));
    } else {
      throw new IllegalArgumentException("Unknown metadata time value " + timestr);
    }
  }

  /**
   * Converts timetypes to data codes used in the table data implementation
   *
   * @param code character M or L otherwise exception thrown
   * @return a TimeType {@link TimeType} represented by code.
   */
  public static TimeType getType(char code) {
    switch (code) {
      case 'M':
        return TimeType.MILLIS;
      case 'L':
        return TimeType.LOGICAL;
      default:
        throw new IllegalArgumentException("Unknown time type code : " + code);
    }
  }

  /**
   * @return the single char code of this objects timeType
   */
  public static char getCode(TimeType type) {
    switch (type) {
      case MILLIS:
        return 'M';
      case LOGICAL:
        return 'L';
      default: // this should never happen
        throw new IllegalArgumentException("Unknown time type: " + type);
    }
  }

  public char getCode() {
    return getCode(this.type);
  }

  public String encode() {
    return "" + getCode() + time;
  }

  public TimeType getType() {
    return type;
  }

  public long getTime() {
    return time;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof MetadataTime) {
      MetadataTime t = (MetadataTime) o;
      return time == t.getTime() && type == t.getType();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(time, type);
  }

  @Override
  public int compareTo(MetadataTime mtime) {
    if (this.type.equals(mtime.getType())) {
      return Long.compare(this.time, mtime.getTime());
    } else {
      throw new IllegalArgumentException(
          "Cannot compare different time types: " + this + " and " + mtime);
    }
  }

}
