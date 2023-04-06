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
package org.apache.accumulo.core.util.format;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.function.Supplier;

/**
 * DateFormatSupplier is a {@code ThreadLocal<DateFormat>} that will set the correct TimeZone when
 * the object is retrieved by {@link #get()}.
 *
 * This exists as a way to get around thread safety issues in {@link DateFormat}. This class also
 * contains helper methods that create some useful DateFormatSuppliers.
 *
 * Instances of DateFormatSuppliers can be shared, but note that a DateFormat generated from it will
 * be shared by all classes within a Thread.
 *
 * In general, the state of a retrieved DateFormat should not be changed, unless it makes sense to
 * only perform a state change within that Thread.
 */
public abstract class DateFormatSupplier extends ThreadLocal<DateFormat>
    implements Supplier<DateFormat> {
  private TimeZone timeZone;

  public DateFormatSupplier() {
    timeZone = TimeZone.getDefault();
  }

  public DateFormatSupplier(TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  public void setTimeZone(TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  /** Always sets the TimeZone, which is a fast operation */
  @Override
  public DateFormat get() {
    final DateFormat df = super.get();
    df.setTimeZone(timeZone);
    return df;
  }

  public static final String HUMAN_READABLE_FORMAT = "yyyy/MM/dd HH:mm:ss.SSS";

  /**
   * Create a Supplier for {@link FormatterConfig.DefaultDateFormat}s
   */
  public static DateFormatSupplier createDefaultFormatSupplier() {
    return new DateFormatSupplier() {
      @Override
      protected DateFormat initialValue() {
        return new FormatterConfig.DefaultDateFormat();
      }
    };
  }

  /** Create a generator for SimpleDateFormats accepting a dateFormat */
  public static DateFormatSupplier createSimpleFormatSupplier(final String dateFormat) {
    return new DateFormatSupplier() {
      @Override
      protected SimpleDateFormat initialValue() {
        return new SimpleDateFormat(dateFormat);
      }
    };
  }

  /** Create a generator for SimpleDateFormats accepting a dateFormat */
  public static DateFormatSupplier createSimpleFormatSupplier(final String dateFormat,
      final TimeZone timeZone) {
    return new DateFormatSupplier(timeZone) {
      @Override
      protected SimpleDateFormat initialValue() {
        return new SimpleDateFormat(dateFormat);
      }
    };
  }
}
