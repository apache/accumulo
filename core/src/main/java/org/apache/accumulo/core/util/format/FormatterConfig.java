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

import static com.google.common.base.Preconditions.checkArgument;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Supplier;

/**
 * Holds configuration settings for a {@link Formatter}
 */
public class FormatterConfig {

  private boolean printTimestamps;
  private int shownLength;
  private Supplier<DateFormat> dateFormatSupplier;

  /** Formats with milliseconds since epoch */
  public static class DefaultDateFormat extends SimpleDateFormat {
    private static final long serialVersionUID = 1L;

    @Override
    public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
      toAppendTo.append(date.getTime());
      return toAppendTo;
    }

    @Override
    public Date parse(String source, ParsePosition pos) {
      return new Date(Long.parseLong(source));
    }
  }

  public FormatterConfig() {
    this.setPrintTimestamps(false);
    this.doNotLimitShowLength();
    this.dateFormatSupplier = DateFormatSupplier.createDefaultFormatSupplier();
  }

  /**
   * Copies most fields, but still points to other.dateFormatSupplier.
   */
  public FormatterConfig(FormatterConfig other) {
    this.printTimestamps = other.printTimestamps;
    this.shownLength = other.shownLength;
    this.dateFormatSupplier = other.dateFormatSupplier;
  }

  public boolean willPrintTimestamps() {
    return printTimestamps;
  }

  public FormatterConfig setPrintTimestamps(boolean printTimestamps) {
    this.printTimestamps = printTimestamps;
    return this;
  }

  public int getShownLength() {
    return shownLength;
  }

  public boolean willLimitShowLength() {
    return this.shownLength != Integer.MAX_VALUE;
  }

  /**
   * If given a negative number, throws an {@link IllegalArgumentException}
   *
   * @param shownLength maximum length of formatted output
   * @return {@code this} to allow chaining of set methods
   */
  public FormatterConfig setShownLength(int shownLength) {
    checkArgument(shownLength >= 0, "Shown length cannot be negative");
    this.shownLength = shownLength;
    return this;
  }

  public FormatterConfig doNotLimitShowLength() {
    this.shownLength = Integer.MAX_VALUE;
    return this;
  }

  public Supplier<DateFormat> getDateFormatSupplier() {
    return dateFormatSupplier;
  }

  /**
   * this.dateFormatSupplier points to dateFormatSupplier, so it is recommended that you create a
   * new {@code Supplier} when calling this function if your {@code Supplier} maintains some kind of
   * state (see {@link DateFormatSupplier}.
   */
  public FormatterConfig setDateFormatSupplier(Supplier<DateFormat> dateFormatSupplier) {
    this.dateFormatSupplier = dateFormatSupplier;
    return this;
  }
}
