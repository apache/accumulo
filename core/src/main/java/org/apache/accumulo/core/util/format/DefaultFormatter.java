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
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class DefaultFormatter implements Formatter {
  private Iterator<Entry<Key,Value>> si;
  protected FormatterConfig config;

  /** Used as default DateFormat for some static methods */
  private static final ThreadLocal<DateFormat> formatter =
      DateFormatSupplier.createDefaultFormatSupplier();

  @Override
  public void initialize(Iterable<Entry<Key,Value>> scanner, FormatterConfig config) {
    checkState(false);
    si = scanner.iterator();
    this.config = new FormatterConfig(config);
  }

  @Override
  public boolean hasNext() {
    checkState(true);
    return si.hasNext();
  }

  @Override
  public String next() {
    checkState(true);
    return formatEntry(si.next());
  }

  @Override
  public void remove() {
    checkState(true);
    si.remove();
  }

  protected void checkState(boolean expectInitialized) {
    if (expectInitialized && si == null) {
      throw new IllegalStateException("Not initialized");
    }
    if (!expectInitialized && si != null) {
      throw new IllegalStateException("Already initialized");
    }
  }

  /**
   * if showTimestamps, will use {@link FormatterConfig.DefaultDateFormat}.<br>
   * Preferably, use
   * {@link DefaultFormatter#formatEntry(java.util.Map.Entry, org.apache.accumulo.core.util.format.FormatterConfig)}
   */
  public static String formatEntry(Entry<Key,Value> entry, boolean showTimestamps) {
    DateFormat timestampFormat = null;

    if (showTimestamps) {
      timestampFormat = formatter.get();
    }

    return formatEntry(entry, timestampFormat);
  }

  /* so a new date object doesn't get created for every record in the scan result */
  private static ThreadLocal<Date> tmpDate = ThreadLocal.withInitial(Date::new);

  /** Does not show timestamps if timestampFormat is null */
  public static String formatEntry(Entry<Key,Value> entry, DateFormat timestampFormat) {
    StringBuilder sb = new StringBuilder();
    Key key = entry.getKey();
    Text buffer = new Text();

    // append row
    appendText(sb, key.getRow(buffer)).append(" ");

    // append column family
    appendText(sb, key.getColumnFamily(buffer)).append(":");

    // append column qualifier
    appendText(sb, key.getColumnQualifier(buffer)).append(" ");

    // append visibility expression
    sb.append(new ColumnVisibility(key.getColumnVisibility(buffer)));

    // append timestamp
    if (timestampFormat != null) {
      tmpDate.get().setTime(entry.getKey().getTimestamp());
      sb.append(" ").append(timestampFormat.format(tmpDate.get()));
    }

    Value value = entry.getValue();

    // append value
    if (value != null && value.getSize() > 0) {
      sb.append("\t");
      appendValue(sb, value);
    }

    return sb.toString();
  }

  public String formatEntry(Entry<Key,Value> entry) {
    return formatEntry(entry, this.config);
  }

  public static String formatEntry(Entry<Key,Value> entry, FormatterConfig config) {
    // originally from BinaryFormatter
    StringBuilder sb = new StringBuilder();
    Key key = entry.getKey();
    Text buffer = new Text();

    final int shownLength = config.getShownLength();

    appendText(sb, key.getRow(buffer), shownLength).append(" ");
    appendText(sb, key.getColumnFamily(buffer), shownLength).append(":");
    appendText(sb, key.getColumnQualifier(buffer), shownLength).append(" ");
    sb.append(new ColumnVisibility(key.getColumnVisibility(buffer)));

    // append timestamp
    if (config.willPrintTimestamps() && config.getDateFormatSupplier() != null) {
      tmpDate.get().setTime(entry.getKey().getTimestamp());
      sb.append(" ").append(config.getDateFormatSupplier().get().format(tmpDate.get()));
    }

    // append value
    Value value = entry.getValue();
    if (value != null && value.getSize() > 0) {
      sb.append("\t");
      appendValue(sb, value, shownLength);
    }
    return sb.toString();

  }

  static StringBuilder appendText(StringBuilder sb, Text t) {
    return appendBytes(sb, t.getBytes(), 0, t.getLength());
  }

  public static StringBuilder appendText(StringBuilder sb, Text t, int shownLength) {
    return appendBytes(sb, t.getBytes(), 0, t.getLength(), shownLength);
  }

  static StringBuilder appendValue(StringBuilder sb, Value value) {
    return appendBytes(sb, value.get(), 0, value.get().length);
  }

  static StringBuilder appendValue(StringBuilder sb, Value value, int shownLength) {
    return appendBytes(sb, value.get(), 0, value.get().length, shownLength);
  }

  static StringBuilder appendBytes(StringBuilder sb, byte[] ba, int offset, int len) {
    for (int i = 0; i < len; i++) {
      int c = 0xff & ba[offset + i];
      if (c == '\\') {
        sb.append("\\\\");
      } else if (c >= 32 && c <= 126) {
        sb.append((char) c);
      } else {
        sb.append("\\x").append(String.format("%02X", c));
      }
    }
    return sb;
  }

  static StringBuilder appendBytes(StringBuilder sb, byte[] ba, int offset, int len,
      int shownLength) {
    int length = Math.min(len, shownLength);
    return DefaultFormatter.appendBytes(sb, ba, offset, length);
  }

  public Iterator<Entry<Key,Value>> getScannerIterator() {
    return si;
  }

  protected boolean isDoTimestamps() {
    return config.willPrintTimestamps();
  }

}
