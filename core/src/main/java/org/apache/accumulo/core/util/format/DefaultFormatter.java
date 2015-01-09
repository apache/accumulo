/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.util.format;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class DefaultFormatter implements Formatter {
  private Iterator<Entry<Key,Value>> si;
  private boolean doTimestamps;
  private static final ThreadLocal<DateFormat> formatter = new ThreadLocal<DateFormat>() {
    @Override
    protected DateFormat initialValue() {
      return new DefaultDateFormat();
    }

    class DefaultDateFormat extends DateFormat {
      private static final long serialVersionUID = 1L;

      @Override
      public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
        toAppendTo.append(Long.toString(date.getTime()));
        return toAppendTo;
      }

      @Override
      public Date parse(String source, ParsePosition pos) {
        return new Date(Long.parseLong(source));
      }

    }
  };

  @Override
  public void initialize(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps) {
    checkState(false);
    si = scanner.iterator();
    doTimestamps = printTimestamps;
  }

  public boolean hasNext() {
    checkState(true);
    return si.hasNext();
  }

  public String next() {
    DateFormat timestampFormat = null;

    if (doTimestamps) {
      timestampFormat = formatter.get();
    }

    return next(timestampFormat);
  }

  protected String next(DateFormat timestampFormat) {
    checkState(true);
    return formatEntry(si.next(), timestampFormat);
  }

  public void remove() {
    checkState(true);
    si.remove();
  }

  protected void checkState(boolean expectInitialized) {
    if (expectInitialized && si == null)
      throw new IllegalStateException("Not initialized");
    if (!expectInitialized && si != null)
      throw new IllegalStateException("Already initialized");
  }

  // this should be replaced with something like Record.toString();
  public static String formatEntry(Entry<Key,Value> entry, boolean showTimestamps) {
    DateFormat timestampFormat = null;

    if (showTimestamps) {
      timestampFormat = formatter.get();
    }

    return formatEntry(entry, timestampFormat);
  }

  /* so a new date object doesn't get created for every record in the scan result */
  private static ThreadLocal<Date> tmpDate = new ThreadLocal<Date>() {
    @Override
    protected Date initialValue() {
      return new Date();
    }
  };

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

  static StringBuilder appendText(StringBuilder sb, Text t) {
    return appendBytes(sb, t.getBytes(), 0, t.getLength());
  }

  static StringBuilder appendValue(StringBuilder sb, Value value) {
    return appendBytes(sb, value.get(), 0, value.get().length);
  }

  static StringBuilder appendBytes(StringBuilder sb, byte ba[], int offset, int len) {
    for (int i = 0; i < len; i++) {
      int c = 0xff & ba[offset + i];
      if (c == '\\')
        sb.append("\\\\");
      else if (c >= 32 && c <= 126)
        sb.append((char) c);
      else
        sb.append("\\x").append(String.format("%02X", c));
    }
    return sb;
  }

  public Iterator<Entry<Key,Value>> getScannerIterator() {
    return si;
  }

  protected boolean isDoTimestamps() {
    return doTimestamps;
  }
}
