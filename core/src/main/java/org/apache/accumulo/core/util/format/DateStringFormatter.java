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
import java.text.SimpleDateFormat;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class DateStringFormatter implements Formatter {
  private boolean printTimestamps = false;
  private DefaultFormatter defaultFormatter = new DefaultFormatter();

  public static final String DATE_FORMAT = "yyyy/MM/dd HH:mm:ss.SSS";
  // SimpleDataFormat is not thread safe
  private static final ThreadLocal<DateFormat> formatter = new ThreadLocal<DateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat(DATE_FORMAT);
    }
  };

  @Override
  public void initialize(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps) {
    this.printTimestamps = printTimestamps;
    defaultFormatter.initialize(scanner, printTimestamps);
  }

  @Override
  public boolean hasNext() {
    return defaultFormatter.hasNext();
  }

  @Override
  public String next() {
    DateFormat timestampformat = null;

    if (printTimestamps) {
      timestampformat = formatter.get();
    }

    return defaultFormatter.next(timestampformat);
  }

  @Override
  public void remove() {
    defaultFormatter.remove();
  }

  public void setTimeZone(TimeZone zone) {
    formatter.get().setTimeZone(zone);
  }
}
