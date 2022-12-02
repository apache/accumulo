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

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * @deprecated since 1.8.0; Use {@link DefaultFormatter} providing showLength and printTimestamps
 *             via {@link FormatterConfig}.
 */
@Deprecated(since = "1.8.0")
public class BinaryFormatter extends DefaultFormatter {
  // this class can probably be replaced by DefaultFormatter since DefaultFormatter has the max
  // length stuff
  @Override
  public String next() {
    checkState(true);
    return formatEntry(getScannerIterator().next(), config.willPrintTimestamps(),
        config.getShownLength());
  }

  public static String formatEntry(Entry<Key,Value> entry, boolean printTimestamps,
      int shownLength) {
    StringBuilder sb = new StringBuilder();

    Key key = entry.getKey();

    // append row
    appendText(sb, key.getRow(), shownLength).append(" ");

    // append column family
    appendText(sb, key.getColumnFamily(), shownLength).append(":");

    // append column qualifier
    appendText(sb, key.getColumnQualifier(), shownLength).append(" ");

    // append visibility expression
    sb.append(new ColumnVisibility(key.getColumnVisibility()));

    // append timestamp
    if (printTimestamps) {
      sb.append(" ").append(entry.getKey().getTimestamp());
    }

    // append value
    Value value = entry.getValue();
    if (value != null && value.getSize() > 0) {
      sb.append("\t");
      appendValue(sb, value, shownLength);
    }
    return sb.toString();
  }

}
