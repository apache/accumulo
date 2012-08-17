/**
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

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * A simple formatter that print the row, column family, column qualifier, and value as hex
 */
public class HexFormatter implements Formatter {
  
  private char chars[] = new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  private Iterator<Entry<Key,Value>> iter;
  private boolean printTimestamps;
  
  private void toHex(StringBuilder sb, byte[] bin) {

    for (int i = 0; i < bin.length; i++) {
      if (i > 0 && i % 2 == 0)
        sb.append('-');
      sb.append(chars[0x0f & (bin[i] >>> 4)]);
      sb.append(chars[0x0f & bin[i]]);
    }
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }
  
  @Override
  public String next() {
    Entry<Key,Value> entry = iter.next();
    
    StringBuilder sb = new StringBuilder();
    
    toHex(sb, entry.getKey().getRowData().toArray());
    sb.append("  ");
    toHex(sb, entry.getKey().getColumnFamilyData().toArray());
    sb.append("  ");
    toHex(sb, entry.getKey().getColumnQualifierData().toArray());
    sb.append(" [");
    sb.append(entry.getKey().getColumnVisibilityData().toString());
    sb.append("] ");
    if (printTimestamps) {
      sb.append(Long.toString(entry.getKey().getTimestamp()));
      sb.append("  ");
    }
    toHex(sb, entry.getValue().get());
    
    return sb.toString();
  }
  
  @Override
  public void remove() {
    iter.remove();
  }
  
  @Override
  public void initialize(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps) {
    this.iter = scanner.iterator();
    this.printTimestamps = printTimestamps;
  }
  
}
