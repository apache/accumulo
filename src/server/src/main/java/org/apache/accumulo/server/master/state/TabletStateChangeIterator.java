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
package org.apache.accumulo.server.master.state;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WholeRowIterator;
import org.apache.accumulo.core.util.AddressUtil;

public class TabletStateChangeIterator extends SkippingIterator {
  
  Set<TServerInstance> current;
  Set<String> onlineTables;
  
  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    current = parseServers(options.get("servers"));
    onlineTables = parseTables(options.get("tables"));
  }
  
  private Set<String> parseTables(String tables) {
    if (tables == null)
      return null;
    Set<String> result = new HashSet<String>();
    for (String table : tables.split(","))
      result.add(table);
    return null;
  }
  
  private Set<TServerInstance> parseServers(String servers) {
    if (servers == null)
      return null;
    // parse "host:port[INSTANCE]"
    Set<TServerInstance> result = new HashSet<TServerInstance>();
    for (String part : servers.split(",")) {
      String parts[] = part.split("\\[", 2);
      String hostport = parts[0];
      String instance = parts[1];
      if (instance != null && instance.endsWith("]"))
        instance = instance.substring(instance.length() - 1);
      result.add(new TServerInstance(AddressUtil.parseAddress(hostport, Property.TSERV_CLIENTPORT), instance));
    }
    return result;
  }
  
  @Override
  protected void consume() throws IOException {
    while (getSource().hasTop()) {
      Key k = getSource().getTopKey();
      Value v = getSource().getTopValue();
      
      if (onlineTables == null || current == null)
        return;
      SortedMap<Key,Value> decodedRow = WholeRowIterator.decodeRow(k, v);
      
      TabletLocationState tls = MetaDataTableScanner.createTabletLocationState(decodedRow);
      boolean shouldBeOnline = onlineTables.contains(tls.extent.getTableId().toString());
      
      switch (tls.getState(current)) {
        case ASSIGNED:
          return;
        case HOSTED:
          if (!shouldBeOnline)
            return;
        case ASSIGNED_TO_DEAD_SERVER:
          /* fall-through intentional */
        case UNASSIGNED:
          if (shouldBeOnline)
            return;
      }
      getSource().next();
    }
  }
  
  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }
  
}
