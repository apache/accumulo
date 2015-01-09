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

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.server.master.state.TabletLocationState.BadLocationStateException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;

public class TabletStateChangeIterator extends SkippingIterator {

  private static final String SERVERS_OPTION = "servers";
  private static final String TABLES_OPTION = "tables";
  private static final String MERGES_OPTION = "merges";
  // private static final Logger log = Logger.getLogger(TabletStateChangeIterator.class);

  Set<TServerInstance> current;
  Set<String> onlineTables;
  Map<Text,MergeInfo> merges;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    current = parseServers(options.get(SERVERS_OPTION));
    onlineTables = parseTables(options.get(TABLES_OPTION));
    merges = parseMerges(options.get(MERGES_OPTION));
  }

  private Set<String> parseTables(String tables) {
    if (tables == null)
      return null;
    Set<String> result = new HashSet<String>();
    for (String table : tables.split(","))
      result.add(table);
    return result;
  }

  private Set<TServerInstance> parseServers(String servers) {
    if (servers == null)
      return null;
    // parse "host:port[INSTANCE]"
    Set<TServerInstance> result = new HashSet<TServerInstance>();
    if (servers.length() > 0) {
      for (String part : servers.split(",")) {
        String parts[] = part.split("\\[", 2);
        String hostport = parts[0];
        String instance = parts[1];
        if (instance != null && instance.endsWith("]"))
          instance = instance.substring(0, instance.length() - 1);
        result.add(new TServerInstance(AddressUtil.parseAddress(hostport, false), instance));
      }
    }
    return result;
  }

  private Map<Text,MergeInfo> parseMerges(String merges) {
    if (merges == null)
      return null;
    try {
      Map<Text,MergeInfo> result = new HashMap<Text,MergeInfo>();
      DataInputBuffer buffer = new DataInputBuffer();
      byte[] data = Base64.decodeBase64(merges.getBytes(UTF_8));
      buffer.reset(data, data.length);
      while (buffer.available() > 0) {
        MergeInfo mergeInfo = new MergeInfo();
        mergeInfo.readFields(buffer);
        result.put(mergeInfo.extent.getTableId(), mergeInfo);
      }
      return result;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected void consume() throws IOException {
    while (getSource().hasTop()) {
      Key k = getSource().getTopKey();
      Value v = getSource().getTopValue();

      if (onlineTables == null || current == null)
        return;

      TabletLocationState tls;
      try {
        tls = MetaDataTableScanner.createTabletLocationState(k, v);
        if (tls == null)
          return;
      } catch (BadLocationStateException e) {
        // maybe the master can do something with a tablet with bad/inconsistent state
        return;
      }
      // we always want data about merges
      MergeInfo merge = merges.get(tls.extent.getTableId());
      if (merge != null && merge.getExtent() != null && merge.getExtent().overlaps(tls.extent)) {
        return;
      }
      // is the table supposed to be online or offline?
      boolean shouldBeOnline = onlineTables.contains(tls.extent.getTableId().toString());

      switch (tls.getState(current)) {
        case ASSIGNED:
          // we always want data about assigned tablets
          return;
        case HOSTED:
          if (!shouldBeOnline)
            return;
        case ASSIGNED_TO_DEAD_SERVER:
          return;
        case UNASSIGNED:
          if (shouldBeOnline)
            return;
      }
      // table is in the expected state so don't bother returning any information about it
      getSource().next();
    }
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  public static void setCurrentServers(IteratorSetting cfg, Set<TServerInstance> goodServers) {
    if (goodServers != null) {
      List<String> servers = new ArrayList<String>();
      for (TServerInstance server : goodServers)
        servers.add(server.toString());
      cfg.addOption(SERVERS_OPTION, StringUtil.join(servers, ","));
    }
  }

  public static void setOnlineTables(IteratorSetting cfg, Set<String> onlineTables) {
    if (onlineTables != null)
      cfg.addOption(TABLES_OPTION, StringUtil.join(onlineTables, ","));
  }

  public static void setMerges(IteratorSetting cfg, Collection<MergeInfo> merges) {
    DataOutputBuffer buffer = new DataOutputBuffer();
    try {
      for (MergeInfo info : merges) {
        KeyExtent extent = info.getExtent();
        if (extent != null && !info.getState().equals(MergeState.NONE)) {
          info.write(buffer);
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    String encoded = Base64.encodeBase64String(Arrays.copyOf(buffer.getData(), buffer.getLength()));
    cfg.addOption(MERGES_OPTION, encoded);
  }

}
