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
package org.apache.accumulo.core.fate.user;

import static org.apache.accumulo.core.fate.ReadOnlyFateStore.TStatus.ALL_STATUSES;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class FateStatusFilter extends Filter {

  private EnumSet<ReadOnlyFateStore.TStatus> valuesToAccept;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    valuesToAccept = EnumSet.noneOf(ReadOnlyFateStore.TStatus.class);
    var option = options.get("statuses");
    if (!option.isBlank()) {
      for (var status : option.split(",")) {
        valuesToAccept.add(ReadOnlyFateStore.TStatus.valueOf(status));
      }
    }
  }

  @Override
  public boolean accept(Key k, Value v) {
    var tstatus = ReadOnlyFateStore.TStatus.valueOf(v.toString());
    return valuesToAccept.contains(tstatus);
  }

  public static void configureScanner(ScannerBase scanner,
      Set<ReadOnlyFateStore.TStatus> statuses) {
    // only filter when getting a subset of statuses
    if (!statuses.equals(ALL_STATUSES)) {
      String statusesStr = statuses.stream().map(Enum::name).collect(Collectors.joining(","));
      var iterSettings = new IteratorSetting(100, "statuses", FateStatusFilter.class);
      iterSettings.addOption("statuses", statusesStr);
      scanner.addScanIterator(iterSettings);
    }
  }
}
