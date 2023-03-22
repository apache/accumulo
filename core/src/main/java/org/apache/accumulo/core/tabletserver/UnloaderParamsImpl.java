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
package org.apache.accumulo.core.tabletserver;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.spi.ondemand.OnDemandTabletUnloader.UnloaderParams;

public class UnloaderParamsImpl implements UnloaderParams {

  private final Map<String,String> conf;
  private final SortedMap<TabletId,Long> online;
  private final Set<KeyExtent> unloads;

  public UnloaderParamsImpl(AccumuloConfiguration conf, Map<KeyExtent,AtomicLong> online,
      Set<KeyExtent> unload) {
    this.conf = new HashMap<>();
    conf.forEach((e) -> this.conf.put(e.getKey(), e.getValue()));
    this.online = new TreeMap<>();
    online.forEach((k, v) -> this.online.put(new TabletIdImpl(k), v.get()));
    this.unloads = unload;
  }

  @Override
  public Map<String,String> getTableConfiguration() {
    return conf;
  }

  @Override
  public SortedMap<TabletId,Long> getOnDemandTablets() {
    return online;
  }

  @Override
  public void setOnDemandTabletsToUnload(Set<TabletId> tablets) {
    tablets.forEach(t -> unloads.add(KeyExtent.fromTabletId(t)));
  }

}
