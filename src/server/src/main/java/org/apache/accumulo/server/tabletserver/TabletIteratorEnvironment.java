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
package org.apache.accumulo.server.tabletserver;

import java.io.IOException;
import java.util.Collections;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.server.tabletserver.FileManager.ScanFileManager;

public class TabletIteratorEnvironment implements IteratorEnvironment {
  
  private ScanFileManager trm;
  private IteratorScope scope;
  private boolean fullMajorCompaction;
  private AccumuloConfiguration config;
  
  TabletIteratorEnvironment(IteratorScope scope, AccumuloConfiguration config) {
    if (scope == IteratorScope.majc)
      throw new IllegalArgumentException("must set if compaction is full");
    
    this.scope = scope;
    this.trm = null;
    this.config = config;
  }
  
  TabletIteratorEnvironment(IteratorScope scope, AccumuloConfiguration config, ScanFileManager trm) {
    if (scope == IteratorScope.majc)
      throw new IllegalArgumentException("must set if compaction is full");
    
    this.scope = scope;
    this.trm = trm;
    this.config = config;
  }
  
  TabletIteratorEnvironment(IteratorScope scope, boolean fullMajC, AccumuloConfiguration config) {
    if (scope != IteratorScope.majc)
      throw new IllegalArgumentException("Tried to set maj compaction type when scope was " + scope);
    
    this.scope = scope;
    this.trm = null;
    this.config = config;
    this.fullMajorCompaction = fullMajC;
  }
  
  @Override
  public AccumuloConfiguration getConfig() {
    return config;
  }
  
  @Override
  public IteratorScope getIteratorScope() {
    return scope;
  }
  
  @Override
  public boolean isFullMajorCompaction() {
    if (scope != IteratorScope.majc)
      throw new IllegalStateException("Asked about major compaction type when scope is " + scope);
    return fullMajorCompaction;
  }
  
  @Override
  public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
    return trm.openFiles(Collections.singleton(mapFileName), false).get(0);
  }
  
}
