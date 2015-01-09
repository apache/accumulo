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
package org.apache.accumulo.tserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.tserver.FileManager.ScanFileManager;
import org.apache.hadoop.fs.Path;

public class TabletIteratorEnvironment implements IteratorEnvironment {

  private final ScanFileManager trm;
  private final IteratorScope scope;
  private final boolean fullMajorCompaction;
  private final AccumuloConfiguration config;
  private final ArrayList<SortedKeyValueIterator<Key,Value>> topLevelIterators = new ArrayList<SortedKeyValueIterator<Key,Value>>();
  private Map<FileRef,DataFileValue> files;

  TabletIteratorEnvironment(IteratorScope scope, AccumuloConfiguration config) {
    if (scope == IteratorScope.majc)
      throw new IllegalArgumentException("must set if compaction is full");

    this.scope = scope;
    this.trm = null;
    this.config = config;
    this.fullMajorCompaction = false;
  }

  TabletIteratorEnvironment(IteratorScope scope, AccumuloConfiguration config, ScanFileManager trm, Map<FileRef,DataFileValue> files) {
    if (scope == IteratorScope.majc)
      throw new IllegalArgumentException("must set if compaction is full");

    this.scope = scope;
    this.trm = trm;
    this.config = config;
    this.fullMajorCompaction = false;
    this.files = files;
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
    FileRef ref = new FileRef(mapFileName, new Path(mapFileName));
    return trm.openFiles(Collections.singletonMap(ref, files.get(ref)), false).get(0);
  }

  @Override
  public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
    topLevelIterators.add(iter);
  }

  SortedKeyValueIterator<Key,Value> getTopLevelIterator(SortedKeyValueIterator<Key,Value> iter) {
    if (topLevelIterators.isEmpty())
      return iter;
    ArrayList<SortedKeyValueIterator<Key,Value>> allIters = new ArrayList<SortedKeyValueIterator<Key,Value>>(topLevelIterators);
    allIters.add(iter);
    return new MultiIterator(allIters, false);
  }
}
