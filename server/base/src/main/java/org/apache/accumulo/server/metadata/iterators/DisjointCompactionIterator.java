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
package org.apache.accumulo.server.metadata.iterators;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.util.LazySingletons;
import org.apache.accumulo.server.metadata.ConditionalTabletMutatorImpl;

import com.google.common.base.Preconditions;
import com.google.gson.reflect.TypeToken;

/**
 * An iterator used with conditional updates to tablets to check that for a given set of files none
 * of them are currently compacting.
 */
public class DisjointCompactionIterator extends ColumnFamilyTransformationIterator {

  private Set<StoredTabletFile> filesToCompact;

  private static final String DISJOINT = "disjoint";
  private static final String OVERLAPS = "overlaps";
  private static final String FILES_KEY = "files";

  @Override
  protected Value transform(SortedKeyValueIterator<Key,Value> source) throws IOException {
    while (source.hasTop()) {
      Preconditions.checkState(
          source.getTopKey().getColumnFamily().equals(ExternalCompactionColumnFamily.NAME));
      var compactionMetadata = CompactionMetadata.fromJson(source.getTopValue().toString());
      if (!Collections.disjoint(filesToCompact, compactionMetadata.getJobFiles())) {
        return new Value(OVERLAPS);
      }
      source.next();
    }
    return new Value(DISJOINT);
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    filesToCompact = decode(options.get(FILES_KEY));
  }

  /**
   * Creates a condition that will only pass if the given files are disjoint with all files
   * currently compacting for a tablet.
   */
  public static Condition createCondition(Set<StoredTabletFile> filesToCompact) {
    Preconditions.checkArgument(!filesToCompact.isEmpty());
    IteratorSetting is = new IteratorSetting(ConditionalTabletMutatorImpl.INITIAL_ITERATOR_PRIO,
        DisjointCompactionIterator.class);
    is.addOption(FILES_KEY, encode(filesToCompact));
    return new Condition(ExternalCompactionColumnFamily.NAME, EMPTY).setValue(DISJOINT)
        .setIterators(is);
  }

  private static String encode(Set<StoredTabletFile> filesToCompact) {
    var files =
        filesToCompact.stream().map(StoredTabletFile::getMetadata).collect(Collectors.toSet());
    return LazySingletons.GSON.get().toJson(files);
  }

  private Set<StoredTabletFile> decode(String s) {
    Type fileSetType = new TypeToken<HashSet<String>>() {}.getType();
    Set<String> files = LazySingletons.GSON.get().fromJson(s, fileSetType);
    return files.stream().map(StoredTabletFile::of).collect(Collectors.toSet());
  }
}
