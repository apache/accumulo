/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.metadata.schema;

import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * This class is used to serialize and deserialize root tablet metadata using GSon. Any changes to
 * this class must consider persisted data.
 */
public class RootGcCandidatesJson {
  // Version 1. Released with Accumulo version 2.1.0
  static final int VERSION = 1;

  // This class is used to serialize and deserialize root tablet metadata using GSon. Any changes to
  // this class must consider persisted data.
  private static class RootGcCandidatesData {
    int version = 1;

    // SortedMap<dir path, SortedSet<file name>>
    SortedMap<String,SortedSet<String>> candidates;
  }

  private final Gson GSON = new GsonBuilder().create();

  /*
   * The root tablet will only have a single dir on each volume. Therefore, root file paths will
   * have a small set of unique prefixes. The following map is structured to avoid storing the same
   * dir prefix over and over in JSon and java.
   *
   * SortedMap<dir path, SortedSet<file name>>
   */
  private final SortedMap<String,SortedSet<String>> candidates;

  public RootGcCandidatesJson() {
    this.candidates = new TreeMap<>();
  }

  public RootGcCandidatesJson(String jsonString) {
    var rootGcCandidatesData = GSON.fromJson(jsonString, RootGcCandidatesData.class);
    Preconditions.checkArgument(rootGcCandidatesData.version == VERSION);
    this.candidates = rootGcCandidatesData.candidates;
  }

  public void add(Iterator<StoredTabletFile> refs) {
    refs.forEachRemaining(ref -> {
      String parent = ref.getPath().getParent().toString();
      candidates.computeIfAbsent(parent, k -> new TreeSet<>()).add(ref.getFileName());
    });
  }

  public int getVersion() {
    return VERSION;
  }

  public void remove(Collection<String> refs) {
    refs.forEach(ref -> {
      Path path = new Path(ref);
      String parent = path.getParent().toString();
      String name = path.getName();

      SortedSet<String> names = candidates.get(parent);
      if (names != null) {
        names.remove(name);
        if (names.isEmpty()) {
          candidates.remove(parent);
        }
      }
    });
  }

  public Stream<String> stream() {
    return candidates.entrySet().stream().flatMap(entry -> {
      String parent = entry.getKey();
      SortedSet<String> names = entry.getValue();
      return names.stream().map(name -> new Path(parent, name).toString());
    });
  }

  public String toJson() {
    return GSON.toJson(this);
  }
}
