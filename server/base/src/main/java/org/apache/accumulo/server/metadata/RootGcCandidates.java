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
package org.apache.accumulo.server.metadata;

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

public class RootGcCandidates {
  // Version 1. Released with Accumulo version 2.1.0
  private static final int VERSION = 1;

  private final Gson gson;
  private final RootGcCandidatesData data;

  // This class is used to serialize and deserialize root tablet metadata using GSon. Any changes to
  // this class must consider persisted data.
  private static class RootGcCandidatesData {
    private final int version;

    /*
     * The root tablet will only have a single dir on each volume. Therefore, root file paths will
     * have a small set of unique prefixes. The following map is structured to avoid storing the
     * same dir prefix over and over in JSon and java.
     *
     * SortedMap<dir path, SortedSet<file name>>
     */
    private final SortedMap<String,SortedSet<String>> candidates;

    public RootGcCandidatesData(int version, SortedMap<String,SortedSet<String>> candidates) {
      this.version = version;
      this.candidates = candidates;
    }
  }

  public RootGcCandidates() {
    this.gson = new GsonBuilder().create();
    this.data = new RootGcCandidatesData(VERSION, new TreeMap<>());
  }

  public RootGcCandidates(Gson gson, String jsonString) {
    this.gson = gson;
    this.data = gson.fromJson(jsonString, RootGcCandidatesData.class);
    Preconditions.checkArgument(data.version == VERSION, "Unrecognized version %s", data.version);
    data.candidates.forEach((parent, files) -> {
      Preconditions.checkArgument(!parent.isBlank(), "Blank parent dir in %s", data.candidates);
      Preconditions.checkArgument(!files.isEmpty(), "Empty files for dir %s", parent);
    });
  }

  public void add(Stream<StoredTabletFile> refs) {
    refs.forEach(ref -> data.candidates
        .computeIfAbsent(ref.getPath().getParent().toString(), k -> new TreeSet<>())
        .add(ref.getFileName()));
  }

  public void remove(Stream<String> refs) {
    refs.map(Path::new).forEach(
        path -> data.candidates.computeIfPresent(path.getParent().toString(), (key, values) -> {
          values.remove(path.getName());
          return values.isEmpty() ? null : values;
        }));
  }

  public Stream<String> sortedStream() {
    return data.candidates.entrySet().stream().flatMap(entry -> {
      String parent = entry.getKey();
      SortedSet<String> names = entry.getValue();
      return names.stream().map(name -> new Path(parent, name));
    }).map(Path::toString).sorted();
  }

  public String toJson() {
    return gson.toJson(data);
  }

}
