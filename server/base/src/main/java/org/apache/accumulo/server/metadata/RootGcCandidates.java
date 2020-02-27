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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collection;
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
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  // This class is used to serialize and deserialize root tablet metadata using GSon. Any changes to
  // this class must consider persisted data.
  private static class GSonData {
    int version = 1;

    // SortedMap<dir path, SortedSet<file name>>
    SortedMap<String,SortedSet<String>> candidates;
  }

  /*
   * The root tablet will only have a single dir on each volume. Therefore root file paths will have
   * a small set of unique prefixes. The following map is structured to avoid storing the same dir
   * prefix over and over in JSon and java.
   *
   * SortedMap<dir path, SortedSet<file name>>
   */
  private SortedMap<String,SortedSet<String>> candidates;

  public RootGcCandidates() {
    this.candidates = new TreeMap<>();
  }

  private RootGcCandidates(SortedMap<String,SortedSet<String>> candidates) {
    this.candidates = candidates;
  }

  public void add(Collection<StoredTabletFile> refs) {
    refs.forEach(ref -> {
      String parent = ref.getPath().getParent().toString();
      candidates.computeIfAbsent(parent, k -> new TreeSet<>()).add(ref.getFileName());
    });
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
    GSonData gd = new GSonData();
    gd.candidates = candidates;
    return GSON.toJson(gd);
  }

  public static RootGcCandidates fromJson(String json) {
    GSonData gd = GSON.fromJson(json, GSonData.class);

    Preconditions.checkArgument(gd.version == 1);

    return new RootGcCandidates(gd.candidates);
  }

  public static RootGcCandidates fromJson(byte[] json) {
    return fromJson(new String(json, UTF_8));
  }
}
