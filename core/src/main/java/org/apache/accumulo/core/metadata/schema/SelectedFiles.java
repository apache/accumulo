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
package org.apache.accumulo.core.metadata.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.util.time.SteadyTime;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * This class is used to manage the set of files selected for a user compaction for a tablet.
 */
public class SelectedFiles {

  private final Set<StoredTabletFile> files;
  private final boolean initiallySelectedAll;
  private final FateId fateId;
  private final int completedJobs;
  private final SteadyTime selectedTime;

  private String metadataValue;

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(SelectedFiles.class, new SelectedFilesTypeAdapter()).create();

  public SelectedFiles(Set<StoredTabletFile> files, boolean initiallySelectedAll, FateId fateId,
      SteadyTime selectedTime) {
    this(files, initiallySelectedAll, fateId, 0, selectedTime);
  }

  public SelectedFiles(Set<StoredTabletFile> files, boolean initiallySelectedAll, FateId fateId,
      int completedJobs, SteadyTime selectedTime) {
    Preconditions.checkArgument(files != null && !files.isEmpty());
    Preconditions.checkArgument(completedJobs >= 0);
    this.files = Set.copyOf(files);
    this.initiallySelectedAll = initiallySelectedAll;
    this.fateId = Objects.requireNonNull(fateId);
    this.completedJobs = completedJobs;
    this.selectedTime = Objects.requireNonNull(selectedTime);
  }

  private static class SelectedFilesTypeAdapter extends TypeAdapter<SelectedFiles> {

    // These fields could be moved to an enum but for now just using static Strings
    // seems better to avoid having to construct an enum each time the string is read
    private static final String FATE_ID = "fateId";
    private static final String SELECTED_ALL = "selAll";
    private static final String COMPLETED_JOBS = "compJobs";
    private static final String FILES = "files";
    private static final String SELECTED_TIME_NANOS = "selTimeNanos";

    @Override
    public void write(JsonWriter out, SelectedFiles selectedFiles) throws IOException {
      out.beginObject();
      out.name(FATE_ID).value(selectedFiles.getFateId().canonical());
      out.name(SELECTED_ALL).value(selectedFiles.initiallySelectedAll());
      out.name(COMPLETED_JOBS).value(selectedFiles.getCompletedJobs());
      out.name(SELECTED_TIME_NANOS).value(selectedFiles.getSelectedTime().getNanos());
      out.name(FILES).beginArray();
      // sort the data to make serialized json comparable
      selectedFiles.getFiles().stream().map(StoredTabletFile::getMetadata).sorted()
          .forEach(file -> {
            try {
              out.value(file);
            } catch (IOException e) {
              throw new UncheckedIOException(
                  "Failed to add file " + file + " to the JSON files array", e);
            }
          });
      out.endArray();
      out.endObject();
    }

    @Override
    public SelectedFiles read(JsonReader in) throws IOException {
      FateId fateId = null;
      boolean selAll = false;
      int completedJobs = 0;
      List<String> files = new ArrayList<>();
      SteadyTime selectedTime = null;

      in.beginObject();
      while (in.hasNext()) {
        String name = in.nextName();
        switch (name) {
          case FATE_ID:
            fateId = FateId.from(in.nextString());
            break;
          case SELECTED_ALL:
            selAll = in.nextBoolean();
            break;
          case COMPLETED_JOBS:
            completedJobs = in.nextInt();
            break;
          case SELECTED_TIME_NANOS:
            selectedTime = SteadyTime.from(in.nextLong(), TimeUnit.NANOSECONDS);
            break;
          case FILES:
            in.beginArray();
            while (in.hasNext()) {
              files.add(in.nextString());
            }
            in.endArray();
            break;
          default:
            throw new IllegalArgumentException("Unknown field name : " + name);
        }
      }
      in.endObject();

      Set<StoredTabletFile> tabletFiles =
          files.stream().map(StoredTabletFile::new).collect(Collectors.toSet());

      return new SelectedFiles(tabletFiles, selAll, fateId, completedJobs, selectedTime);
    }

  }

  public static SelectedFiles from(String json) {
    return GSON.fromJson(json, SelectedFiles.class);
  }

  public Set<StoredTabletFile> getFiles() {
    return files;
  }

  public boolean initiallySelectedAll() {
    return initiallySelectedAll;
  }

  public FateId getFateId() {
    return fateId;
  }

  public int getCompletedJobs() {
    return completedJobs;
  }

  public SteadyTime getSelectedTime() {
    return selectedTime;
  }

  public String getMetadataValue() {
    if (this.metadataValue == null) {
      // use the custom TypeAdapter to create the json
      this.metadataValue = GSON.toJson(this);
    }
    return this.metadataValue;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SelectedFiles other = (SelectedFiles) obj;
    return fateId.equals(other.fateId) && files.equals(other.files)
        && initiallySelectedAll == other.initiallySelectedAll;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fateId, files, initiallySelectedAll);
  }

}
