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
import java.util.stream.Collectors;

import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.metadata.StoredTabletFile;

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
  private final long fateTxId;

  private String metadataValue;

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(SelectedFiles.class, new SelectedFilesTypeAdapter()).create();

  public SelectedFiles(Set<StoredTabletFile> files, boolean initiallySelectedAll, long fateTxId) {
    Preconditions.checkArgument(files != null && !files.isEmpty());
    this.files = Set.copyOf(files);
    this.initiallySelectedAll = initiallySelectedAll;
    this.fateTxId = fateTxId;
  }

  private static class SelectedFilesTypeAdapter extends TypeAdapter<SelectedFiles> {

    @Override
    public void write(JsonWriter out, SelectedFiles selectedFiles) throws IOException {
      out.beginObject();
      out.name("txid").value(FateTxId.formatTid(selectedFiles.getFateTxId()));
      out.name("selAll").value(selectedFiles.initiallySelectedAll());
      out.name("files").beginArray();
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
      long fateTxId = 0L;
      boolean selAll = false;
      List<String> files = new ArrayList<>();

      in.beginObject();
      while (in.hasNext()) {
        String name = in.nextName();
        switch (name) {
          case "txid":
            fateTxId = FateTxId.fromString(in.nextString());
            break;
          case "selAll":
            selAll = in.nextBoolean();
            break;
          case "files":
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

      return new SelectedFiles(tabletFiles, selAll, fateTxId);
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

  public long getFateTxId() {
    return fateTxId;
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
    return fateTxId == other.fateTxId && files.equals(other.files)
        && initiallySelectedAll == other.initiallySelectedAll;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fateTxId, files, initiallySelectedAll);
  }

}
