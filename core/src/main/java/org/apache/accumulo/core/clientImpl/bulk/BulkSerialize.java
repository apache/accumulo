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
package org.apache.accumulo.core.clientImpl.bulk;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.bulk.Bulk.Files;
import org.apache.accumulo.core.clientImpl.bulk.Bulk.Mapping;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonWriter;

/**
 * Place for all bulk import serialization code. For the objects being serialized see {@link Bulk}
 */
public class BulkSerialize {

  private static class ByteArrayToBase64TypeAdapter
      implements JsonSerializer<byte[]>, JsonDeserializer<byte[]> {

    Decoder decoder = Base64.getUrlDecoder();
    Encoder encoder = Base64.getUrlEncoder();

    @Override
    public byte[] deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {
      return decoder.decode(json.getAsString());
    }

    @Override
    public JsonElement serialize(byte[] src, Type typeOfSrc, JsonSerializationContext context) {
      return new JsonPrimitive(encoder.encodeToString(src));
    }
  }

  static Gson createGson() {
    return new GsonBuilder()
        .registerTypeHierarchyAdapter(byte[].class, new ByteArrayToBase64TypeAdapter()).create();
  }

  public interface Output {
    OutputStream create(Path path) throws IOException;
  }

  public interface Input {
    InputStream open(Path path) throws IOException;
  }

  /**
   * Serialize bulk load mapping to {@value Constants#BULK_LOAD_MAPPING}
   */
  public static void writeLoadMapping(SortedMap<KeyExtent,Bulk.Files> loadMapping, String sourceDir,
      Output output) throws IOException {
    final Path lmFile = new Path(sourceDir, Constants.BULK_LOAD_MAPPING);

    try (OutputStream fsOut = output.create(lmFile); JsonWriter writer =
        new JsonWriter(new BufferedWriter(new OutputStreamWriter(fsOut, UTF_8)))) {
      Gson gson = createGson();
      writer.setIndent("  ");
      writer.beginArray();
      Set<Entry<KeyExtent,Files>> es = loadMapping.entrySet();
      for (Entry<KeyExtent,Files> entry : es) {
        Mapping mapping = new Bulk.Mapping(entry.getKey(), entry.getValue());
        gson.toJson(mapping, Mapping.class, writer);
      }
      writer.endArray();
    }
  }

  /**
   * Read Json array of Bulk.Mapping into LoadMappingIterator
   */
  public static LoadMappingIterator readLoadMapping(String bulkDir, TableId tableId, Input input)
      throws IOException {
    final Path lmFile = new Path(bulkDir, Constants.BULK_LOAD_MAPPING);
    return new LoadMappingIterator(tableId, input.open(lmFile));
  }

  /**
   * Writes rename file to JSON. This file maps all the old names to the new names for the
   * BulkImportMove FATE operation.
   */
  public static void writeRenameMap(Map<String,String> oldToNewNameMap, String bulkDir,
      Output output) throws IOException {
    final Path renamingFile = new Path(bulkDir, Constants.BULK_RENAME_FILE);
    try (OutputStream fsOut = output.create(renamingFile);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsOut))) {
      new Gson().toJson(oldToNewNameMap, writer);
    }
  }

  /**
   * Reads the serialized rename file. This file maps all the old names to the new names for the
   * BulkImportMove FATE operation.
   */
  public static Map<String,String> readRenameMap(String bulkDir, Input input) throws IOException {
    final Path renamingFile = new Path(bulkDir, Constants.BULK_RENAME_FILE);
    Map<String,String> oldToNewNameMap;
    Gson gson = createGson();
    try (InputStream fis = input.open(renamingFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis))) {
      oldToNewNameMap = gson.fromJson(reader, new TypeToken<Map<String,String>>() {}.getType());
    }
    return oldToNewNameMap;
  }

  /**
   * Read in both maps and change all the file names in the mapping to the new names. This is needed
   * because the load mapping file was written with the original file names before they were moved
   * by BulkImportMove
   */
  public static LoadMappingIterator getUpdatedLoadMapping(String bulkDir, TableId tableId,
      Input input) throws IOException {
    Map<String,String> renames = readRenameMap(bulkDir, input);
    LoadMappingIterator lmi = readLoadMapping(bulkDir, tableId, input);
    lmi.setRenameMap(renames);
    return lmi;
  }
}
