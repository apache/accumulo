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
package org.apache.accumulo.master.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.Bulk;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

/**
 * Place for all bulk import serialization
 */
public class BulkSerialize {

  /**
   * Convert the SortedMap to a SortedSet of Json friendly Bulk.Mapping objects and serialize to
   * json
   */
  public static void writeLoadMapping(SortedMap<KeyExtent,Bulk.Files> loadMapping, String sourceDir,
      Table.ID tableId, String tableName, VolumeManager fs) throws ThriftTableOperationException {
    final Path lmFile = new Path(sourceDir, Constants.BULK_LOAD_MAPPING);
    // convert to a json friendly collection
    SortedSet<Bulk.Mapping> loadSet = new TreeSet<>();
    loadMapping.forEach((ke, files) -> loadSet.add(new Bulk.Mapping(ke, files)));

    try (FSDataOutputStream fsOut = fs.create(lmFile);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsOut))) {
      Gson gson = new GsonBuilder().create();
      gson.toJson(loadSet, writer);
    } catch (IOException e) {
      throw new ThriftTableOperationException(tableId.canonicalID(), tableName,
          TableOperation.BULK_IMPORT, TableOperationExceptionType.BULK_BAD_LOAD_MAPPING,
          e.getMessage());
    }
  }

  /**
   * Read Json array of Bulk.Mapping objects and return SortedMap of the bulk load mapping
   */
  public static SortedMap<KeyExtent,Bulk.Files> readLoadMapping(String bulkDir, Table.ID tableId,
      VolumeManager fs) throws AcceptableThriftTableOperationException {
    final Path lmFile = new Path(bulkDir, Constants.BULK_LOAD_MAPPING);
    SortedMap<KeyExtent,Bulk.Files> loadMapping = new TreeMap<>();
    try (FSDataInputStream fis = fs.open(lmFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis))) {
      Gson gson = new GsonBuilder().create();
      JsonParser parser = new JsonParser();
      JsonArray array = parser.parse(reader).getAsJsonArray();
      for (JsonElement jsonElement : array) {
        Bulk.Mapping bm = gson.fromJson(jsonElement, Bulk.Mapping.class);
        loadMapping.put(bm.getKeyExtent(), bm.getFiles());
      }
    } catch (Exception e) {
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), "",
          TableOperation.BULK_IMPORT, TableOperationExceptionType.BULK_BAD_LOAD_MAPPING,
          e.getMessage());
    }

    return loadMapping;
  }

  public static void writeRenameMap(Map<String,String> oldToNewNameMap, String bulkDir,
      Table.ID tableId, VolumeManager fs) throws AcceptableThriftTableOperationException {
    final Path renamingFile = new Path(bulkDir, Constants.BULK_RENAME_FILE);
    try (FSDataOutputStream fsOut = fs.create(renamingFile);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsOut))) {
      Gson gson = new GsonBuilder().create();
      gson.toJson(oldToNewNameMap, writer);
    } catch (IOException e) {
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), "",
          TableOperation.BULK_IMPORT, TableOperationExceptionType.BULK_BAD_LOAD_MAPPING,
          e.getMessage());
    }
  }

  public static Map<String,String> readRenameMap(String bulkDir, Table.ID tableId, VolumeManager fs)
      throws AcceptableThriftTableOperationException {
    final Path renamingFile = new Path(bulkDir, Constants.BULK_RENAME_FILE);
    Map<String,String> oldToNewNameMap;
    Gson gson = new GsonBuilder().create();
    try (FSDataInputStream fis = fs.open(renamingFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis))) {
      oldToNewNameMap = gson.fromJson(reader, new TypeToken<Map<String,String>>() {}.getType());
    } catch (Exception e) {
      throw new AcceptableThriftTableOperationException(tableId.canonicalID(), "",
          TableOperation.BULK_IMPORT, TableOperationExceptionType.BULK_BAD_LOAD_MAPPING,
          e.getMessage());
    }
    return oldToNewNameMap;
  }

  /**
   * Read in both maps and change all the file names in the mapping to the new names. This is needed
   * because the load mapping file was written with the original file names before they were moved
   * by BulkImportMove
   */
  public static SortedMap<KeyExtent,Bulk.Files> getUpdatedLoadMapping(String bulkDir,
      Table.ID tableId, VolumeManager fs) throws AcceptableThriftTableOperationException {
    Map<String,String> renames = readRenameMap(bulkDir, tableId, fs);
    SortedMap<KeyExtent,Bulk.Files> loadMapping = readLoadMapping(bulkDir, tableId, fs);
    return Bulk.mapNames(loadMapping, renames);
  }

  /**
   * Get a list of files to ingest from the bulk dir
   */
  public static List<String> getAllFiles(String bulkDir, Table.ID tableId, VolumeManager fs)
      throws AcceptableThriftTableOperationException {
    Map<String,String> renames = readRenameMap(bulkDir, tableId, fs);
    List<String> allFiles = new ArrayList<>();
    renames.forEach((oldName, newName) -> {
      if (!newName.endsWith(".json"))
        allFiles.add(newName);
    });
    return allFiles;
  }
}
