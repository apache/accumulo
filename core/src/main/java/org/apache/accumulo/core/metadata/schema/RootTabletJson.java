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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * This class is used to serialize and deserialize root tablet metadata using GSon. Any changes to
 * this class must consider persisted data. The only data stored about the Root Table is the
 * COLUMN_FAMILY, COLUMN_QUALIFIER and VALUE. The data is mapped using Strings as follows:
 *
 * {@code Map<column_family, Map<column_qualifier, value>> columnValues = new TreeMap<>();}
 *
 * @since 2.1.0
 */
public class RootTabletJson {
  // JSON Mapping Version 1. Released with Accumulo version 2.1.0
  public final int VERSION = 1;

  // Internal class used to serialize and deserialize root tablet metadata using GSon. Any changes
  // to
  // this class must consider persisted data.
  private static class RootTabletData {
    int version = 1;

    // Map<column_family, Map<column_qualifier, value>>
    Map<String,Map<String,String>> columnValues;
  }

  private static final ByteSequence CURR_LOC_FAM =
      new ArrayByteSequence(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.STR_NAME);
  private static final ByteSequence FUTURE_LOC_FAM =
      new ArrayByteSequence(MetadataSchema.TabletsSection.FutureLocationColumnFamily.STR_NAME);
  private static final Logger log = LoggerFactory.getLogger(RootTabletJson.class);
  static final Gson GSON = new GsonBuilder().create();

  // In memory representation of the Json data
  private final TreeMap<Key,Value> entries;

  public RootTabletJson(String json) {
    log.debug("Creating object from json: {}", json);
    var rootTabletData = GSON.fromJson(json, RootTabletData.class);

    checkArgument(rootTabletData.version == VERSION, "Invalid Root Tablet JSON version");

    String row = RootTable.EXTENT.toMetaRow().toString();

    this.entries = new TreeMap<>();

    // convert each of the JSON values into Key Values in memory
    rootTabletData.columnValues.forEach((fam, qualVals) -> {
      qualVals.forEach((qual, val) -> {
        Key k = new Key(row, fam, qual, 1);
        Value v = new Value(val);

        entries.put(k, v);
      });
    });
  }

  public RootTabletJson() {
    this.entries = new TreeMap<>();
  }

  public int getVersion() {
    return VERSION;
  }

  /**
   * Convert this class to a {@link TabletMetadata}
   */
  public TabletMetadata toTabletMetadata() {
    return TabletMetadata.convertRow(entries.entrySet().iterator(),
        EnumSet.allOf(TabletMetadata.ColumnType.class), false);
  }

  /**
   * @return a json representation of the rootTabletData.
   */
  public String toJson() {
    RootTabletData rootTabletData = new RootTabletData();
    rootTabletData.columnValues = new TreeMap<>();

    var entrySet = entries.entrySet();
    for (var entry : entrySet) {
      String fam = bytesToUtf8(entry.getKey().getColumnFamilyData().toArray());
      String qual = bytesToUtf8(entry.getKey().getColumnQualifierData().toArray());
      String val = bytesToUtf8(entry.getValue().get());

      rootTabletData.columnValues.computeIfAbsent(fam, k -> new TreeMap<>()).put(qual, val);
    }

    return GSON.toJson(rootTabletData);
  }

  /**
   * The expectation is that all data stored in the root tablet can be converted to UTF8. This
   * method checks to ensure the byte sequence can be converted from byte[] to UTF8 to byte[] w/o
   * data corruption. Not all byte arrays can be converted to UTF8.
   */
  private String bytesToUtf8(byte[] byteSequence) {
    String str = new String(byteSequence, UTF_8);
    checkArgument(Arrays.equals(byteSequence, str.getBytes(UTF_8)),
        "Unsuccessful conversion of %s to utf8", str);
    return str;
  }

  /**
   * Apply a metadata table mutation to update internal entries.
   */
  public void update(Mutation m) {
    checkArgument(new Text(m.getRow()).equals(RootTable.EXTENT.toMetaRow()),
        "Invalid Root Table Row " + new Text(m.getRow()));

    m.getUpdates().forEach(cup -> {
      checkArgument(!cup.hasTimestamp(), "Root Table timestamp must be empty.");
      checkArgument(cup.getColumnVisibility().length == 0, "Root Table visibility must be empty.");
    });

    for (ColumnUpdate cup : m.getUpdates()) {
      Key newKey = new Key(m.getRow(), cup.getColumnFamily(), cup.getColumnQualifier(),
          cup.getColumnVisibility(), 1, false, false);

      if (cup.isDeleted()) {
        entries.remove(newKey);
      } else {
        entries.put(newKey, new Value(cup.getValue()));
      }
    }

    // Ensure there is ever only one location
    long locsSeen = entries.keySet().stream().map(Key::getColumnFamilyData)
        .filter(fam -> fam.equals(CURR_LOC_FAM) || fam.equals(FUTURE_LOC_FAM)).count();

    if (locsSeen > 1) {
      throw new IllegalStateException(
          "After mutation, root tablet has multiple locations : " + m + " " + entries);
    }
  }

  /**
   * Assumes the byte Array is UTF8 encoded.
   */
  public static RootTabletJson fromJson(byte[] bs) {
    return new RootTabletJson(new String(bs, UTF_8));
  }
}
