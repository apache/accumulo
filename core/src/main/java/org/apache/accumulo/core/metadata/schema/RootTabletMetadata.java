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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Serializes the root tablet metadata as Json using Accumulo's standard metadata table schema.
 */
public class RootTabletMetadata {

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private static final ByteSequence CURR_LOC_FAM =
      new ArrayByteSequence(TabletsSection.CurrentLocationColumnFamily.STR_NAME);
  private static final ByteSequence FUTURE_LOC_FAM =
      new ArrayByteSequence(TabletsSection.FutureLocationColumnFamily.STR_NAME);

  private TreeMap<Key,Value> entries = new TreeMap<>();

  // This class is used to serialize and deserialize root tablet metadata using GSon. Any changes to
  // this class must consider persisted data.
  private static class GSonData {
    int version = 1;

    // Map<column_family, Map<column_qualifier, value>>
    Map<String,Map<String,String>> columnValues;
  }

  /**
   * Apply a metadata table mutation to update internal json.
   */
  public void update(Mutation m) {
    Preconditions.checkArgument(new Text(m.getRow()).equals(RootTable.EXTENT.getMetadataEntry()));

    m.getUpdates().forEach(cup -> {
      Preconditions.checkArgument(!cup.hasTimestamp());
      Preconditions.checkArgument(cup.getColumnVisibility().length == 0);
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
   * Convert json to tablet metadata. *
   */
  public TabletMetadata convertToTabletMetadata() {
    return TabletMetadata.convertRow(entries.entrySet().iterator(), EnumSet.allOf(ColumnType.class),
        false);
  }

  private static String bs2Str(byte[] bs) {
    String str = new String(bs, UTF_8);

    // The expectation is that all data stored in the root tablet can be converted to UTF8. This is
    // a sanity check to ensure the byte sequence can be converted from byte[] to UTF8 to byte[] w/o
    // data corruption. Not all byte arrays can be converted to UTF8.
    Preconditions.checkArgument(Arrays.equals(bs, str.getBytes(UTF_8)),
        "Unsuccessful conversion of %s to utf8", str);

    return str;
  }

  /**
   * @return a json representation of this object, use {@link #fromJson(String)} to convert the json
   *         back to an object.
   */
  public String toJson() {
    GSonData gd = new GSonData();
    gd.columnValues = new TreeMap<>();

    Set<Entry<Key,Value>> es = entries.entrySet();
    for (Entry<Key,Value> entry : es) {
      String fam = bs2Str(entry.getKey().getColumnFamilyData().toArray());
      String qual = bs2Str(entry.getKey().getColumnQualifierData().toArray());
      String val = bs2Str(entry.getValue().get());

      gd.columnValues.computeIfAbsent(fam, k -> new TreeMap<>()).put(qual, val);
    }

    return GSON.toJson(gd);
  }

  /**
   * Converts created by calling {@link #toJson()} back to an object.
   */
  public static RootTabletMetadata fromJson(String json) {
    GSonData gd = GSON.fromJson(json, GSonData.class);

    Preconditions.checkArgument(gd.version == 1);

    String row = RootTable.EXTENT.getMetadataEntry().toString();

    TreeMap<Key,Value> entries = new TreeMap<>();

    gd.columnValues.forEach((fam, qualVals) -> {
      qualVals.forEach((qual, val) -> {
        Key k = new Key(row, fam, qual, 1);
        Value v = new Value(val);

        entries.put(k, v);
      });
    });

    RootTabletMetadata rtm = new RootTabletMetadata();
    rtm.entries = entries;

    return rtm;
  }

  /**
   * Converts created by calling {@link #toJson()} back to an object. Assumes the json is UTF8
   * encoded.
   */
  public static RootTabletMetadata fromJson(byte[] bs) {
    return fromJson(new String(bs, UTF_8));
  }

  /**
   * Generate initial json for the root tablet metadata.
   */
  public static byte[] getInitialJson(String dirName, String file) {
    ServerColumnFamily.validateDirCol(dirName);
    Mutation mutation = RootTable.EXTENT.getPrevRowUpdateMutation();
    ServerColumnFamily.DIRECTORY_COLUMN.put(mutation, new Value(dirName));

    mutation.put(DataFileColumnFamily.STR_NAME, file, new DataFileValue(0, 0).encodeAsValue());

    ServerColumnFamily.TIME_COLUMN.put(mutation,
        new Value(new MetadataTime(0, TimeType.LOGICAL).encode()));

    RootTabletMetadata rtm = new RootTabletMetadata();

    rtm.update(mutation);

    return rtm.toJson().getBytes(UTF_8);
  }
}
