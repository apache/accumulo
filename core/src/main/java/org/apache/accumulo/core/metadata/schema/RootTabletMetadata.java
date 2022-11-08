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

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.EnumSet;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * This class is used to serialize and deserialize root tablet metadata using GSon. The only data
 * stored about the Root Table is the COLUMN_FAMILY, COLUMN_QUALIFIER and VALUE.
 *
 * @since 2.1.0
 */
public class RootTabletMetadata {

  private static final Logger log = LoggerFactory.getLogger(RootTabletMetadata.class);
  private static final CharsetDecoder UTF8_error_detecting_decoder = UTF_8.newDecoder();
  private static final Predicate<Entry<String,TreeMap<String,String>>> isLocationCF = e -> {
    String fam = e.getKey();
    return fam.equals(CurrentLocationColumnFamily.STR_NAME)
        || fam.equals(FutureLocationColumnFamily.STR_NAME);
  };

  // JSON Mapping Version 1. Released with Accumulo version 2.1.0
  private static final int VERSION = 1;

  // This class is used to serialize and deserialize root tablet metadata using GSon. Any changes to
  // this class must consider persisted data.
  private static class Data {
    private final int version;

    /*
     * The data is mapped using Strings as follows:
     *
     * TreeMap<column_family, TreeMap<column_qualifier, value>>
     */
    private final TreeMap<String,TreeMap<String,String>> columnValues;

    public Data(int version, TreeMap<String,TreeMap<String,String>> columnValues) {
      this.version = version;
      this.columnValues = columnValues;
    }
  }

  /**
   * The expectation is that all data stored in the root tablet can be converted to UTF8. This
   * method checks to ensure the byte sequence can be converted from byte[] to UTF8 to byte[] w/o
   * data corruption. Not all byte arrays can be converted to UTF8.
   */
  private static String bytesToUtf8(byte[] byteSequence) {
    try {
      return UTF8_error_detecting_decoder.decode(ByteBuffer.wrap(byteSequence)).toString();
    } catch (CharacterCodingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private final Gson gson = new Gson();
  private final Data data;

  public RootTabletMetadata(String json) {
    log.trace("Creating root tablet metadata from stored JSON: {}", json);
    this.data = gson.fromJson(json, Data.class);
    checkArgument(data.version == VERSION, "Invalid Root Table Metadata JSON version %s",
        data.version);
    data.columnValues.forEach((fam, qualVals) -> {
      checkArgument(!fam.isBlank(), "Blank column family in %s", data.columnValues);
      checkArgument(!qualVals.isEmpty(), "No columns in family %s", fam);
    });
  }

  public RootTabletMetadata() {
    this.data = new Data(VERSION, new TreeMap<>());
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

    m.getUpdates().forEach(cup -> {
      String fam = bytesToUtf8(cup.getColumnFamily());
      String qual = bytesToUtf8(cup.getColumnQualifier());
      String val = bytesToUtf8(cup.getValue());
      if (cup.isDeleted()) {
        data.columnValues.computeIfPresent(fam, (key, qualVals) -> {
          qualVals.remove(qual);
          return qualVals.isEmpty() ? null : qualVals;
        });
      } else {
        data.columnValues.computeIfAbsent(fam, k -> new TreeMap<>()).put(qual, val);
      }
    });

    // Ensure there is ever only one location
    if (data.columnValues.entrySet().stream().filter(isLocationCF).map(Entry::getValue)
        .mapToInt(TreeMap::size).sum() > 1) {
      throw new IllegalStateException(
          "After mutation, root tablet has multiple locations : " + m + " " + data.columnValues);
    }
  }

  /**
   * Convert this class to a {@link TabletMetadata}
   */
  public TabletMetadata toTabletMetadata() {
    String row = RootTable.EXTENT.toMetaRow().toString();
    // use a stream so we don't have to re-sort in a new TreeMap<Key,Value> structure
    Stream<SimpleImmutableEntry<Key,Value>> entries = data.columnValues.entrySet().stream()
        .flatMap(famToQualVal -> famToQualVal.getValue().entrySet().stream()
            .map(qualVal -> new SimpleImmutableEntry<>(
                new Key(row, famToQualVal.getKey(), qualVal.getKey(), 1),
                new Value(qualVal.getValue()))));
    return TabletMetadata.convertRow(entries.iterator(),
        EnumSet.allOf(TabletMetadata.ColumnType.class), false);
  }

  /**
   * @return a JSON representation of the root tablet's data.
   */
  public String toJson() {
    return gson.toJson(data);
  }

}
