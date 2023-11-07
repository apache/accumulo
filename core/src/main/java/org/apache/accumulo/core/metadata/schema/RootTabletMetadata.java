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
import static org.apache.accumulo.core.util.LazySingletons.GSON;

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
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final int VERSION_1 = 1;
  // JSON Mapping Version 2. Released with Accumulo version 3,1
  private static final int VERSION_2 = 2;
  private static final int VERSION = VERSION_2;

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

    private Data(int version, TreeMap<String,TreeMap<String,String>> columnValues) {
      this.version = version;
      this.columnValues = columnValues;
    }

    public int getVersion() {
      return version;
    }

    public static boolean needsUpgrade(final String json) {
      var rootData = GSON.get().fromJson(json, Data.class);
      int currVersion = rootData.getVersion();
      return currVersion < VERSION;
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

  private final Data data;

  public RootTabletMetadata(String json) {
    this(GSON.get().fromJson(json, Data.class));
  }

  private RootTabletMetadata(final Data data) {
    this.data = data;
    checkArgument(data.version == VERSION, "Invalid Root Table Metadata JSON version %s",
        data.version);
    data.columnValues.forEach((fam, qualVals) -> {
      checkArgument(!fam.isBlank(), "Blank column family in %s", data.columnValues);
      checkArgument(!qualVals.isEmpty(), "No columns in family %s", fam);
    });
  }

  public RootTabletMetadata() {
    data = new Data(VERSION, new TreeMap<>());
  }

  public static RootTabletMetadata upgrade(final String json) {
    Data data = GSON.get().fromJson(json, Data.class);
    int currVersion = data.getVersion();
    switch (currVersion) {
      case VERSION_1:
        RootTabletMetadata rtm = new RootTabletMetadata();
        Mutation m = convert1To2(data);
        rtm.update(m);
        return rtm;
      case VERSION_2:
        log.debug("no metadata version conversion required for {}", currVersion);
        return new RootTabletMetadata(data);
      default:
        throw new IllegalArgumentException("Unsupported data version: " + currVersion);
    }
  }

  private static Mutation convert1To2(final Data data) {
    Mutation mutation =
        MetadataSchema.TabletsSection.TabletColumnFamily.createPrevRowMutation(RootTable.EXTENT);
    data.columnValues.forEach((colFam, colQuals) -> {
      if (colFam.equals(MetadataSchema.TabletsSection.DataFileColumnFamily.STR_NAME)) {
        colQuals.forEach((colQual, value) -> {
          mutation.put(colFam, StoredTabletFile.serialize(colQual), value);
        });
      } else {
        colQuals.forEach((colQual, value) -> {
          mutation.put(colFam, colQual, value);
        });
      }
    });
    return mutation;
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

  public static boolean needsUpgrade(final String json) {
    return Data.needsUpgrade(json);
  }

  /**
   * @return a JSON representation of the root tablet's data.
   */
  public String toJson() {
    return GSON.get().toJson(data);
  }

}
