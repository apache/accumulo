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
package org.apache.accumulo.core.metadata;

import static org.apache.accumulo.core.util.RowRangeUtil.requireKeyExtentDataRange;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Comparator;
import java.util.Objects;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.json.ByteArrayToBase64TypeAdapter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;

/**
 * Object representing a tablet file entry stored in the metadata table. Keeps a string of the exact
 * entry of what is in the metadata table, which is important for updating and deleting metadata
 * entries. If the exact string is not used, erroneous entries can pollute the metadata table. The
 * main column qualifier used can be found:
 * {@link org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily}
 * The tablet file entry is also stored for scans and bulk imports.
 * <p>
 * As of 2.1, Tablet file paths should now be only absolute URIs with the removal of relative paths
 * in Upgrader9to10.upgradeRelativePaths()
 */
public class StoredTabletFile extends AbstractTabletFile<StoredTabletFile> {
  private final String metadataEntry;
  private final ReferencedTabletFile referencedTabletFile;
  private final String metadataEntryPath;

  private static final Comparator<StoredTabletFile> comparator = Comparator
      .comparing(StoredTabletFile::getMetadataPath).thenComparing(StoredTabletFile::getRange);

  /**
   * Construct a tablet file using the string read from the metadata. Preserve the exact string so
   * the entry can be deleted.
   */
  public StoredTabletFile(String metadataEntry) {
    this(metadataEntry, deserialize(metadataEntry));
  }

  private StoredTabletFile(TabletFileCq fileCq) {
    this(serialize(fileCq), fileCq);
  }

  private StoredTabletFile(String metadataEntry, TabletFileCq fileCq) {
    super(Objects.requireNonNull(fileCq).path, fileCq.range);
    this.metadataEntry = Objects.requireNonNull(metadataEntry);
    this.metadataEntryPath = fileCq.path.toString();
    this.referencedTabletFile = ReferencedTabletFile.of(getPath(), fileCq.range);
  }

  /**
   * Return the exact string that is stored in the metadata table. This is important for updating
   * and deleting metadata entries. If the exact string is not used, erroneous entries can pollute
   * the metadata table.
   */
  public String getMetadata() {
    return metadataEntry;
  }

  /**
   * Returns just the Path portion of the metadata, not the full Json.
   */
  public String getMetadataPath() {
    return metadataEntryPath;
  }

  /**
   * Return a new Text object of {@link #getMetadata()}
   */
  public Text getMetadataText() {
    return new Text(getMetadata());
  }

  public ReferencedTabletFile getTabletFile() {
    return referencedTabletFile;
  }

  public TableId getTableId() {
    return referencedTabletFile.getTableId();
  }

  @Override
  public String getFileName() {
    return referencedTabletFile.getFileName();
  }

  public String getNormalizedPathStr() {
    return referencedTabletFile.getNormalizedPathStr();
  }

  @Override
  public int compareTo(StoredTabletFile o) {
    if (equals(o)) {
      return 0;
    } else {
      return comparator.compare(this, o);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StoredTabletFile that = (StoredTabletFile) o;
    return Objects.equals(metadataEntry, that.metadataEntry);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metadataEntry);
  }

  @Override
  public String toString() {
    return metadataEntry;
  }

  /**
   * Validates that the provided metadata string for the StoredTabletFile is valid.
   */
  public static void validate(String metadataEntry) {
    final TabletFileCq tabletFileCq = deserialize(metadataEntry);
    // Validate the path
    ReferencedTabletFile.parsePath(deserialize(metadataEntry).path);
    // Validate the range
    requireKeyExtentDataRange(tabletFileCq.range);
  }

  public static StoredTabletFile of(final Text metadataEntry) {
    return new StoredTabletFile(Objects.requireNonNull(metadataEntry).toString());
  }

  public static StoredTabletFile of(final String metadataEntry) {
    return new StoredTabletFile(metadataEntry);
  }

  public static StoredTabletFile of(final URI path, Range range) {
    return of(new Path(Objects.requireNonNull(path)), range);
  }

  public static StoredTabletFile of(final Path path, Range range) {
    return new StoredTabletFile(new TabletFileCq(Objects.requireNonNull(path), range));
  }

  public static StoredTabletFile of(final URI path) {
    return of(path, new Range());
  }

  public static StoredTabletFile of(final Path path) {
    return of(path, new Range());
  }

  private static final Gson gson = ByteArrayToBase64TypeAdapter.createBase64Gson();

  private static TabletFileCq deserialize(String json) {
    final TabletFileCqMetadataGson metadata =
        gson.fromJson(Objects.requireNonNull(json), TabletFileCqMetadataGson.class);

    // Check each field and provide better error messages if null as all fields should be set
    Objects.requireNonNull(metadata.path, "Serialized StoredTabletFile path must not be null");
    Objects.requireNonNull(metadata.startRow,
        "Serialized StoredTabletFile range startRow must not be null");
    Objects.requireNonNull(metadata.endRow,
        "Serialized StoredTabletFile range endRow must not be null");

    // Recreate the exact Range that was originally stored in Metadata. Stored ranges are originally
    // constructed with inclusive/exclusive for the start and end key inclusivity settings.
    // (Except for Ranges with no start/endkey as then the inclusivity flags do not matter)
    // The ranges must match the format of KeyExtent.toDataRange()
    //
    // With this particular constructor, when setting the startRowInclusive to true and
    // endRowInclusive to false, both the start and end row values will be taken as is
    // and not modified and will recreate the original Range.
    //
    // This constructor will always set the resulting inclusivity of the Range to be true for the
    // start row and false for end row regardless of what the startRowInclusive and endRowInclusive
    // flags are set to.
    return new TabletFileCq(new Path(URI.create(metadata.path)),
        new Range(decodeRow(metadata.startRow), true, decodeRow(metadata.endRow), false));
  }

  public static String serialize(String path) {
    return serialize(path, new Range());
  }

  public static String serialize(String path, Range range) {
    requireKeyExtentDataRange(range);
    final TabletFileCqMetadataGson metadata = new TabletFileCqMetadataGson();
    metadata.path = Objects.requireNonNull(path);
    metadata.startRow = encodeRow(range.getStartKey());
    metadata.endRow = encodeRow(range.getEndKey());

    return gson.toJson(metadata);
  }

  private static String serialize(TabletFileCq tabletFileCq) {
    return serialize(Objects.requireNonNull(tabletFileCq).path.toString(), tabletFileCq.range);
  }

  /**
   * Helper methods to encode and decode rows in a range to/from byte arrays. Null rows will just be
   * returned as an empty byte array
   **/

  private static byte[] encodeRow(final Key key) {
    final Text row = key != null ? key.getRow() : null;
    if (row != null) {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
          DataOutputStream dos = new DataOutputStream(baos)) {
        row.write(dos);
        dos.close();
        return baos.toByteArray();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    // Empty byte array means null row
    return new byte[0];
  }

  private static Text decodeRow(byte[] serialized) {
    // Empty byte array means null row
    if (serialized.length == 0) {
      return null;
    }

    try (DataInputBuffer buffer = new DataInputBuffer()) {
      final Text row = new Text();
      buffer.reset(serialized, serialized.length);
      row.readFields(buffer);
      return row;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Quick validation to see if value has been converted by checking if the candidate looks like
   * json by checking the candidate starts with "{" and ends with "}".
   *
   * @param candidate a possible file: reference.
   * @return false if a likely a json object, true if not a likely json object
   */
  @VisibleForTesting
  public static boolean fileNeedsConversion(@NonNull final String candidate) {
    String trimmed = candidate.trim();
    return !trimmed.startsWith("{") || !trimmed.endsWith("}");
  }

  private static class TabletFileCq {
    public final Path path;
    public final Range range;

    public TabletFileCq(Path path, Range range) {
      this.path = Objects.requireNonNull(path);
      this.range = Objects.requireNonNull(range);
    }
  }

  private static class TabletFileCqMetadataGson {
    private String path;
    private byte[] startRow;
    private byte[] endRow;
  }
}
