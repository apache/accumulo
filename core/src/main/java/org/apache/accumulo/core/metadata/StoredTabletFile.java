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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Objects;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.json.ByteArrayToBase64TypeAdapter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;

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
    this.referencedTabletFile = ReferencedTabletFile.of(getPath());
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
      return metadataEntry.compareTo(o.metadataEntry);
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

  private static final Gson gson = ByteArrayToBase64TypeAdapter.createBase64Gson();

  private static TabletFileCq deserialize(String json) {
    final TabletFileCqMetadataGson metadata =
        gson.fromJson(Objects.requireNonNull(json), TabletFileCqMetadataGson.class);
    // If we have previously enforced the inclusive/exclusive of a range then can just set that here
    return new TabletFileCq(new Path(metadata.path),
        new Range(decodeRow(metadata.startRow), true, decodeRow(metadata.endRow), false));
  }

  public static String serialize(String path) {
    return serialize(path, new Range());
  }

  public static String serialize(String path, Range range) {
    final TabletFileCqMetadataGson metadata = new TabletFileCqMetadataGson();
    metadata.path = Objects.requireNonNull(path);

    // TODO - Add validation on start/end rows inclusive/exclusive in a Range if not null
    // If we can guarantee start is inclusive and end is exclusive then we don't need to encode
    // those boolean values or store them.
    // Should we validate and enforce this when we serialize here or even earlier when we crate the
    // TabletFile object with a range?
    metadata.startRow = encodeRow(Objects.requireNonNull(range).getStartKey());
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
      try (DataOutputBuffer buffer = new DataOutputBuffer()) {
        row.write(buffer);
        return buffer.getData();
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
