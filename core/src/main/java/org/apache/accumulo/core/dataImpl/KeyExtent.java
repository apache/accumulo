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
package org.apache.accumulo.core.dataImpl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;

/**
 * keeps track of information needed to identify a tablet
 */
public class KeyExtent implements Comparable<KeyExtent> {

  private static final String OBSCURING_HASH_ALGORITHM = "SHA-256";

  private final TableId tableId;
  private final Text endRow;
  private final Text prevEndRow;

  // lazily computed, as needed
  private volatile int hashCode = 0;

  private static final Text EMPTY_TEXT = new Text("");

  // The last tablet in a table has no end row, so null sorts last for end row; similarly, the first
  // tablet has no previous end row, so null sorts first for previous end row
  private static final Comparator<KeyExtent> COMPARATOR = Comparator.comparing(KeyExtent::tableId)
      .thenComparing(KeyExtent::endRow, Comparator.nullsLast(Text::compareTo))
      .thenComparing(KeyExtent::prevEndRow, Comparator.nullsFirst(Text::compareTo));

  /**
   * Create a new KeyExtent from its components.
   *
   * @param table
   *          the ID for the table
   * @param endRow
   *          the last row in this tablet, or null if this is the last tablet in this table
   * @param prevEndRow
   *          the last row in the immediately preceding tablet for the table, or null if this
   *          represents the first tablet in this table
   */
  public KeyExtent(TableId table, Text endRow, Text prevEndRow) {
    tableId = requireNonNull(table, "null table ID not allowed");
    if (endRow != null && prevEndRow != null && prevEndRow.compareTo(endRow) >= 0) {
      throw new IllegalArgumentException(
          "prevEndRow (" + prevEndRow + ") >= endRow (" + endRow + ")");
    }
    this.endRow = endRow == null ? null : new Text(endRow);
    this.prevEndRow = prevEndRow == null ? null : new Text(prevEndRow);
  }

  /**
   * Create a copy of a provided KeyExtent.
   *
   * @param original
   *          the KeyExtent to copy
   */
  public static KeyExtent copyOf(KeyExtent original) {
    return new KeyExtent(original.tableId(), original.endRow(), original.prevEndRow());
  }

  /**
   * Create a KeyExtent from its Thrift form.
   *
   * @param tke
   *          the KeyExtent in its Thrift object form
   */
  public static KeyExtent fromThrift(TKeyExtent tke) {
    TableId tableId = TableId.of(new String(ByteBufferUtil.toBytes(tke.table), UTF_8));
    Text endRow = tke.endRow == null ? null : new Text(ByteBufferUtil.toBytes(tke.endRow));
    Text prevEndRow =
        tke.prevEndRow == null ? null : new Text(ByteBufferUtil.toBytes(tke.prevEndRow));
    return new KeyExtent(tableId, endRow, prevEndRow);
  }

  /**
   * Convert to Thrift form.
   */
  public TKeyExtent toThrift() {
    return new TKeyExtent(ByteBuffer.wrap(tableId().canonical().getBytes(UTF_8)),
        endRow() == null ? null : TextUtil.getByteBuffer(endRow()),
        prevEndRow() == null ? null : TextUtil.getByteBuffer(prevEndRow()));
  }

  /**
   * Create a KeyExtent from a metadata previous end row entry.
   *
   * @param prevRowEntry
   *          a provided previous end row entry from the metadata table, stored in the
   *          <code>{@value TabletColumnFamily#STR_NAME}</code> column family and
   *          <code>{@value TabletColumnFamily#PREV_ROW_QUAL}</code> column. If the individual
   *          components are needed, consider {@link #KeyExtent(TableId, Text, Text)} or
   *          {@link #fromMetaRow(Text, Text)} instead.
   */
  public static KeyExtent fromMetaPrevRow(Entry<Key,Value> prevRowEntry) {
    return fromMetaRow(prevRowEntry.getKey().getRow(),
        TabletColumnFamily.decodePrevEndRow(prevRowEntry.getValue()));
  }

  /**
   * Create a KeyExtent from the table ID and the end row encoded in the row field of a tablet's
   * metadata entry, with no previous end row.
   *
   * @param encodedMetadataRow
   *          the encoded <code>tableId</code> and <code>endRow</code> from a metadata entry, as in
   *          <code>entry.getKey().getRow()</code> or from
   *          {@link TabletsSection#encodeRow(TableId, Text)}
   */
  public static KeyExtent fromMetaRow(Text encodedMetadataRow) {
    return fromMetaRow(encodedMetadataRow, (Text) null);
  }

  /**
   * Create a KeyExtent from the table ID and the end row encoded in the row field of a tablet's
   * metadata entry, along with a previous end row.
   *
   * @param encodedMetadataRow
   *          the encoded <code>tableId</code> and <code>endRow</code> from a metadata entry, as in
   *          <code>entry.getKey().getRow()</code> or from
   *          {@link TabletsSection#encodeRow(TableId, Text)}
   * @param prevEndRow
   *          the unencoded previous end row (a copy will be made)
   */
  public static KeyExtent fromMetaRow(Text encodedMetadataRow, Text prevEndRow) {
    Pair<TableId,Text> tableIdAndEndRow = TabletsSection.decodeRow(encodedMetadataRow);
    TableId tableId = tableIdAndEndRow.getFirst();
    Text endRow = tableIdAndEndRow.getSecond();
    return new KeyExtent(tableId, endRow, prevEndRow);
  }

  /**
   * Return a serialized form of the table ID and end row for this extent, in a form suitable for
   * use in the row portion of metadata entries for the tablet this extent represents.
   */
  public Text toMetaRow() {
    return TabletsSection.encodeRow(tableId(), endRow());
  }

  /**
   * Return the extent's table ID.
   */
  public TableId tableId() {
    return tableId;
  }

  /**
   * Returns this extent's end row
   */
  public Text endRow() {
    return endRow;
  }

  /**
   * Return the previous extent's end row
   */
  public Text prevEndRow() {
    return prevEndRow;
  }

  /**
   * Create a KeyExtent from a serialized form.
   *
   * @see #writeTo(DataOutput)
   */
  public static KeyExtent readFrom(DataInput in) throws IOException {
    Text tid = new Text();
    tid.readFields(in);
    TableId tableId = TableId.of(tid.toString());
    Text endRow = null;
    Text prevEndRow = null;
    boolean hasRow = in.readBoolean();
    if (hasRow) {
      endRow = new Text();
      endRow.readFields(in);
    }
    boolean hasPrevRow = in.readBoolean();
    if (hasPrevRow) {
      prevEndRow = new Text();
      prevEndRow.readFields(in);
    }
    return new KeyExtent(tableId, endRow, prevEndRow);
  }

  /**
   * Serialize this KeyExtent.
   *
   * @see #readFrom(DataInput)
   */
  public void writeTo(DataOutput out) throws IOException {
    new Text(tableId().canonical()).write(out);
    if (endRow() != null) {
      out.writeBoolean(true);
      endRow().write(out);
    } else {
      out.writeBoolean(false);
    }
    if (prevEndRow() != null) {
      out.writeBoolean(true);
      prevEndRow().write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public int compareTo(KeyExtent other) {
    return COMPARATOR.compare(this, other);
  }

  @Override
  public int hashCode() {
    // lazily compute hash code, if needed
    if (hashCode != 0) {
      return hashCode;
    }
    int tmpHashCode = tableId().hashCode();
    tmpHashCode += endRow() == null ? 0 : endRow().hashCode();
    tmpHashCode += prevEndRow() == null ? 0 : prevEndRow().hashCode();
    hashCode = tmpHashCode;
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof KeyExtent)) {
      return false;
    }
    KeyExtent oke = (KeyExtent) o;
    return tableId().equals(oke.tableId()) && Objects.equals(endRow(), oke.endRow())
        && Objects.equals(prevEndRow(), oke.prevEndRow());
  }

  @Override
  public String toString() {
    String endRowString;
    String prevEndRowString;
    String tableIdString =
        tableId().canonical().replaceAll(";", "\\\\;").replaceAll("\\\\", "\\\\\\\\");

    if (endRow() == null) {
      endRowString = "<";
    } else {
      endRowString = ";" + TextUtil.truncate(endRow()).toString().replaceAll(";", "\\\\;")
          .replaceAll("\\\\", "\\\\\\\\");
    }

    if (prevEndRow() == null) {
      prevEndRowString = "<";
    } else {
      prevEndRowString = ";" + TextUtil.truncate(prevEndRow()).toString().replaceAll(";", "\\\\;")
          .replaceAll("\\\\", "\\\\\\\\");
    }

    return tableIdString + endRowString + prevEndRowString;
  }

  /**
   * Retrieve a unique identifier for this tablet that is useful for logging, without revealing the
   * contents of the end row and previous end row.
   */
  public UUID getUUID() {
    try (var baos = new ByteArrayOutputStream(); var dos = new DataOutputStream(baos)) {
      // to get a unique hash it is important to encode the data
      // like it is being serialized
      writeTo(dos);
      return UUID.nameUUIDFromBytes(baos.toByteArray());
    } catch (IOException impossible) {
      // impossible when ByteArrayOutputStream is backing the DataOutputStream
      throw new AssertionError(impossible);
    }
  }

  /**
   * Determine if another KeyExtent is wholly contained within the range of this one. * @param oke
   *
   * @return true if and only if the provided KeyExtent is contained within the range of this one
   */
  public boolean contains(KeyExtent oke) {
    // true if this prev row is before the other
    boolean containsPrevRow = prevEndRow() == null
        || (oke.prevEndRow() != null && prevEndRow().compareTo(oke.prevEndRow()) <= 0);
    // true if this end row is after the other
    boolean containsEndRow =
        endRow() == null || (oke.endRow() != null && endRow().compareTo(oke.endRow()) >= 0);
    return containsPrevRow && containsEndRow;
  }

  /**
   * Determine if the provided row is contained within the range of this KeyExtent.
   *
   * @return true if and only if the provided row is contained within the range of this one
   */
  public boolean contains(BinaryComparable row) {
    if (row == null) {
      throw new IllegalArgumentException(
          "Passing null to contains is ambiguous, could be in first or last extent of table");
    }
    return (prevEndRow() == null || prevEndRow().compareTo(row) < 0)
        && (endRow() == null || endRow().compareTo(row) >= 0);
  }

  /**
   * Compute a representation of this extent as a Range suitable for scanning this extent's table
   * and retrieving all data between this extent's previous end row (exclusive) and its end row
   * (inclusive) in that table (or another table in the same range, since the returned range does
   * not specify the table to scan).
   *
   * <p>
   * For example, if this extent represented a range of data from <code>A</code> to <code>Z</code>
   * for table <code>T</code>, the resulting Range could be used to scan table <code>T</code>'s data
   * in the exclusive-inclusive range <code>(A,Z]</code>, or the same range in another table,
   * <code>T2</code>.
   */
  public Range toDataRange() {
    return new Range(prevEndRow(), false, endRow(), true);
  }

  /**
   * Compute a representation of this extent as a Range suitable for scanning the corresponding
   * metadata table for this extent's table, and retrieving all the metadata for the table's tablets
   * between this extent's previous end row (exclusive) and its end row (inclusive) in that table.
   *
   * <p>
   * For example, if this extent represented a range of data from <code>A</code> to <code>Z</code>
   * for a user table, <code>T</code>, this would compute the range to scan
   * <code>accumulo.metadata</code> that would include all the the metadata for <code>T</code>'s
   * tablets that contain data in the range <code>(A,Z]</code>.
   */
  public Range toMetaRange() {
    Text metadataPrevRow =
        TabletsSection.encodeRow(tableId(), prevEndRow() == null ? EMPTY_TEXT : prevEndRow());
    return new Range(metadataPrevRow, prevEndRow() == null, toMetaRow(), true);
  }

  private boolean startsAfter(KeyExtent other) {
    KeyExtent nke = requireNonNull(other);
    return tableId().compareTo(nke.tableId()) > 0 || (prevEndRow() != null && nke.endRow() != null
        && prevEndRow().compareTo(nke.endRow()) >= 0);
  }

  private static Text rowAfterPrevRow(KeyExtent nke) {
    Text row = new Text(nke.prevEndRow());
    row.append(new byte[] {0}, 0, 1);
    return row;
  }

  // Some duplication with TabletLocatorImpl
  public static Set<KeyExtent> findOverlapping(KeyExtent nke, SortedSet<KeyExtent> extents) {
    if (nke == null || extents == null || extents.isEmpty()) {
      return Collections.emptySet();
    }

    SortedSet<KeyExtent> start;

    if (nke.prevEndRow() != null) {
      Text row = rowAfterPrevRow(nke);
      KeyExtent lookupKey = new KeyExtent(nke.tableId(), row, null);
      start = extents.tailSet(lookupKey);
    } else {
      KeyExtent lookupKey = new KeyExtent(nke.tableId(), new Text(), null);
      start = extents.tailSet(lookupKey);
    }

    TreeSet<KeyExtent> result = new TreeSet<>();
    for (KeyExtent ke : start) {
      if (ke.startsAfter(nke)) {
        break;
      }
      result.add(ke);
    }
    return result;
  }

  public boolean overlaps(KeyExtent other) {
    SortedSet<KeyExtent> set = new TreeSet<>();
    set.add(other);
    return !findOverlapping(this, set).isEmpty();
  }

  // Specialization of findOverlapping(KeyExtent, SortedSet<KeyExtent> to work with SortedMap
  public static Set<KeyExtent> findOverlapping(KeyExtent nke, SortedMap<KeyExtent,?> extents) {
    if (nke == null || extents == null || extents.isEmpty()) {
      return Collections.emptySet();
    }

    SortedMap<KeyExtent,?> start;

    if (nke.prevEndRow() != null) {
      Text row = rowAfterPrevRow(nke);
      KeyExtent lookupKey = new KeyExtent(nke.tableId(), row, null);
      start = extents.tailMap(lookupKey);
    } else {
      KeyExtent lookupKey = new KeyExtent(nke.tableId(), new Text(), null);
      start = extents.tailMap(lookupKey);
    }

    TreeSet<KeyExtent> result = new TreeSet<>();
    for (Entry<KeyExtent,?> entry : start.entrySet()) {
      KeyExtent ke = entry.getKey();
      if (ke.startsAfter(nke)) {
        break;
      }
      result.add(ke);
    }
    return result;
  }

  public boolean isPreviousExtent(KeyExtent prevExtent) {
    if (prevExtent == null) {
      return prevEndRow() == null;
    }
    if (!prevExtent.tableId().equals(tableId())) {
      throw new IllegalArgumentException("Cannot compare across tables " + prevExtent + " " + this);
    }
    if (prevExtent.endRow() == null || prevEndRow() == null) {
      return false;
    }
    return prevExtent.endRow().equals(prevEndRow());
  }

  public boolean isMeta() {
    return tableId().equals(MetadataTable.ID) || isRootTablet();
  }

  public boolean isRootTablet() {
    return tableId().equals(RootTable.ID);
  }

  public String obscured() {
    MessageDigest digester;
    try {
      digester = MessageDigest.getInstance(OBSCURING_HASH_ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
    if (endRow() != null && endRow().getLength() > 0) {
      digester.update(endRow().getBytes(), 0, endRow().getLength());
    }
    return Base64.getEncoder().encodeToString(digester.digest());
  }

}
