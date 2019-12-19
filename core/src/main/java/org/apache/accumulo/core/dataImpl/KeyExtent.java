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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * keeps track of information needed to identify a tablet
 */
public class KeyExtent implements WritableComparable<KeyExtent> {

  private TableId tableId;
  private Text textEndRow;
  private Text textPrevEndRow;

  private static final TableId EMPTY_ID = TableId.of("");
  private static final Text EMPTY_TEXT = new Text("");

  private void check() {

    if (getTableId() == null)
      throw new IllegalArgumentException("null table id not allowed");

    if (getEndRow() == null || getPrevEndRow() == null)
      return;

    if (getPrevEndRow().compareTo(getEndRow()) >= 0) {
      throw new IllegalArgumentException(
          "prevEndRow (" + getPrevEndRow() + ") >= endRow (" + getEndRow() + ")");
    }
  }

  /**
   * Default constructor
   *
   */
  public KeyExtent() {
    this.setTableId(EMPTY_ID);
    this.setEndRow(new Text(), false, false);
    this.setPrevEndRow(new Text(), false, false);
  }

  public KeyExtent(TableId table, Text endRow, Text prevEndRow) {
    this.setTableId(table);
    this.setEndRow(endRow, false, true);
    this.setPrevEndRow(prevEndRow, false, true);

    check();
  }

  public KeyExtent(KeyExtent extent) {
    // extent has already deduped table id, so there is no need to do it again
    this.tableId = extent.tableId;
    this.setEndRow(extent.getEndRow(), false, true);
    this.setPrevEndRow(extent.getPrevEndRow(), false, true);

    check();
  }

  public KeyExtent(TKeyExtent tke) {
    this.setTableId(TableId.of(new String(ByteBufferUtil.toBytes(tke.table), UTF_8)));
    this.setEndRow(tke.endRow == null ? null : new Text(ByteBufferUtil.toBytes(tke.endRow)), false,
        false);
    this.setPrevEndRow(
        tke.prevEndRow == null ? null : new Text(ByteBufferUtil.toBytes(tke.prevEndRow)), false,
        false);

    check();
  }

  /**
   * Returns a String representing this extent's entry in the Metadata table
   *
   */
  public Text getMetadataEntry() {
    return TabletsSection.getRow(getTableId(), getEndRow());
  }

  // constructor for loading extents from metadata rows
  public KeyExtent(Text flattenedExtent, Value prevEndRow) {
    decodeMetadataRow(flattenedExtent);

    // decode the prev row
    this.setPrevEndRow(decodePrevEndRow(prevEndRow), false, true);

    check();
  }

  // recreates an encoded extent from a string representation
  // this encoding is what is stored as the row id of the metadata table
  public KeyExtent(Text flattenedExtent, Text prevEndRow) {

    decodeMetadataRow(flattenedExtent);

    this.setPrevEndRow(null, false, false);
    if (prevEndRow != null)
      this.setPrevEndRow(prevEndRow, false, true);

    check();
  }

  /**
   * Sets the extents table id
   *
   */
  public void setTableId(TableId tId) {
    Objects.requireNonNull(tId, "null table id not allowed");

    this.tableId = tId;

    hashCode = 0;
  }

  /**
   * Returns the extent's table id
   *
   */
  public TableId getTableId() {
    return tableId;
  }

  private void setEndRow(Text endRow, boolean check, boolean copy) {
    if (endRow != null)
      if (copy)
        this.textEndRow = new Text(endRow);
      else
        this.textEndRow = endRow;
    else
      this.textEndRow = null;

    hashCode = 0;
    if (check)
      check();
  }

  /**
   * Sets this extent's end row
   *
   */
  public void setEndRow(Text endRow) {
    setEndRow(endRow, true, true);
  }

  /**
   * Returns this extent's end row
   *
   */
  public Text getEndRow() {
    return textEndRow;
  }

  /**
   * Return the previous extent's end row
   *
   */
  public Text getPrevEndRow() {
    return textPrevEndRow;
  }

  private void setPrevEndRow(Text prevEndRow, boolean check, boolean copy) {
    if (prevEndRow != null)
      if (copy)
        this.textPrevEndRow = new Text(prevEndRow);
      else
        this.textPrevEndRow = prevEndRow;
    else
      this.textPrevEndRow = null;

    hashCode = 0;
    if (check)
      check();
  }

  /**
   * Sets the previous extent's end row
   *
   */
  public void setPrevEndRow(Text prevEndRow) {
    setPrevEndRow(prevEndRow, true, true);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Text tid = new Text();
    tid.readFields(in);
    setTableId(TableId.of(tid.toString()));
    boolean hasRow = in.readBoolean();
    if (hasRow) {
      Text er = new Text();
      er.readFields(in);
      setEndRow(er, false, false);
    } else {
      setEndRow(null, false, false);
    }
    boolean hasPrevRow = in.readBoolean();
    if (hasPrevRow) {
      Text per = new Text();
      per.readFields(in);
      setPrevEndRow(per, false, true);
    } else {
      setPrevEndRow(null);
    }

    hashCode = 0;
    check();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    new Text(getTableId().canonical()).write(out);
    if (getEndRow() != null) {
      out.writeBoolean(true);
      getEndRow().write(out);
    } else {
      out.writeBoolean(false);
    }
    if (getPrevEndRow() != null) {
      out.writeBoolean(true);
      getPrevEndRow().write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  /**
   * Returns a String representing the previous extent's entry in the Metadata table
   *
   */
  public Mutation getPrevRowUpdateMutation() {
    return getPrevRowUpdateMutation(this);
  }

  public static Text decodePrevEndRow(Value ibw) {
    Text per = null;

    if (ibw.get()[0] != 0) {
      per = new Text();
      per.set(ibw.get(), 1, ibw.get().length - 1);
    }

    return per;
  }

  public static Value encodePrevEndRow(Text per) {
    if (per == null)
      return new Value(new byte[] {0});
    byte[] b = new byte[per.getLength() + 1];
    b[0] = 1;
    System.arraycopy(per.getBytes(), 0, b, 1, per.getLength());
    return new Value(b);
  }

  public static Mutation getPrevRowUpdateMutation(KeyExtent ke) {
    Mutation m = new Mutation(ke.getMetadataEntry());
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.put(m, encodePrevEndRow(ke.getPrevEndRow()));
    return m;
  }

  // The last tablet in a table has no end row, so null sorts last for end row; similarly, the first
  // tablet has no previous end row, so null sorts first for previous end row
  private static final Comparator<KeyExtent> COMPARATOR =
      Comparator.comparing(KeyExtent::getTableId)
          .thenComparing(KeyExtent::getEndRow, Comparator.nullsLast(Text::compareTo))
          .thenComparing(KeyExtent::getPrevEndRow, Comparator.nullsFirst(Text::compareTo));

  @Override
  public int compareTo(KeyExtent other) {
    return COMPARATOR.compare(this, other);
  }

  private int hashCode = 0;

  @Override
  public int hashCode() {
    if (hashCode != 0)
      return hashCode;

    int prevEndRowHash = 0;
    int endRowHash = 0;
    if (this.getEndRow() != null) {
      endRowHash = this.getEndRow().hashCode();
    }

    if (this.getPrevEndRow() != null) {
      prevEndRowHash = this.getPrevEndRow().hashCode();
    }

    hashCode = getTableId().hashCode() + endRowHash + prevEndRowHash;
    return hashCode;
  }

  private boolean equals(Text t1, Text t2) {
    if (t1 == null || t2 == null)
      return t1 == t2;

    return t1.equals(t2);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (!(o instanceof KeyExtent))
      return false;
    KeyExtent oke = (KeyExtent) o;
    return tableId.equals(oke.tableId) && equals(textEndRow, oke.textEndRow)
        && equals(textPrevEndRow, oke.textPrevEndRow);
  }

  @Override
  public String toString() {
    String endRowString;
    String prevEndRowString;
    String tableIdString =
        getTableId().canonical().replaceAll(";", "\\\\;").replaceAll("\\\\", "\\\\\\\\");

    if (getEndRow() == null)
      endRowString = "<";
    else
      endRowString = ";" + TextUtil.truncate(getEndRow()).toString().replaceAll(";", "\\\\;")
          .replaceAll("\\\\", "\\\\\\\\");

    if (getPrevEndRow() == null)
      prevEndRowString = "<";
    else
      prevEndRowString = ";" + TextUtil.truncate(getPrevEndRow()).toString()
          .replaceAll(";", "\\\\;").replaceAll("\\\\", "\\\\\\\\");

    return tableIdString + endRowString + prevEndRowString;
  }

  public UUID getUUID() {
    try {

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);

      // to get a unique hash it is important to encode the data
      // like it is being serialized

      this.write(dos);

      dos.close();

      return UUID.nameUUIDFromBytes(baos.toByteArray());

    } catch (IOException e) {
      // should not happen since we are writing to memory
      throw new RuntimeException(e);
    }
  }

  // note: this is only the encoding of the table id and the last row, not the prev row
  /**
   * Populates the extent's fields based on a flatted extent
   *
   */
  private void decodeMetadataRow(Text flattenedExtent) {
    int semiPos = -1;
    int ltPos = -1;

    for (int i = 0; i < flattenedExtent.getLength(); i++) {
      if (flattenedExtent.getBytes()[i] == ';' && semiPos < 0) {
        // want the position of the first semicolon
        semiPos = i;
      }

      if (flattenedExtent.getBytes()[i] == '<') {
        ltPos = i;
      }
    }

    if (semiPos < 0 && ltPos < 0) {
      throw new IllegalArgumentException(
          "Metadata row does not contain ; or <  " + flattenedExtent);
    }

    if (semiPos < 0) {

      if (ltPos != flattenedExtent.getLength() - 1) {
        throw new IllegalArgumentException(
            "< must come at end of Metadata row  " + flattenedExtent);
      }

      String decodedString = new String(
          Arrays.copyOfRange(flattenedExtent.getBytes(), 0, flattenedExtent.getLength() - 1),
          UTF_8);
      TableId tableId = TableId.of(decodedString);
      this.setTableId(tableId);
      this.setEndRow(null, false, false);
    } else {

      TableId tableId =
          TableId.of(new String(Arrays.copyOfRange(flattenedExtent.getBytes(), 0, semiPos), UTF_8));

      Text endRow = new Text();
      endRow.set(flattenedExtent.getBytes(), semiPos + 1,
          flattenedExtent.getLength() - (semiPos + 1));

      this.setTableId(tableId);

      this.setEndRow(endRow, false, false);
    }
  }

  public static TableId tableOfMetadataRow(Text row) {
    KeyExtent ke = new KeyExtent();
    ke.decodeMetadataRow(row);
    return ke.getTableId();
  }

  public boolean contains(final ByteSequence bsrow) {
    if (bsrow == null) {
      throw new IllegalArgumentException(
          "Passing null to contains is ambiguous, could be in first or last extent of table");
    }

    BinaryComparable row = new BinaryComparable() {

      @Override
      public int getLength() {
        return bsrow.length();
      }

      @Override
      public byte[] getBytes() {
        if (bsrow.isBackedByArray() && bsrow.offset() == 0)
          return bsrow.getBackingArray();

        return bsrow.toArray();
      }
    };

    return (this.getPrevEndRow() == null || this.getPrevEndRow().compareTo(row) < 0)
        && (this.getEndRow() == null || this.getEndRow().compareTo(row) >= 0);
  }

  public boolean contains(BinaryComparable row) {
    if (row == null) {
      throw new IllegalArgumentException(
          "Passing null to contains is ambiguous, could be in first or last extent of table");
    }

    return (this.getPrevEndRow() == null || this.getPrevEndRow().compareTo(row) < 0)
        && (this.getEndRow() == null || this.getEndRow().compareTo(row) >= 0);
  }

  public Range toDataRange() {
    return new Range(getPrevEndRow(), false, getEndRow(), true);
  }

  public Range toMetadataRange() {

    Text metadataPrevRow =
        TabletsSection.getRow(getTableId(), getPrevEndRow() == null ? EMPTY_TEXT : getPrevEndRow());

    return new Range(metadataPrevRow, getPrevEndRow() == null, getMetadataEntry(), true);
  }

  public static SortedSet<KeyExtent> findChildren(KeyExtent ke, SortedSet<KeyExtent> tablets) {

    SortedSet<KeyExtent> children = null;

    for (KeyExtent tabletKe : tablets) {

      if (ke.getPrevEndRow() == tabletKe.getPrevEndRow()
          || ke.getPrevEndRow() != null && tabletKe.getPrevEndRow() != null
              && tabletKe.getPrevEndRow().compareTo(ke.getPrevEndRow()) == 0) {
        children = new TreeSet<>();
      }

      if (children != null) {
        children.add(tabletKe);
      }

      if (ke.getEndRow() == tabletKe.getEndRow() || ke.getEndRow() != null
          && tabletKe.getEndRow() != null && tabletKe.getEndRow().compareTo(ke.getEndRow()) == 0) {
        return children;
      }
    }

    return new TreeSet<>();
  }

  public static KeyExtent findContainingExtent(KeyExtent extent, SortedSet<KeyExtent> extents) {

    KeyExtent lookupExtent = new KeyExtent(extent);
    lookupExtent.setPrevEndRow(null);

    SortedSet<KeyExtent> tailSet = extents.tailSet(lookupExtent);

    if (tailSet.isEmpty()) {
      return null;
    }

    KeyExtent first = tailSet.first();

    if (first.getTableId().compareTo(extent.getTableId()) != 0) {
      return null;
    }

    if (first.getPrevEndRow() == null) {
      return first;
    }

    if (extent.getPrevEndRow() == null) {
      return null;
    }

    if (extent.getPrevEndRow().compareTo(first.getPrevEndRow()) >= 0)
      return first;
    return null;
  }

  private static boolean startsAfter(KeyExtent nke, KeyExtent ke) {

    int tiCmp = ke.getTableId().compareTo(nke.getTableId());

    if (tiCmp > 0) {
      return true;
    }

    return ke.getPrevEndRow() != null && nke.getEndRow() != null
        && ke.getPrevEndRow().compareTo(nke.getEndRow()) >= 0;
  }

  private static Text rowAfterPrevRow(KeyExtent nke) {
    Text row = new Text(nke.getPrevEndRow());
    row.append(new byte[] {0}, 0, 1);
    return row;
  }

  // Some duplication with TabletLocatorImpl
  public static Set<KeyExtent> findOverlapping(KeyExtent nke, SortedSet<KeyExtent> extents) {
    if (nke == null || extents == null || extents.isEmpty())
      return Collections.emptySet();

    SortedSet<KeyExtent> start;

    if (nke.getPrevEndRow() != null) {
      Text row = rowAfterPrevRow(nke);
      KeyExtent lookupKey = new KeyExtent(nke.getTableId(), row, null);
      start = extents.tailSet(lookupKey);
    } else {
      KeyExtent lookupKey = new KeyExtent(nke.getTableId(), new Text(), null);
      start = extents.tailSet(lookupKey);
    }

    TreeSet<KeyExtent> result = new TreeSet<>();
    for (KeyExtent ke : start) {
      if (startsAfter(nke, ke)) {
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
    if (nke == null || extents == null || extents.isEmpty())
      return Collections.emptySet();

    SortedMap<KeyExtent,?> start;

    if (nke.getPrevEndRow() != null) {
      Text row = rowAfterPrevRow(nke);
      KeyExtent lookupKey = new KeyExtent(nke.getTableId(), row, null);
      start = extents.tailMap(lookupKey);
    } else {
      KeyExtent lookupKey = new KeyExtent(nke.getTableId(), new Text(), null);
      start = extents.tailMap(lookupKey);
    }

    TreeSet<KeyExtent> result = new TreeSet<>();
    for (Entry<KeyExtent,?> entry : start.entrySet()) {
      KeyExtent ke = entry.getKey();
      if (startsAfter(nke, ke)) {
        break;
      }
      result.add(ke);
    }
    return result;
  }

  public static Text getMetadataEntry(KeyExtent extent) {
    return TabletsSection.getRow(extent.getTableId(), extent.getEndRow());
  }

  public TKeyExtent toThrift() {
    return new TKeyExtent(ByteBuffer.wrap(tableId.canonical().getBytes(UTF_8)),
        textEndRow == null ? null : TextUtil.getByteBuffer(textEndRow),
        textPrevEndRow == null ? null : TextUtil.getByteBuffer(textPrevEndRow));
  }

  public boolean isPreviousExtent(KeyExtent prevExtent) {
    if (prevExtent == null)
      return getPrevEndRow() == null;

    if (!prevExtent.getTableId().equals(getTableId()))
      throw new IllegalArgumentException("Cannot compare across tables " + prevExtent + " " + this);

    if (prevExtent.getEndRow() == null)
      return false;

    if (getPrevEndRow() == null)
      return false;

    return prevExtent.getEndRow().equals(getPrevEndRow());
  }

  public boolean isMeta() {
    return getTableId().equals(MetadataTable.ID) || isRootTablet();
  }

  public boolean isRootTablet() {
    return getTableId().equals(RootTable.ID);
  }
}
