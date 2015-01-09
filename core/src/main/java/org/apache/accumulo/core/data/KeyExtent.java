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
package org.apache.accumulo.core.data;

/**
 * keeps track of information needed to identify a tablet
 * apparently, we only need the endKey and not the start as well
 *
 */

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.WeakHashMap;

import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class KeyExtent implements WritableComparable<KeyExtent> {

  private static final WeakHashMap<Text,WeakReference<Text>> tableIds = new WeakHashMap<Text,WeakReference<Text>>();

  private static Text dedupeTableId(Text tableId) {
    synchronized (tableIds) {
      WeakReference<Text> etir = tableIds.get(tableId);
      if (etir != null) {
        Text eti = etir.get();
        if (eti != null) {
          return eti;
        }
      }

      tableId = new Text(tableId);
      tableIds.put(tableId, new WeakReference<Text>(tableId));
      return tableId;
    }
  }

  private Text textTableId;
  private Text textEndRow;
  private Text textPrevEndRow;

  private void check() {

    if (getTableId() == null)
      throw new IllegalArgumentException("null table id not allowed");

    if (getEndRow() == null || getPrevEndRow() == null)
      return;

    if (getPrevEndRow().compareTo(getEndRow()) >= 0) {
      throw new IllegalArgumentException("prevEndRow (" + getPrevEndRow() + ") >= endRow (" + getEndRow() + ")");
    }
  }

  /**
   * Default constructor
   *
   */
  public KeyExtent() {
    this.setTableId(new Text());
    this.setEndRow(new Text(), false, false);
    this.setPrevEndRow(new Text(), false, false);
  }

  public KeyExtent(Text table, Text endRow, Text prevEndRow) {
    this.setTableId(table);
    this.setEndRow(endRow, false, true);
    this.setPrevEndRow(prevEndRow, false, true);

    check();
  }

  public KeyExtent(KeyExtent extent) {
    // extent has already deduped table id, so there is no need to do it again
    this.textTableId = extent.textTableId;
    this.setEndRow(extent.getEndRow(), false, true);
    this.setPrevEndRow(extent.getPrevEndRow(), false, true);

    check();
  }

  public KeyExtent(TKeyExtent tke) {
    this.setTableId(new Text(ByteBufferUtil.toBytes(tke.table)));
    this.setEndRow(tke.endRow == null ? null : new Text(ByteBufferUtil.toBytes(tke.endRow)), false, false);
    this.setPrevEndRow(tke.prevEndRow == null ? null : new Text(ByteBufferUtil.toBytes(tke.prevEndRow)), false, false);

    check();
  }

  /**
   * Returns a String representing this extent's entry in the Metadata table
   *
   */
  public Text getMetadataEntry() {
    return getMetadataEntry(getTableId(), getEndRow());
  }

  public static Text getMetadataEntry(Text tableId, Text endRow) {
    return MetadataSchema.TabletsSection.getRow(tableId, endRow);
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
  public void setTableId(Text tId) {

    if (tId == null)
      throw new IllegalArgumentException("null table name not allowed");

    this.textTableId = dedupeTableId(tId);

    hashCode = 0;
  }

  /**
   * Returns the extent's table id
   *
   */
  public Text getTableId() {
    return textTableId;
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

  /**
   * Populates the extents data fields from a DataInput object
   *
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    Text tid = new Text();
    tid.readFields(in);
    setTableId(tid);
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
      setPrevEndRow((Text) null);
    }

    hashCode = 0;
    check();
  }

  /**
   * Writes this extent's data fields to a DataOutput object
   *
   */
  @Override
  public void write(DataOutput out) throws IOException {
    getTableId().write(out);
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

  /**
   * Empty start or end rows tell the method there are no start or end rows, and to use all the keyextents that are before the end row if no start row etc.
   *
   * @deprecated this method not intended for public use and is likely to be removed in a future version.
   * @return all the key extents that the rows cover
   */
  @Deprecated
  public static Collection<KeyExtent> getKeyExtentsForRange(Text startRow, Text endRow, Set<KeyExtent> kes) {
    if (kes == null)
      return Collections.emptyList();
    if (startRow == null)
      startRow = new Text();
    if (endRow == null)
      endRow = new Text();
    Collection<KeyExtent> keys = new ArrayList<KeyExtent>();
    for (KeyExtent ckes : kes) {
      if (ckes.getPrevEndRow() == null) {
        if (ckes.getEndRow() == null) {
          // only tablet
          keys.add(ckes);
        } else {
          // first tablet
          // if start row = '' then we want everything up to the endRow which will always include the first tablet
          if (startRow.getLength() == 0) {
            keys.add(ckes);
          } else if (ckes.getEndRow().compareTo(startRow) >= 0) {
            keys.add(ckes);
          }
        }
      } else {
        if (ckes.getEndRow() == null) {
          // last tablet
          // if endRow = '' and we're at the last tablet, add it
          if (endRow.getLength() == 0) {
            keys.add(ckes);
          }
          if (ckes.getPrevEndRow().compareTo(endRow) < 0) {
            keys.add(ckes);
          }
        } else {
          // tablet in the middle
          if (startRow.getLength() == 0) {
            // no start row

            if (endRow.getLength() == 0) {
              // no start & end row
              keys.add(ckes);
            } else {
              // just no start row
              if (ckes.getPrevEndRow().compareTo(endRow) < 0) {
                keys.add(ckes);
              }
            }
          } else if (endRow.getLength() == 0) {
            // no end row
            if (ckes.getEndRow().compareTo(startRow) >= 0) {
              keys.add(ckes);
            }
          } else {
            // no null prevend or endrows and no empty string start or end rows
            if (ckes.getPrevEndRow().compareTo(endRow) < 0 && ckes.getEndRow().compareTo(startRow) >= 0) {
              keys.add(ckes);
            }
          }

        }
      }
    }
    return keys;
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

  /**
   * Compares extents based on rows
   *
   */
  @Override
  public int compareTo(KeyExtent other) {

    int result = getTableId().compareTo(other.getTableId());
    if (result != 0)
      return result;

    if (this.getEndRow() == null) {
      if (other.getEndRow() != null)
        return 1;
    } else {
      if (other.getEndRow() == null)
        return -1;

      result = getEndRow().compareTo(other.getEndRow());
      if (result != 0)
        return result;
    }
    if (this.getPrevEndRow() == null) {
      if (other.getPrevEndRow() == null)
        return 0;
      return -1;
    }
    if (other.getPrevEndRow() == null)
      return 1;
    return this.getPrevEndRow().compareTo(other.getPrevEndRow());
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
    return textTableId.equals(oke.textTableId) && equals(textEndRow, oke.textEndRow) && equals(textPrevEndRow, oke.textPrevEndRow);
  }

  @Override
  public String toString() {
    String endRowString;
    String prevEndRowString;
    String tableIdString = getTableId().toString().replaceAll(";", "\\\\;").replaceAll("\\\\", "\\\\\\\\");

    if (getEndRow() == null)
      endRowString = "<";
    else
      endRowString = ";" + TextUtil.truncate(getEndRow()).toString().replaceAll(";", "\\\\;").replaceAll("\\\\", "\\\\\\\\");

    if (getPrevEndRow() == null)
      prevEndRowString = "<";
    else
      prevEndRowString = ";" + TextUtil.truncate(getPrevEndRow()).toString().replaceAll(";", "\\\\;").replaceAll("\\\\", "\\\\\\\\");

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
      throw new IllegalArgumentException("Metadata row does not contain ; or <  " + flattenedExtent);
    }

    if (semiPos < 0) {

      if (ltPos != flattenedExtent.getLength() - 1) {
        throw new IllegalArgumentException("< must come at end of Metadata row  " + flattenedExtent);
      }

      Text tableId = new Text();
      tableId.set(flattenedExtent.getBytes(), 0, flattenedExtent.getLength() - 1);
      this.setTableId(tableId);
      this.setEndRow(null, false, false);
    } else {

      Text tableId = new Text();
      tableId.set(flattenedExtent.getBytes(), 0, semiPos);

      Text endRow = new Text();
      endRow.set(flattenedExtent.getBytes(), semiPos + 1, flattenedExtent.getLength() - (semiPos + 1));

      this.setTableId(tableId);

      this.setEndRow(endRow, false, false);
    }
  }

  public static byte[] tableOfMetadataRow(Text row) {
    KeyExtent ke = new KeyExtent();
    ke.decodeMetadataRow(row);
    return TextUtil.getBytes(ke.getTableId());
  }

  public boolean contains(final ByteSequence bsrow) {
    if (bsrow == null) {
      throw new IllegalArgumentException("Passing null to contains is ambiguous, could be in first or last extent of table");
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

    if ((this.getPrevEndRow() == null || this.getPrevEndRow().compareTo(row) < 0) && (this.getEndRow() == null || this.getEndRow().compareTo(row) >= 0)) {
      return true;
    }
    return false;
  }

  public boolean contains(BinaryComparable row) {
    if (row == null) {
      throw new IllegalArgumentException("Passing null to contains is ambiguous, could be in first or last extent of table");
    }

    if ((this.getPrevEndRow() == null || this.getPrevEndRow().compareTo(row) < 0) && (this.getEndRow() == null || this.getEndRow().compareTo(row) >= 0)) {
      return true;
    }
    return false;
  }

  public Range toDataRange() {
    return new Range(getPrevEndRow(), false, getEndRow(), true);
  }

  public Range toMetadataRange() {
    Text metadataPrevRow = new Text(getTableId());
    metadataPrevRow.append(new byte[] {';'}, 0, 1);
    if (getPrevEndRow() != null) {
      metadataPrevRow.append(getPrevEndRow().getBytes(), 0, getPrevEndRow().getLength());
    }

    Range range = new Range(metadataPrevRow, getPrevEndRow() == null, getMetadataEntry(), true);
    return range;
  }

  public static SortedSet<KeyExtent> findChildren(KeyExtent ke, SortedSet<KeyExtent> tablets) {

    SortedSet<KeyExtent> children = null;

    for (KeyExtent tabletKe : tablets) {

      if (ke.getPrevEndRow() == tabletKe.getPrevEndRow() || ke.getPrevEndRow() != null && tabletKe.getPrevEndRow() != null
          && tabletKe.getPrevEndRow().compareTo(ke.getPrevEndRow()) == 0) {
        children = new TreeSet<KeyExtent>();
      }

      if (children != null) {
        children.add(tabletKe);
      }

      if (ke.getEndRow() == tabletKe.getEndRow() || ke.getEndRow() != null && tabletKe.getEndRow() != null
          && tabletKe.getEndRow().compareTo(ke.getEndRow()) == 0) {
        return children;
      }
    }

    return new TreeSet<KeyExtent>();
  }

  public static KeyExtent findContainingExtent(KeyExtent extent, SortedSet<KeyExtent> extents) {

    KeyExtent lookupExtent = new KeyExtent(extent);
    lookupExtent.setPrevEndRow((Text) null);

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

    return ke.getPrevEndRow() != null && nke.getEndRow() != null && ke.getPrevEndRow().compareTo(nke.getEndRow()) >= 0;
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

    TreeSet<KeyExtent> result = new TreeSet<KeyExtent>();
    for (KeyExtent ke : start) {
      if (startsAfter(nke, ke)) {
        break;
      }
      result.add(ke);
    }
    return result;
  }

  public boolean overlaps(KeyExtent other) {
    SortedSet<KeyExtent> set = new TreeSet<KeyExtent>();
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

    TreeSet<KeyExtent> result = new TreeSet<KeyExtent>();
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
    return getMetadataEntry(extent.getTableId(), extent.getEndRow());
  }

  public TKeyExtent toThrift() {
    return new TKeyExtent(TextUtil.getByteBuffer(textTableId), textEndRow == null ? null : TextUtil.getByteBuffer(textEndRow), textPrevEndRow == null ? null
        : TextUtil.getByteBuffer(textPrevEndRow));
  }

  public boolean isPreviousExtent(KeyExtent prevExtent) {
    if (prevExtent == null)
      return getPrevEndRow() == null;

    if (!prevExtent.getTableId().equals(getTableId()))
      throw new IllegalArgumentException("Cannot compare accross tables " + prevExtent + " " + this);

    if (prevExtent.getEndRow() == null)
      return false;

    if (getPrevEndRow() == null)
      return false;

    return prevExtent.getEndRow().equals(getPrevEndRow());
  }

  public boolean isMeta() {
    return getTableId().toString().equals(MetadataTable.ID) || isRootTablet();
  }

  public boolean isRootTablet() {
    return getTableId().toString().equals(RootTable.ID);
  }
}
