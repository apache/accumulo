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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * keeps track of information needed to identify a tablet
 *
 * @deprecated since 1.7.0 use {@link TabletId}
 */
@Deprecated
public class KeyExtent implements WritableComparable<KeyExtent> {

  // Wrapping impl.KeyExtent to resuse code. Did not want to extend impl.KeyExtent because any changes to impl.KeyExtent would be reflected in this class.
  // Wrapping impl.KeyExtent allows the API of this deprecated class to be frozen.
  private org.apache.accumulo.core.data.impl.KeyExtent wrapped;

  public KeyExtent() {
    this.wrapped = new org.apache.accumulo.core.data.impl.KeyExtent();
  }

  public KeyExtent(Text table, Text endRow, Text prevEndRow) {
    this.wrapped = new org.apache.accumulo.core.data.impl.KeyExtent(table, endRow, prevEndRow);
  }

  public KeyExtent(KeyExtent extent) {
    this.wrapped = new org.apache.accumulo.core.data.impl.KeyExtent(extent.getTableId(), extent.getEndRow(), extent.getPrevEndRow());
  }

  public KeyExtent(TKeyExtent tke) {
    this.wrapped = new org.apache.accumulo.core.data.impl.KeyExtent(tke);
  }

  // constructor for loading extents from metadata rows
  public KeyExtent(Text flattenedExtent, Value prevEndRow) {
    this.wrapped = new org.apache.accumulo.core.data.impl.KeyExtent(flattenedExtent, prevEndRow);
  }

  // recreates an encoded extent from a string representation
  // this encoding is what is stored as the row id of the metadata table
  public KeyExtent(Text flattenedExtent, Text prevEndRow) {
    this.wrapped = new org.apache.accumulo.core.data.impl.KeyExtent(flattenedExtent, prevEndRow);
  }

  public Text getMetadataEntry() {
    return wrapped.getMetadataEntry();
  }

  public void setTableId(Text tId) {
    wrapped.setTableId(tId);
  }

  public Text getTableId() {
    return wrapped.getTableId();
  }

  public void setEndRow(Text endRow) {
    wrapped.setEndRow(endRow);
  }

  public Text getEndRow() {
    return wrapped.getEndRow();
  }

  public Text getPrevEndRow() {
    return wrapped.getPrevEndRow();
  }

  public void setPrevEndRow(Text prevEndRow) {
    wrapped.setPrevEndRow(prevEndRow);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    wrapped.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    wrapped.write(out);
  }

  public Mutation getPrevRowUpdateMutation() {
    return wrapped.getPrevRowUpdateMutation();
  }

  @Override
  public int compareTo(KeyExtent other) {
    return wrapped.compareTo(other.wrapped);
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof KeyExtent) {
      return wrapped.equals(((KeyExtent) o).wrapped);
    }

    return false;
  }

  @Override
  public String toString() {
    return wrapped.toString();
  }

  public UUID getUUID() {
    return wrapped.getUUID();
  }

  public boolean contains(ByteSequence bsrow) {
    return wrapped.contains(bsrow);
  }

  public boolean contains(BinaryComparable row) {
    return wrapped.contains(row);
  }

  public Range toDataRange() {
    return wrapped.toDataRange();
  }

  public Range toMetadataRange() {
    return wrapped.toMetadataRange();
  }

  public boolean overlaps(KeyExtent other) {
    return wrapped.overlaps(other.wrapped);
  }

  public TKeyExtent toThrift() {
    return wrapped.toThrift();
  }

  public boolean isPreviousExtent(KeyExtent prevExtent) {
    return wrapped.isPreviousExtent(prevExtent.wrapped);
  }

  public boolean isMeta() {
    return wrapped.isMeta();
  }

  public boolean isRootTablet() {
    return wrapped.isRootTablet();
  }

  private static SortedSet<org.apache.accumulo.core.data.impl.KeyExtent> unwrap(Set<KeyExtent> tablets) {
    SortedSet<org.apache.accumulo.core.data.impl.KeyExtent> trans = new TreeSet<>();
    for (KeyExtent wrapper : tablets) {
      trans.add(wrapper.wrapped);
    }

    return trans;
  }

  private static KeyExtent wrap(org.apache.accumulo.core.data.impl.KeyExtent ke) {
    return new KeyExtent(ke.getTableId(), ke.getEndRow(), ke.getPrevEndRow());
  }

  private static SortedSet<KeyExtent> wrap(Collection<org.apache.accumulo.core.data.impl.KeyExtent> unwrapped) {
    SortedSet<KeyExtent> wrapped = new TreeSet<>();
    for (org.apache.accumulo.core.data.impl.KeyExtent wrappee : unwrapped) {
      wrapped.add(wrap(wrappee));
    }

    return wrapped;
  }

  public static Text getMetadataEntry(Text tableId, Text endRow) {
    return MetadataSchema.TabletsSection.getRow(tableId, endRow);
  }

  /**
   * Empty start or end rows tell the method there are no start or end rows, and to use all the keyextents that are before the end row if no start row etc.
   *
   * @deprecated this method not intended for public use and is likely to be removed in a future version.
   * @return all the key extents that the rows cover
   */
  @Deprecated
  public static Collection<KeyExtent> getKeyExtentsForRange(Text startRow, Text endRow, Set<KeyExtent> kes) {
    return wrap(org.apache.accumulo.core.data.impl.KeyExtent.getKeyExtentsForRange(startRow, endRow, unwrap(kes)));
  }

  public static Text decodePrevEndRow(Value ibw) {
    return org.apache.accumulo.core.data.impl.KeyExtent.decodePrevEndRow(ibw);
  }

  public static Value encodePrevEndRow(Text per) {
    return org.apache.accumulo.core.data.impl.KeyExtent.encodePrevEndRow(per);
  }

  public static Mutation getPrevRowUpdateMutation(KeyExtent ke) {
    return org.apache.accumulo.core.data.impl.KeyExtent.getPrevRowUpdateMutation(ke.wrapped);
  }

  public static byte[] tableOfMetadataRow(Text row) {
    return org.apache.accumulo.core.data.impl.KeyExtent.tableOfMetadataRow(row);
  }

  public static SortedSet<KeyExtent> findChildren(KeyExtent ke, SortedSet<KeyExtent> tablets) {
    return wrap(org.apache.accumulo.core.data.impl.KeyExtent.findChildren(ke.wrapped, unwrap(tablets)));
  }

  public static KeyExtent findContainingExtent(KeyExtent extent, SortedSet<KeyExtent> extents) {
    return wrap(org.apache.accumulo.core.data.impl.KeyExtent.findContainingExtent(extent.wrapped, unwrap(extents)));
  }

  public static Set<KeyExtent> findOverlapping(KeyExtent nke, SortedSet<KeyExtent> extents) {
    return wrap(org.apache.accumulo.core.data.impl.KeyExtent.findOverlapping(nke.wrapped, unwrap(extents)));
  }

  public static Set<KeyExtent> findOverlapping(KeyExtent nke, SortedMap<KeyExtent,?> extents) {
    SortedMap<org.apache.accumulo.core.data.impl.KeyExtent,Object> trans = new TreeMap<>();
    for (Entry<KeyExtent,?> entry : extents.entrySet()) {
      trans.put(entry.getKey().wrapped, entry.getValue());
    }

    return wrap(org.apache.accumulo.core.data.impl.KeyExtent.findOverlapping(nke.wrapped, trans));
  }

  public static Text getMetadataEntry(KeyExtent extent) {
    return org.apache.accumulo.core.data.impl.KeyExtent.getMetadataEntry(extent.wrapped);
  }
}
