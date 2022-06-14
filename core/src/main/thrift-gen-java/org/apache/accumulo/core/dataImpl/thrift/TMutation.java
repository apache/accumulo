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
/**
 * Autogenerated by Thrift Compiler (0.16.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.dataImpl.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class TMutation implements org.apache.thrift.TBase<TMutation, TMutation._Fields>, java.io.Serializable, Cloneable, Comparable<TMutation> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TMutation");

  private static final org.apache.thrift.protocol.TField ROW_FIELD_DESC = new org.apache.thrift.protocol.TField("row", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField DATA_FIELD_DESC = new org.apache.thrift.protocol.TField("data", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField VALUES_FIELD_DESC = new org.apache.thrift.protocol.TField("values", org.apache.thrift.protocol.TType.LIST, (short)3);
  private static final org.apache.thrift.protocol.TField ENTRIES_FIELD_DESC = new org.apache.thrift.protocol.TField("entries", org.apache.thrift.protocol.TType.I32, (short)4);
  private static final org.apache.thrift.protocol.TField SOURCES_FIELD_DESC = new org.apache.thrift.protocol.TField("sources", org.apache.thrift.protocol.TType.LIST, (short)5);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TMutationStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TMutationTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer row; // required
  public @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer data; // required
  public @org.apache.thrift.annotation.Nullable java.util.List<java.nio.ByteBuffer> values; // required
  public int entries; // required
  public @org.apache.thrift.annotation.Nullable java.util.List<java.lang.String> sources; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ROW((short)1, "row"),
    DATA((short)2, "data"),
    VALUES((short)3, "values"),
    ENTRIES((short)4, "entries"),
    SOURCES((short)5, "sources");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // ROW
          return ROW;
        case 2: // DATA
          return DATA;
        case 3: // VALUES
          return VALUES;
        case 4: // ENTRIES
          return ENTRIES;
        case 5: // SOURCES
          return SOURCES;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __ENTRIES_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.SOURCES};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ROW, new org.apache.thrift.meta_data.FieldMetaData("row", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    tmpMap.put(_Fields.DATA, new org.apache.thrift.meta_data.FieldMetaData("data", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    tmpMap.put(_Fields.VALUES, new org.apache.thrift.meta_data.FieldMetaData("values", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING            , true))));
    tmpMap.put(_Fields.ENTRIES, new org.apache.thrift.meta_data.FieldMetaData("entries", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.SOURCES, new org.apache.thrift.meta_data.FieldMetaData("sources", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TMutation.class, metaDataMap);
  }

  public TMutation() {
  }

  public TMutation(
    java.nio.ByteBuffer row,
    java.nio.ByteBuffer data,
    java.util.List<java.nio.ByteBuffer> values,
    int entries)
  {
    this();
    this.row = org.apache.thrift.TBaseHelper.copyBinary(row);
    this.data = org.apache.thrift.TBaseHelper.copyBinary(data);
    this.values = values;
    this.entries = entries;
    setEntriesIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TMutation(TMutation other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetRow()) {
      this.row = org.apache.thrift.TBaseHelper.copyBinary(other.row);
    }
    if (other.isSetData()) {
      this.data = org.apache.thrift.TBaseHelper.copyBinary(other.data);
    }
    if (other.isSetValues()) {
      java.util.List<java.nio.ByteBuffer> __this__values = new java.util.ArrayList<java.nio.ByteBuffer>(other.values);
      this.values = __this__values;
    }
    this.entries = other.entries;
    if (other.isSetSources()) {
      java.util.List<java.lang.String> __this__sources = new java.util.ArrayList<java.lang.String>(other.sources);
      this.sources = __this__sources;
    }
  }

  public TMutation deepCopy() {
    return new TMutation(this);
  }

  @Override
  public void clear() {
    this.row = null;
    this.data = null;
    this.values = null;
    setEntriesIsSet(false);
    this.entries = 0;
    this.sources = null;
  }

  public byte[] getRow() {
    setRow(org.apache.thrift.TBaseHelper.rightSize(row));
    return row == null ? null : row.array();
  }

  public java.nio.ByteBuffer bufferForRow() {
    return org.apache.thrift.TBaseHelper.copyBinary(row);
  }

  public TMutation setRow(byte[] row) {
    this.row = row == null ? (java.nio.ByteBuffer)null   : java.nio.ByteBuffer.wrap(row.clone());
    return this;
  }

  public TMutation setRow(@org.apache.thrift.annotation.Nullable java.nio.ByteBuffer row) {
    this.row = org.apache.thrift.TBaseHelper.copyBinary(row);
    return this;
  }

  public void unsetRow() {
    this.row = null;
  }

  /** Returns true if field row is set (has been assigned a value) and false otherwise */
  public boolean isSetRow() {
    return this.row != null;
  }

  public void setRowIsSet(boolean value) {
    if (!value) {
      this.row = null;
    }
  }

  public byte[] getData() {
    setData(org.apache.thrift.TBaseHelper.rightSize(data));
    return data == null ? null : data.array();
  }

  public java.nio.ByteBuffer bufferForData() {
    return org.apache.thrift.TBaseHelper.copyBinary(data);
  }

  public TMutation setData(byte[] data) {
    this.data = data == null ? (java.nio.ByteBuffer)null   : java.nio.ByteBuffer.wrap(data.clone());
    return this;
  }

  public TMutation setData(@org.apache.thrift.annotation.Nullable java.nio.ByteBuffer data) {
    this.data = org.apache.thrift.TBaseHelper.copyBinary(data);
    return this;
  }

  public void unsetData() {
    this.data = null;
  }

  /** Returns true if field data is set (has been assigned a value) and false otherwise */
  public boolean isSetData() {
    return this.data != null;
  }

  public void setDataIsSet(boolean value) {
    if (!value) {
      this.data = null;
    }
  }

  public int getValuesSize() {
    return (this.values == null) ? 0 : this.values.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<java.nio.ByteBuffer> getValuesIterator() {
    return (this.values == null) ? null : this.values.iterator();
  }

  public void addToValues(java.nio.ByteBuffer elem) {
    if (this.values == null) {
      this.values = new java.util.ArrayList<java.nio.ByteBuffer>();
    }
    this.values.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<java.nio.ByteBuffer> getValues() {
    return this.values;
  }

  public TMutation setValues(@org.apache.thrift.annotation.Nullable java.util.List<java.nio.ByteBuffer> values) {
    this.values = values;
    return this;
  }

  public void unsetValues() {
    this.values = null;
  }

  /** Returns true if field values is set (has been assigned a value) and false otherwise */
  public boolean isSetValues() {
    return this.values != null;
  }

  public void setValuesIsSet(boolean value) {
    if (!value) {
      this.values = null;
    }
  }

  public int getEntries() {
    return this.entries;
  }

  public TMutation setEntries(int entries) {
    this.entries = entries;
    setEntriesIsSet(true);
    return this;
  }

  public void unsetEntries() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __ENTRIES_ISSET_ID);
  }

  /** Returns true if field entries is set (has been assigned a value) and false otherwise */
  public boolean isSetEntries() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __ENTRIES_ISSET_ID);
  }

  public void setEntriesIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __ENTRIES_ISSET_ID, value);
  }

  public int getSourcesSize() {
    return (this.sources == null) ? 0 : this.sources.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<java.lang.String> getSourcesIterator() {
    return (this.sources == null) ? null : this.sources.iterator();
  }

  public void addToSources(java.lang.String elem) {
    if (this.sources == null) {
      this.sources = new java.util.ArrayList<java.lang.String>();
    }
    this.sources.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<java.lang.String> getSources() {
    return this.sources;
  }

  public TMutation setSources(@org.apache.thrift.annotation.Nullable java.util.List<java.lang.String> sources) {
    this.sources = sources;
    return this;
  }

  public void unsetSources() {
    this.sources = null;
  }

  /** Returns true if field sources is set (has been assigned a value) and false otherwise */
  public boolean isSetSources() {
    return this.sources != null;
  }

  public void setSourcesIsSet(boolean value) {
    if (!value) {
      this.sources = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case ROW:
      if (value == null) {
        unsetRow();
      } else {
        if (value instanceof byte[]) {
          setRow((byte[])value);
        } else {
          setRow((java.nio.ByteBuffer)value);
        }
      }
      break;

    case DATA:
      if (value == null) {
        unsetData();
      } else {
        if (value instanceof byte[]) {
          setData((byte[])value);
        } else {
          setData((java.nio.ByteBuffer)value);
        }
      }
      break;

    case VALUES:
      if (value == null) {
        unsetValues();
      } else {
        setValues((java.util.List<java.nio.ByteBuffer>)value);
      }
      break;

    case ENTRIES:
      if (value == null) {
        unsetEntries();
      } else {
        setEntries((java.lang.Integer)value);
      }
      break;

    case SOURCES:
      if (value == null) {
        unsetSources();
      } else {
        setSources((java.util.List<java.lang.String>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case ROW:
      return getRow();

    case DATA:
      return getData();

    case VALUES:
      return getValues();

    case ENTRIES:
      return getEntries();

    case SOURCES:
      return getSources();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case ROW:
      return isSetRow();
    case DATA:
      return isSetData();
    case VALUES:
      return isSetValues();
    case ENTRIES:
      return isSetEntries();
    case SOURCES:
      return isSetSources();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TMutation)
      return this.equals((TMutation)that);
    return false;
  }

  public boolean equals(TMutation that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_row = true && this.isSetRow();
    boolean that_present_row = true && that.isSetRow();
    if (this_present_row || that_present_row) {
      if (!(this_present_row && that_present_row))
        return false;
      if (!this.row.equals(that.row))
        return false;
    }

    boolean this_present_data = true && this.isSetData();
    boolean that_present_data = true && that.isSetData();
    if (this_present_data || that_present_data) {
      if (!(this_present_data && that_present_data))
        return false;
      if (!this.data.equals(that.data))
        return false;
    }

    boolean this_present_values = true && this.isSetValues();
    boolean that_present_values = true && that.isSetValues();
    if (this_present_values || that_present_values) {
      if (!(this_present_values && that_present_values))
        return false;
      if (!this.values.equals(that.values))
        return false;
    }

    boolean this_present_entries = true;
    boolean that_present_entries = true;
    if (this_present_entries || that_present_entries) {
      if (!(this_present_entries && that_present_entries))
        return false;
      if (this.entries != that.entries)
        return false;
    }

    boolean this_present_sources = true && this.isSetSources();
    boolean that_present_sources = true && that.isSetSources();
    if (this_present_sources || that_present_sources) {
      if (!(this_present_sources && that_present_sources))
        return false;
      if (!this.sources.equals(that.sources))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetRow()) ? 131071 : 524287);
    if (isSetRow())
      hashCode = hashCode * 8191 + row.hashCode();

    hashCode = hashCode * 8191 + ((isSetData()) ? 131071 : 524287);
    if (isSetData())
      hashCode = hashCode * 8191 + data.hashCode();

    hashCode = hashCode * 8191 + ((isSetValues()) ? 131071 : 524287);
    if (isSetValues())
      hashCode = hashCode * 8191 + values.hashCode();

    hashCode = hashCode * 8191 + entries;

    hashCode = hashCode * 8191 + ((isSetSources()) ? 131071 : 524287);
    if (isSetSources())
      hashCode = hashCode * 8191 + sources.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TMutation other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetRow(), other.isSetRow());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRow()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.row, other.row);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetData(), other.isSetData());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetData()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.data, other.data);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetValues(), other.isSetValues());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetValues()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.values, other.values);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetEntries(), other.isSetEntries());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEntries()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.entries, other.entries);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetSources(), other.isSetSources());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSources()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sources, other.sources);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TMutation(");
    boolean first = true;

    sb.append("row:");
    if (this.row == null) {
      sb.append("null");
    } else {
      org.apache.thrift.TBaseHelper.toString(this.row, sb);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("data:");
    if (this.data == null) {
      sb.append("null");
    } else {
      org.apache.thrift.TBaseHelper.toString(this.data, sb);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("values:");
    if (this.values == null) {
      sb.append("null");
    } else {
      org.apache.thrift.TBaseHelper.toString(this.values, sb);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("entries:");
    sb.append(this.entries);
    first = false;
    if (isSetSources()) {
      if (!first) sb.append(", ");
      sb.append("sources:");
      if (this.sources == null) {
        sb.append("null");
      } else {
        sb.append(this.sources);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TMutationStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TMutationStandardScheme getScheme() {
      return new TMutationStandardScheme();
    }
  }

  private static class TMutationStandardScheme extends org.apache.thrift.scheme.StandardScheme<TMutation> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TMutation struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // ROW
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.row = iprot.readBinary();
              struct.setRowIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // DATA
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.data = iprot.readBinary();
              struct.setDataIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // VALUES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list0 = iprot.readListBegin();
                struct.values = new java.util.ArrayList<java.nio.ByteBuffer>(_list0.size);
                @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer _elem1;
                for (int _i2 = 0; _i2 < _list0.size; ++_i2)
                {
                  _elem1 = iprot.readBinary();
                  struct.values.add(_elem1);
                }
                iprot.readListEnd();
              }
              struct.setValuesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // ENTRIES
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.entries = iprot.readI32();
              struct.setEntriesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // SOURCES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list3 = iprot.readListBegin();
                struct.sources = new java.util.ArrayList<java.lang.String>(_list3.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _elem4;
                for (int _i5 = 0; _i5 < _list3.size; ++_i5)
                {
                  _elem4 = iprot.readString();
                  struct.sources.add(_elem4);
                }
                iprot.readListEnd();
              }
              struct.setSourcesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TMutation struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.row != null) {
        oprot.writeFieldBegin(ROW_FIELD_DESC);
        oprot.writeBinary(struct.row);
        oprot.writeFieldEnd();
      }
      if (struct.data != null) {
        oprot.writeFieldBegin(DATA_FIELD_DESC);
        oprot.writeBinary(struct.data);
        oprot.writeFieldEnd();
      }
      if (struct.values != null) {
        oprot.writeFieldBegin(VALUES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.values.size()));
          for (java.nio.ByteBuffer _iter6 : struct.values)
          {
            oprot.writeBinary(_iter6);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(ENTRIES_FIELD_DESC);
      oprot.writeI32(struct.entries);
      oprot.writeFieldEnd();
      if (struct.sources != null) {
        if (struct.isSetSources()) {
          oprot.writeFieldBegin(SOURCES_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.sources.size()));
            for (java.lang.String _iter7 : struct.sources)
            {
              oprot.writeString(_iter7);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TMutationTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TMutationTupleScheme getScheme() {
      return new TMutationTupleScheme();
    }
  }

  private static class TMutationTupleScheme extends org.apache.thrift.scheme.TupleScheme<TMutation> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TMutation struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetRow()) {
        optionals.set(0);
      }
      if (struct.isSetData()) {
        optionals.set(1);
      }
      if (struct.isSetValues()) {
        optionals.set(2);
      }
      if (struct.isSetEntries()) {
        optionals.set(3);
      }
      if (struct.isSetSources()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetRow()) {
        oprot.writeBinary(struct.row);
      }
      if (struct.isSetData()) {
        oprot.writeBinary(struct.data);
      }
      if (struct.isSetValues()) {
        {
          oprot.writeI32(struct.values.size());
          for (java.nio.ByteBuffer _iter8 : struct.values)
          {
            oprot.writeBinary(_iter8);
          }
        }
      }
      if (struct.isSetEntries()) {
        oprot.writeI32(struct.entries);
      }
      if (struct.isSetSources()) {
        {
          oprot.writeI32(struct.sources.size());
          for (java.lang.String _iter9 : struct.sources)
          {
            oprot.writeString(_iter9);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TMutation struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.row = iprot.readBinary();
        struct.setRowIsSet(true);
      }
      if (incoming.get(1)) {
        struct.data = iprot.readBinary();
        struct.setDataIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TList _list10 = iprot.readListBegin(org.apache.thrift.protocol.TType.STRING);
          struct.values = new java.util.ArrayList<java.nio.ByteBuffer>(_list10.size);
          @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer _elem11;
          for (int _i12 = 0; _i12 < _list10.size; ++_i12)
          {
            _elem11 = iprot.readBinary();
            struct.values.add(_elem11);
          }
        }
        struct.setValuesIsSet(true);
      }
      if (incoming.get(3)) {
        struct.entries = iprot.readI32();
        struct.setEntriesIsSet(true);
      }
      if (incoming.get(4)) {
        {
          org.apache.thrift.protocol.TList _list13 = iprot.readListBegin(org.apache.thrift.protocol.TType.STRING);
          struct.sources = new java.util.ArrayList<java.lang.String>(_list13.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _elem14;
          for (int _i15 = 0; _i15 < _list13.size; ++_i15)
          {
            _elem14 = iprot.readString();
            struct.sources.add(_elem14);
          }
        }
        struct.setSourcesIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
  private static void unusedMethod() {}
}

