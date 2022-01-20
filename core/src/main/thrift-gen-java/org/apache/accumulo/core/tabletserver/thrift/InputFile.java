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
/**
 * Autogenerated by Thrift Compiler (0.15.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.tabletserver.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class InputFile implements org.apache.thrift.TBase<InputFile, InputFile._Fields>, java.io.Serializable, Cloneable, Comparable<InputFile> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("InputFile");

  private static final org.apache.thrift.protocol.TField METADATA_FILE_ENTRY_FIELD_DESC = new org.apache.thrift.protocol.TField("metadataFileEntry", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField SIZE_FIELD_DESC = new org.apache.thrift.protocol.TField("size", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField ENTRIES_FIELD_DESC = new org.apache.thrift.protocol.TField("entries", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField TIMESTAMP_FIELD_DESC = new org.apache.thrift.protocol.TField("timestamp", org.apache.thrift.protocol.TType.I64, (short)4);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new InputFileStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new InputFileTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.lang.String metadataFileEntry; // required
  public long size; // required
  public long entries; // required
  public long timestamp; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    METADATA_FILE_ENTRY((short)1, "metadataFileEntry"),
    SIZE((short)2, "size"),
    ENTRIES((short)3, "entries"),
    TIMESTAMP((short)4, "timestamp");

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
        case 1: // METADATA_FILE_ENTRY
          return METADATA_FILE_ENTRY;
        case 2: // SIZE
          return SIZE;
        case 3: // ENTRIES
          return ENTRIES;
        case 4: // TIMESTAMP
          return TIMESTAMP;
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
  private static final int __SIZE_ISSET_ID = 0;
  private static final int __ENTRIES_ISSET_ID = 1;
  private static final int __TIMESTAMP_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.METADATA_FILE_ENTRY, new org.apache.thrift.meta_data.FieldMetaData("metadataFileEntry", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.SIZE, new org.apache.thrift.meta_data.FieldMetaData("size", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.ENTRIES, new org.apache.thrift.meta_data.FieldMetaData("entries", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.TIMESTAMP, new org.apache.thrift.meta_data.FieldMetaData("timestamp", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(InputFile.class, metaDataMap);
  }

  public InputFile() {
  }

  public InputFile(
    java.lang.String metadataFileEntry,
    long size,
    long entries,
    long timestamp)
  {
    this();
    this.metadataFileEntry = metadataFileEntry;
    this.size = size;
    setSizeIsSet(true);
    this.entries = entries;
    setEntriesIsSet(true);
    this.timestamp = timestamp;
    setTimestampIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public InputFile(InputFile other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetMetadataFileEntry()) {
      this.metadataFileEntry = other.metadataFileEntry;
    }
    this.size = other.size;
    this.entries = other.entries;
    this.timestamp = other.timestamp;
  }

  public InputFile deepCopy() {
    return new InputFile(this);
  }

  @Override
  public void clear() {
    this.metadataFileEntry = null;
    setSizeIsSet(false);
    this.size = 0;
    setEntriesIsSet(false);
    this.entries = 0;
    setTimestampIsSet(false);
    this.timestamp = 0;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getMetadataFileEntry() {
    return this.metadataFileEntry;
  }

  public InputFile setMetadataFileEntry(@org.apache.thrift.annotation.Nullable java.lang.String metadataFileEntry) {
    this.metadataFileEntry = metadataFileEntry;
    return this;
  }

  public void unsetMetadataFileEntry() {
    this.metadataFileEntry = null;
  }

  /** Returns true if field metadataFileEntry is set (has been assigned a value) and false otherwise */
  public boolean isSetMetadataFileEntry() {
    return this.metadataFileEntry != null;
  }

  public void setMetadataFileEntryIsSet(boolean value) {
    if (!value) {
      this.metadataFileEntry = null;
    }
  }

  public long getSize() {
    return this.size;
  }

  public InputFile setSize(long size) {
    this.size = size;
    setSizeIsSet(true);
    return this;
  }

  public void unsetSize() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __SIZE_ISSET_ID);
  }

  /** Returns true if field size is set (has been assigned a value) and false otherwise */
  public boolean isSetSize() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __SIZE_ISSET_ID);
  }

  public void setSizeIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __SIZE_ISSET_ID, value);
  }

  public long getEntries() {
    return this.entries;
  }

  public InputFile setEntries(long entries) {
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

  public long getTimestamp() {
    return this.timestamp;
  }

  public InputFile setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    setTimestampIsSet(true);
    return this;
  }

  public void unsetTimestamp() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __TIMESTAMP_ISSET_ID);
  }

  /** Returns true if field timestamp is set (has been assigned a value) and false otherwise */
  public boolean isSetTimestamp() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __TIMESTAMP_ISSET_ID);
  }

  public void setTimestampIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __TIMESTAMP_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case METADATA_FILE_ENTRY:
      if (value == null) {
        unsetMetadataFileEntry();
      } else {
        setMetadataFileEntry((java.lang.String)value);
      }
      break;

    case SIZE:
      if (value == null) {
        unsetSize();
      } else {
        setSize((java.lang.Long)value);
      }
      break;

    case ENTRIES:
      if (value == null) {
        unsetEntries();
      } else {
        setEntries((java.lang.Long)value);
      }
      break;

    case TIMESTAMP:
      if (value == null) {
        unsetTimestamp();
      } else {
        setTimestamp((java.lang.Long)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case METADATA_FILE_ENTRY:
      return getMetadataFileEntry();

    case SIZE:
      return getSize();

    case ENTRIES:
      return getEntries();

    case TIMESTAMP:
      return getTimestamp();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case METADATA_FILE_ENTRY:
      return isSetMetadataFileEntry();
    case SIZE:
      return isSetSize();
    case ENTRIES:
      return isSetEntries();
    case TIMESTAMP:
      return isSetTimestamp();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof InputFile)
      return this.equals((InputFile)that);
    return false;
  }

  public boolean equals(InputFile that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_metadataFileEntry = true && this.isSetMetadataFileEntry();
    boolean that_present_metadataFileEntry = true && that.isSetMetadataFileEntry();
    if (this_present_metadataFileEntry || that_present_metadataFileEntry) {
      if (!(this_present_metadataFileEntry && that_present_metadataFileEntry))
        return false;
      if (!this.metadataFileEntry.equals(that.metadataFileEntry))
        return false;
    }

    boolean this_present_size = true;
    boolean that_present_size = true;
    if (this_present_size || that_present_size) {
      if (!(this_present_size && that_present_size))
        return false;
      if (this.size != that.size)
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

    boolean this_present_timestamp = true;
    boolean that_present_timestamp = true;
    if (this_present_timestamp || that_present_timestamp) {
      if (!(this_present_timestamp && that_present_timestamp))
        return false;
      if (this.timestamp != that.timestamp)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetMetadataFileEntry()) ? 131071 : 524287);
    if (isSetMetadataFileEntry())
      hashCode = hashCode * 8191 + metadataFileEntry.hashCode();

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(size);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(entries);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(timestamp);

    return hashCode;
  }

  @Override
  public int compareTo(InputFile other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetMetadataFileEntry(), other.isSetMetadataFileEntry());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMetadataFileEntry()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.metadataFileEntry, other.metadataFileEntry);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetSize(), other.isSetSize());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSize()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.size, other.size);
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
    lastComparison = java.lang.Boolean.compare(isSetTimestamp(), other.isSetTimestamp());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTimestamp()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.timestamp, other.timestamp);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("InputFile(");
    boolean first = true;

    sb.append("metadataFileEntry:");
    if (this.metadataFileEntry == null) {
      sb.append("null");
    } else {
      sb.append(this.metadataFileEntry);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("size:");
    sb.append(this.size);
    first = false;
    if (!first) sb.append(", ");
    sb.append("entries:");
    sb.append(this.entries);
    first = false;
    if (!first) sb.append(", ");
    sb.append("timestamp:");
    sb.append(this.timestamp);
    first = false;
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

  private static class InputFileStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public InputFileStandardScheme getScheme() {
      return new InputFileStandardScheme();
    }
  }

  private static class InputFileStandardScheme extends org.apache.thrift.scheme.StandardScheme<InputFile> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, InputFile struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // METADATA_FILE_ENTRY
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.metadataFileEntry = iprot.readString();
              struct.setMetadataFileEntryIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // SIZE
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.size = iprot.readI64();
              struct.setSizeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // ENTRIES
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.entries = iprot.readI64();
              struct.setEntriesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // TIMESTAMP
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.timestamp = iprot.readI64();
              struct.setTimestampIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, InputFile struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.metadataFileEntry != null) {
        oprot.writeFieldBegin(METADATA_FILE_ENTRY_FIELD_DESC);
        oprot.writeString(struct.metadataFileEntry);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(SIZE_FIELD_DESC);
      oprot.writeI64(struct.size);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(ENTRIES_FIELD_DESC);
      oprot.writeI64(struct.entries);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(TIMESTAMP_FIELD_DESC);
      oprot.writeI64(struct.timestamp);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class InputFileTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public InputFileTupleScheme getScheme() {
      return new InputFileTupleScheme();
    }
  }

  private static class InputFileTupleScheme extends org.apache.thrift.scheme.TupleScheme<InputFile> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, InputFile struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetMetadataFileEntry()) {
        optionals.set(0);
      }
      if (struct.isSetSize()) {
        optionals.set(1);
      }
      if (struct.isSetEntries()) {
        optionals.set(2);
      }
      if (struct.isSetTimestamp()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetMetadataFileEntry()) {
        oprot.writeString(struct.metadataFileEntry);
      }
      if (struct.isSetSize()) {
        oprot.writeI64(struct.size);
      }
      if (struct.isSetEntries()) {
        oprot.writeI64(struct.entries);
      }
      if (struct.isSetTimestamp()) {
        oprot.writeI64(struct.timestamp);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, InputFile struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.metadataFileEntry = iprot.readString();
        struct.setMetadataFileEntryIsSet(true);
      }
      if (incoming.get(1)) {
        struct.size = iprot.readI64();
        struct.setSizeIsSet(true);
      }
      if (incoming.get(2)) {
        struct.entries = iprot.readI64();
        struct.setEntriesIsSet(true);
      }
      if (incoming.get(3)) {
        struct.timestamp = iprot.readI64();
        struct.setTimestampIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
  private static void unusedMethod() {}
}

