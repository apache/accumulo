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
 * Autogenerated by Thrift Compiler (0.17.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.dataImpl.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class TSummaryRequest implements org.apache.thrift.TBase<TSummaryRequest, TSummaryRequest._Fields>, java.io.Serializable, Cloneable, Comparable<TSummaryRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TSummaryRequest");

  private static final org.apache.thrift.protocol.TField TABLE_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("tableId", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField BOUNDS_FIELD_DESC = new org.apache.thrift.protocol.TField("bounds", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField SUMMARIZERS_FIELD_DESC = new org.apache.thrift.protocol.TField("summarizers", org.apache.thrift.protocol.TType.LIST, (short)3);
  private static final org.apache.thrift.protocol.TField SUMMARIZER_PATTERN_FIELD_DESC = new org.apache.thrift.protocol.TField("summarizerPattern", org.apache.thrift.protocol.TType.STRING, (short)4);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TSummaryRequestStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TSummaryRequestTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.lang.String tableId; // required
  public @org.apache.thrift.annotation.Nullable TRowRange bounds; // required
  public @org.apache.thrift.annotation.Nullable java.util.List<TSummarizerConfiguration> summarizers; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String summarizerPattern; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TABLE_ID((short)1, "tableId"),
    BOUNDS((short)2, "bounds"),
    SUMMARIZERS((short)3, "summarizers"),
    SUMMARIZER_PATTERN((short)4, "summarizerPattern");

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
        case 1: // TABLE_ID
          return TABLE_ID;
        case 2: // BOUNDS
          return BOUNDS;
        case 3: // SUMMARIZERS
          return SUMMARIZERS;
        case 4: // SUMMARIZER_PATTERN
          return SUMMARIZER_PATTERN;
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

    @Override
    public short getThriftFieldId() {
      return _thriftId;
    }

    @Override
    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TABLE_ID, new org.apache.thrift.meta_data.FieldMetaData("tableId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.BOUNDS, new org.apache.thrift.meta_data.FieldMetaData("bounds", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TRowRange.class)));
    tmpMap.put(_Fields.SUMMARIZERS, new org.apache.thrift.meta_data.FieldMetaData("summarizers", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TSummarizerConfiguration.class))));
    tmpMap.put(_Fields.SUMMARIZER_PATTERN, new org.apache.thrift.meta_data.FieldMetaData("summarizerPattern", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TSummaryRequest.class, metaDataMap);
  }

  public TSummaryRequest() {
  }

  public TSummaryRequest(
    java.lang.String tableId,
    TRowRange bounds,
    java.util.List<TSummarizerConfiguration> summarizers,
    java.lang.String summarizerPattern)
  {
    this();
    this.tableId = tableId;
    this.bounds = bounds;
    this.summarizers = summarizers;
    this.summarizerPattern = summarizerPattern;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TSummaryRequest(TSummaryRequest other) {
    if (other.isSetTableId()) {
      this.tableId = other.tableId;
    }
    if (other.isSetBounds()) {
      this.bounds = new TRowRange(other.bounds);
    }
    if (other.isSetSummarizers()) {
      java.util.List<TSummarizerConfiguration> __this__summarizers = new java.util.ArrayList<TSummarizerConfiguration>(other.summarizers.size());
      for (TSummarizerConfiguration other_element : other.summarizers) {
        __this__summarizers.add(new TSummarizerConfiguration(other_element));
      }
      this.summarizers = __this__summarizers;
    }
    if (other.isSetSummarizerPattern()) {
      this.summarizerPattern = other.summarizerPattern;
    }
  }

  @Override
  public TSummaryRequest deepCopy() {
    return new TSummaryRequest(this);
  }

  @Override
  public void clear() {
    this.tableId = null;
    this.bounds = null;
    this.summarizers = null;
    this.summarizerPattern = null;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getTableId() {
    return this.tableId;
  }

  public TSummaryRequest setTableId(@org.apache.thrift.annotation.Nullable java.lang.String tableId) {
    this.tableId = tableId;
    return this;
  }

  public void unsetTableId() {
    this.tableId = null;
  }

  /** Returns true if field tableId is set (has been assigned a value) and false otherwise */
  public boolean isSetTableId() {
    return this.tableId != null;
  }

  public void setTableIdIsSet(boolean value) {
    if (!value) {
      this.tableId = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public TRowRange getBounds() {
    return this.bounds;
  }

  public TSummaryRequest setBounds(@org.apache.thrift.annotation.Nullable TRowRange bounds) {
    this.bounds = bounds;
    return this;
  }

  public void unsetBounds() {
    this.bounds = null;
  }

  /** Returns true if field bounds is set (has been assigned a value) and false otherwise */
  public boolean isSetBounds() {
    return this.bounds != null;
  }

  public void setBoundsIsSet(boolean value) {
    if (!value) {
      this.bounds = null;
    }
  }

  public int getSummarizersSize() {
    return (this.summarizers == null) ? 0 : this.summarizers.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<TSummarizerConfiguration> getSummarizersIterator() {
    return (this.summarizers == null) ? null : this.summarizers.iterator();
  }

  public void addToSummarizers(TSummarizerConfiguration elem) {
    if (this.summarizers == null) {
      this.summarizers = new java.util.ArrayList<TSummarizerConfiguration>();
    }
    this.summarizers.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<TSummarizerConfiguration> getSummarizers() {
    return this.summarizers;
  }

  public TSummaryRequest setSummarizers(@org.apache.thrift.annotation.Nullable java.util.List<TSummarizerConfiguration> summarizers) {
    this.summarizers = summarizers;
    return this;
  }

  public void unsetSummarizers() {
    this.summarizers = null;
  }

  /** Returns true if field summarizers is set (has been assigned a value) and false otherwise */
  public boolean isSetSummarizers() {
    return this.summarizers != null;
  }

  public void setSummarizersIsSet(boolean value) {
    if (!value) {
      this.summarizers = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getSummarizerPattern() {
    return this.summarizerPattern;
  }

  public TSummaryRequest setSummarizerPattern(@org.apache.thrift.annotation.Nullable java.lang.String summarizerPattern) {
    this.summarizerPattern = summarizerPattern;
    return this;
  }

  public void unsetSummarizerPattern() {
    this.summarizerPattern = null;
  }

  /** Returns true if field summarizerPattern is set (has been assigned a value) and false otherwise */
  public boolean isSetSummarizerPattern() {
    return this.summarizerPattern != null;
  }

  public void setSummarizerPatternIsSet(boolean value) {
    if (!value) {
      this.summarizerPattern = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case TABLE_ID:
      if (value == null) {
        unsetTableId();
      } else {
        setTableId((java.lang.String)value);
      }
      break;

    case BOUNDS:
      if (value == null) {
        unsetBounds();
      } else {
        setBounds((TRowRange)value);
      }
      break;

    case SUMMARIZERS:
      if (value == null) {
        unsetSummarizers();
      } else {
        setSummarizers((java.util.List<TSummarizerConfiguration>)value);
      }
      break;

    case SUMMARIZER_PATTERN:
      if (value == null) {
        unsetSummarizerPattern();
      } else {
        setSummarizerPattern((java.lang.String)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case TABLE_ID:
      return getTableId();

    case BOUNDS:
      return getBounds();

    case SUMMARIZERS:
      return getSummarizers();

    case SUMMARIZER_PATTERN:
      return getSummarizerPattern();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  @Override
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case TABLE_ID:
      return isSetTableId();
    case BOUNDS:
      return isSetBounds();
    case SUMMARIZERS:
      return isSetSummarizers();
    case SUMMARIZER_PATTERN:
      return isSetSummarizerPattern();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TSummaryRequest)
      return this.equals((TSummaryRequest)that);
    return false;
  }

  public boolean equals(TSummaryRequest that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_tableId = true && this.isSetTableId();
    boolean that_present_tableId = true && that.isSetTableId();
    if (this_present_tableId || that_present_tableId) {
      if (!(this_present_tableId && that_present_tableId))
        return false;
      if (!this.tableId.equals(that.tableId))
        return false;
    }

    boolean this_present_bounds = true && this.isSetBounds();
    boolean that_present_bounds = true && that.isSetBounds();
    if (this_present_bounds || that_present_bounds) {
      if (!(this_present_bounds && that_present_bounds))
        return false;
      if (!this.bounds.equals(that.bounds))
        return false;
    }

    boolean this_present_summarizers = true && this.isSetSummarizers();
    boolean that_present_summarizers = true && that.isSetSummarizers();
    if (this_present_summarizers || that_present_summarizers) {
      if (!(this_present_summarizers && that_present_summarizers))
        return false;
      if (!this.summarizers.equals(that.summarizers))
        return false;
    }

    boolean this_present_summarizerPattern = true && this.isSetSummarizerPattern();
    boolean that_present_summarizerPattern = true && that.isSetSummarizerPattern();
    if (this_present_summarizerPattern || that_present_summarizerPattern) {
      if (!(this_present_summarizerPattern && that_present_summarizerPattern))
        return false;
      if (!this.summarizerPattern.equals(that.summarizerPattern))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetTableId()) ? 131071 : 524287);
    if (isSetTableId())
      hashCode = hashCode * 8191 + tableId.hashCode();

    hashCode = hashCode * 8191 + ((isSetBounds()) ? 131071 : 524287);
    if (isSetBounds())
      hashCode = hashCode * 8191 + bounds.hashCode();

    hashCode = hashCode * 8191 + ((isSetSummarizers()) ? 131071 : 524287);
    if (isSetSummarizers())
      hashCode = hashCode * 8191 + summarizers.hashCode();

    hashCode = hashCode * 8191 + ((isSetSummarizerPattern()) ? 131071 : 524287);
    if (isSetSummarizerPattern())
      hashCode = hashCode * 8191 + summarizerPattern.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TSummaryRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetTableId(), other.isSetTableId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTableId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tableId, other.tableId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetBounds(), other.isSetBounds());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBounds()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.bounds, other.bounds);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetSummarizers(), other.isSetSummarizers());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSummarizers()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.summarizers, other.summarizers);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetSummarizerPattern(), other.isSetSummarizerPattern());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSummarizerPattern()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.summarizerPattern, other.summarizerPattern);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  @Override
  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  @Override
  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TSummaryRequest(");
    boolean first = true;

    sb.append("tableId:");
    if (this.tableId == null) {
      sb.append("null");
    } else {
      sb.append(this.tableId);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("bounds:");
    if (this.bounds == null) {
      sb.append("null");
    } else {
      sb.append(this.bounds);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("summarizers:");
    if (this.summarizers == null) {
      sb.append("null");
    } else {
      sb.append(this.summarizers);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("summarizerPattern:");
    if (this.summarizerPattern == null) {
      sb.append("null");
    } else {
      sb.append(this.summarizerPattern);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (bounds != null) {
      bounds.validate();
    }
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TSummaryRequestStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TSummaryRequestStandardScheme getScheme() {
      return new TSummaryRequestStandardScheme();
    }
  }

  private static class TSummaryRequestStandardScheme extends org.apache.thrift.scheme.StandardScheme<TSummaryRequest> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, TSummaryRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TABLE_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.tableId = iprot.readString();
              struct.setTableIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // BOUNDS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.bounds = new TRowRange();
              struct.bounds.read(iprot);
              struct.setBoundsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SUMMARIZERS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list122 = iprot.readListBegin();
                struct.summarizers = new java.util.ArrayList<TSummarizerConfiguration>(_list122.size);
                @org.apache.thrift.annotation.Nullable TSummarizerConfiguration _elem123;
                for (int _i124 = 0; _i124 < _list122.size; ++_i124)
                {
                  _elem123 = new TSummarizerConfiguration();
                  _elem123.read(iprot);
                  struct.summarizers.add(_elem123);
                }
                iprot.readListEnd();
              }
              struct.setSummarizersIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // SUMMARIZER_PATTERN
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.summarizerPattern = iprot.readString();
              struct.setSummarizerPatternIsSet(true);
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

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot, TSummaryRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.tableId != null) {
        oprot.writeFieldBegin(TABLE_ID_FIELD_DESC);
        oprot.writeString(struct.tableId);
        oprot.writeFieldEnd();
      }
      if (struct.bounds != null) {
        oprot.writeFieldBegin(BOUNDS_FIELD_DESC);
        struct.bounds.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.summarizers != null) {
        oprot.writeFieldBegin(SUMMARIZERS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.summarizers.size()));
          for (TSummarizerConfiguration _iter125 : struct.summarizers)
          {
            _iter125.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.summarizerPattern != null) {
        oprot.writeFieldBegin(SUMMARIZER_PATTERN_FIELD_DESC);
        oprot.writeString(struct.summarizerPattern);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TSummaryRequestTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TSummaryRequestTupleScheme getScheme() {
      return new TSummaryRequestTupleScheme();
    }
  }

  private static class TSummaryRequestTupleScheme extends org.apache.thrift.scheme.TupleScheme<TSummaryRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TSummaryRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetTableId()) {
        optionals.set(0);
      }
      if (struct.isSetBounds()) {
        optionals.set(1);
      }
      if (struct.isSetSummarizers()) {
        optionals.set(2);
      }
      if (struct.isSetSummarizerPattern()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetTableId()) {
        oprot.writeString(struct.tableId);
      }
      if (struct.isSetBounds()) {
        struct.bounds.write(oprot);
      }
      if (struct.isSetSummarizers()) {
        {
          oprot.writeI32(struct.summarizers.size());
          for (TSummarizerConfiguration _iter126 : struct.summarizers)
          {
            _iter126.write(oprot);
          }
        }
      }
      if (struct.isSetSummarizerPattern()) {
        oprot.writeString(struct.summarizerPattern);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TSummaryRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.tableId = iprot.readString();
        struct.setTableIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.bounds = new TRowRange();
        struct.bounds.read(iprot);
        struct.setBoundsIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TList _list127 = iprot.readListBegin(org.apache.thrift.protocol.TType.STRUCT);
          struct.summarizers = new java.util.ArrayList<TSummarizerConfiguration>(_list127.size);
          @org.apache.thrift.annotation.Nullable TSummarizerConfiguration _elem128;
          for (int _i129 = 0; _i129 < _list127.size; ++_i129)
          {
            _elem128 = new TSummarizerConfiguration();
            _elem128.read(iprot);
            struct.summarizers.add(_elem128);
          }
        }
        struct.setSummarizersIsSet(true);
      }
      if (incoming.get(3)) {
        struct.summarizerPattern = iprot.readString();
        struct.setSummarizerPatternIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
  private static void unusedMethod() {}
}

