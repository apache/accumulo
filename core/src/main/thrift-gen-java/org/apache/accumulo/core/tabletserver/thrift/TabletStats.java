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
package org.apache.accumulo.core.tabletserver.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class TabletStats implements org.apache.thrift.TBase<TabletStats, TabletStats._Fields>, java.io.Serializable, Cloneable, Comparable<TabletStats> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TabletStats");

  private static final org.apache.thrift.protocol.TField EXTENT_FIELD_DESC = new org.apache.thrift.protocol.TField("extent", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField MINORS_FIELD_DESC = new org.apache.thrift.protocol.TField("minors", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField NUM_ENTRIES_FIELD_DESC = new org.apache.thrift.protocol.TField("numEntries", org.apache.thrift.protocol.TType.I64, (short)5);
  private static final org.apache.thrift.protocol.TField INGEST_RATE_FIELD_DESC = new org.apache.thrift.protocol.TField("ingestRate", org.apache.thrift.protocol.TType.DOUBLE, (short)6);
  private static final org.apache.thrift.protocol.TField QUERY_RATE_FIELD_DESC = new org.apache.thrift.protocol.TField("queryRate", org.apache.thrift.protocol.TType.DOUBLE, (short)7);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TabletStatsStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TabletStatsTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable org.apache.accumulo.core.dataImpl.thrift.TKeyExtent extent; // required
  public @org.apache.thrift.annotation.Nullable ActionStats minors; // required
  public long numEntries; // required
  public double ingestRate; // required
  public double queryRate; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    EXTENT((short)1, "extent"),
    MINORS((short)3, "minors"),
    NUM_ENTRIES((short)5, "numEntries"),
    INGEST_RATE((short)6, "ingestRate"),
    QUERY_RATE((short)7, "queryRate");

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
        case 1: // EXTENT
          return EXTENT;
        case 3: // MINORS
          return MINORS;
        case 5: // NUM_ENTRIES
          return NUM_ENTRIES;
        case 6: // INGEST_RATE
          return INGEST_RATE;
        case 7: // QUERY_RATE
          return QUERY_RATE;
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
  private static final int __NUMENTRIES_ISSET_ID = 0;
  private static final int __INGESTRATE_ISSET_ID = 1;
  private static final int __QUERYRATE_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.EXTENT, new org.apache.thrift.meta_data.FieldMetaData("extent", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, org.apache.accumulo.core.dataImpl.thrift.TKeyExtent.class)));
    tmpMap.put(_Fields.MINORS, new org.apache.thrift.meta_data.FieldMetaData("minors", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ActionStats.class)));
    tmpMap.put(_Fields.NUM_ENTRIES, new org.apache.thrift.meta_data.FieldMetaData("numEntries", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.INGEST_RATE, new org.apache.thrift.meta_data.FieldMetaData("ingestRate", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    tmpMap.put(_Fields.QUERY_RATE, new org.apache.thrift.meta_data.FieldMetaData("queryRate", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TabletStats.class, metaDataMap);
  }

  public TabletStats() {
  }

  public TabletStats(
    org.apache.accumulo.core.dataImpl.thrift.TKeyExtent extent,
    ActionStats minors,
    long numEntries,
    double ingestRate,
    double queryRate)
  {
    this();
    this.extent = extent;
    this.minors = minors;
    this.numEntries = numEntries;
    setNumEntriesIsSet(true);
    this.ingestRate = ingestRate;
    setIngestRateIsSet(true);
    this.queryRate = queryRate;
    setQueryRateIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TabletStats(TabletStats other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetExtent()) {
      this.extent = new org.apache.accumulo.core.dataImpl.thrift.TKeyExtent(other.extent);
    }
    if (other.isSetMinors()) {
      this.minors = new ActionStats(other.minors);
    }
    this.numEntries = other.numEntries;
    this.ingestRate = other.ingestRate;
    this.queryRate = other.queryRate;
  }

  @Override
  public TabletStats deepCopy() {
    return new TabletStats(this);
  }

  @Override
  public void clear() {
    this.extent = null;
    this.minors = null;
    setNumEntriesIsSet(false);
    this.numEntries = 0;
    setIngestRateIsSet(false);
    this.ingestRate = 0.0;
    setQueryRateIsSet(false);
    this.queryRate = 0.0;
  }

  @org.apache.thrift.annotation.Nullable
  public org.apache.accumulo.core.dataImpl.thrift.TKeyExtent getExtent() {
    return this.extent;
  }

  public TabletStats setExtent(@org.apache.thrift.annotation.Nullable org.apache.accumulo.core.dataImpl.thrift.TKeyExtent extent) {
    this.extent = extent;
    return this;
  }

  public void unsetExtent() {
    this.extent = null;
  }

  /** Returns true if field extent is set (has been assigned a value) and false otherwise */
  public boolean isSetExtent() {
    return this.extent != null;
  }

  public void setExtentIsSet(boolean value) {
    if (!value) {
      this.extent = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public ActionStats getMinors() {
    return this.minors;
  }

  public TabletStats setMinors(@org.apache.thrift.annotation.Nullable ActionStats minors) {
    this.minors = minors;
    return this;
  }

  public void unsetMinors() {
    this.minors = null;
  }

  /** Returns true if field minors is set (has been assigned a value) and false otherwise */
  public boolean isSetMinors() {
    return this.minors != null;
  }

  public void setMinorsIsSet(boolean value) {
    if (!value) {
      this.minors = null;
    }
  }

  public long getNumEntries() {
    return this.numEntries;
  }

  public TabletStats setNumEntries(long numEntries) {
    this.numEntries = numEntries;
    setNumEntriesIsSet(true);
    return this;
  }

  public void unsetNumEntries() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __NUMENTRIES_ISSET_ID);
  }

  /** Returns true if field numEntries is set (has been assigned a value) and false otherwise */
  public boolean isSetNumEntries() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __NUMENTRIES_ISSET_ID);
  }

  public void setNumEntriesIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __NUMENTRIES_ISSET_ID, value);
  }

  public double getIngestRate() {
    return this.ingestRate;
  }

  public TabletStats setIngestRate(double ingestRate) {
    this.ingestRate = ingestRate;
    setIngestRateIsSet(true);
    return this;
  }

  public void unsetIngestRate() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __INGESTRATE_ISSET_ID);
  }

  /** Returns true if field ingestRate is set (has been assigned a value) and false otherwise */
  public boolean isSetIngestRate() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __INGESTRATE_ISSET_ID);
  }

  public void setIngestRateIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __INGESTRATE_ISSET_ID, value);
  }

  public double getQueryRate() {
    return this.queryRate;
  }

  public TabletStats setQueryRate(double queryRate) {
    this.queryRate = queryRate;
    setQueryRateIsSet(true);
    return this;
  }

  public void unsetQueryRate() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __QUERYRATE_ISSET_ID);
  }

  /** Returns true if field queryRate is set (has been assigned a value) and false otherwise */
  public boolean isSetQueryRate() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __QUERYRATE_ISSET_ID);
  }

  public void setQueryRateIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __QUERYRATE_ISSET_ID, value);
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case EXTENT:
      if (value == null) {
        unsetExtent();
      } else {
        setExtent((org.apache.accumulo.core.dataImpl.thrift.TKeyExtent)value);
      }
      break;

    case MINORS:
      if (value == null) {
        unsetMinors();
      } else {
        setMinors((ActionStats)value);
      }
      break;

    case NUM_ENTRIES:
      if (value == null) {
        unsetNumEntries();
      } else {
        setNumEntries((java.lang.Long)value);
      }
      break;

    case INGEST_RATE:
      if (value == null) {
        unsetIngestRate();
      } else {
        setIngestRate((java.lang.Double)value);
      }
      break;

    case QUERY_RATE:
      if (value == null) {
        unsetQueryRate();
      } else {
        setQueryRate((java.lang.Double)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case EXTENT:
      return getExtent();

    case MINORS:
      return getMinors();

    case NUM_ENTRIES:
      return getNumEntries();

    case INGEST_RATE:
      return getIngestRate();

    case QUERY_RATE:
      return getQueryRate();

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
    case EXTENT:
      return isSetExtent();
    case MINORS:
      return isSetMinors();
    case NUM_ENTRIES:
      return isSetNumEntries();
    case INGEST_RATE:
      return isSetIngestRate();
    case QUERY_RATE:
      return isSetQueryRate();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TabletStats)
      return this.equals((TabletStats)that);
    return false;
  }

  public boolean equals(TabletStats that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_extent = true && this.isSetExtent();
    boolean that_present_extent = true && that.isSetExtent();
    if (this_present_extent || that_present_extent) {
      if (!(this_present_extent && that_present_extent))
        return false;
      if (!this.extent.equals(that.extent))
        return false;
    }

    boolean this_present_minors = true && this.isSetMinors();
    boolean that_present_minors = true && that.isSetMinors();
    if (this_present_minors || that_present_minors) {
      if (!(this_present_minors && that_present_minors))
        return false;
      if (!this.minors.equals(that.minors))
        return false;
    }

    boolean this_present_numEntries = true;
    boolean that_present_numEntries = true;
    if (this_present_numEntries || that_present_numEntries) {
      if (!(this_present_numEntries && that_present_numEntries))
        return false;
      if (this.numEntries != that.numEntries)
        return false;
    }

    boolean this_present_ingestRate = true;
    boolean that_present_ingestRate = true;
    if (this_present_ingestRate || that_present_ingestRate) {
      if (!(this_present_ingestRate && that_present_ingestRate))
        return false;
      if (this.ingestRate != that.ingestRate)
        return false;
    }

    boolean this_present_queryRate = true;
    boolean that_present_queryRate = true;
    if (this_present_queryRate || that_present_queryRate) {
      if (!(this_present_queryRate && that_present_queryRate))
        return false;
      if (this.queryRate != that.queryRate)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetExtent()) ? 131071 : 524287);
    if (isSetExtent())
      hashCode = hashCode * 8191 + extent.hashCode();

    hashCode = hashCode * 8191 + ((isSetMinors()) ? 131071 : 524287);
    if (isSetMinors())
      hashCode = hashCode * 8191 + minors.hashCode();

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(numEntries);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(ingestRate);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(queryRate);

    return hashCode;
  }

  @Override
  public int compareTo(TabletStats other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetExtent(), other.isSetExtent());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetExtent()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.extent, other.extent);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetMinors(), other.isSetMinors());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMinors()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.minors, other.minors);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetNumEntries(), other.isSetNumEntries());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNumEntries()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.numEntries, other.numEntries);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetIngestRate(), other.isSetIngestRate());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIngestRate()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ingestRate, other.ingestRate);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetQueryRate(), other.isSetQueryRate());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQueryRate()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.queryRate, other.queryRate);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TabletStats(");
    boolean first = true;

    sb.append("extent:");
    if (this.extent == null) {
      sb.append("null");
    } else {
      sb.append(this.extent);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("minors:");
    if (this.minors == null) {
      sb.append("null");
    } else {
      sb.append(this.minors);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("numEntries:");
    sb.append(this.numEntries);
    first = false;
    if (!first) sb.append(", ");
    sb.append("ingestRate:");
    sb.append(this.ingestRate);
    first = false;
    if (!first) sb.append(", ");
    sb.append("queryRate:");
    sb.append(this.queryRate);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (extent != null) {
      extent.validate();
    }
    if (minors != null) {
      minors.validate();
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TabletStatsStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TabletStatsStandardScheme getScheme() {
      return new TabletStatsStandardScheme();
    }
  }

  private static class TabletStatsStandardScheme extends org.apache.thrift.scheme.StandardScheme<TabletStats> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, TabletStats struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // EXTENT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.extent = new org.apache.accumulo.core.dataImpl.thrift.TKeyExtent();
              struct.extent.read(iprot);
              struct.setExtentIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // MINORS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.minors = new ActionStats();
              struct.minors.read(iprot);
              struct.setMinorsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // NUM_ENTRIES
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.numEntries = iprot.readI64();
              struct.setNumEntriesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // INGEST_RATE
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.ingestRate = iprot.readDouble();
              struct.setIngestRateIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // QUERY_RATE
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.queryRate = iprot.readDouble();
              struct.setQueryRateIsSet(true);
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
    public void write(org.apache.thrift.protocol.TProtocol oprot, TabletStats struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.extent != null) {
        oprot.writeFieldBegin(EXTENT_FIELD_DESC);
        struct.extent.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.minors != null) {
        oprot.writeFieldBegin(MINORS_FIELD_DESC);
        struct.minors.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(NUM_ENTRIES_FIELD_DESC);
      oprot.writeI64(struct.numEntries);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(INGEST_RATE_FIELD_DESC);
      oprot.writeDouble(struct.ingestRate);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(QUERY_RATE_FIELD_DESC);
      oprot.writeDouble(struct.queryRate);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TabletStatsTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TabletStatsTupleScheme getScheme() {
      return new TabletStatsTupleScheme();
    }
  }

  private static class TabletStatsTupleScheme extends org.apache.thrift.scheme.TupleScheme<TabletStats> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TabletStats struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetExtent()) {
        optionals.set(0);
      }
      if (struct.isSetMinors()) {
        optionals.set(1);
      }
      if (struct.isSetNumEntries()) {
        optionals.set(2);
      }
      if (struct.isSetIngestRate()) {
        optionals.set(3);
      }
      if (struct.isSetQueryRate()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetExtent()) {
        struct.extent.write(oprot);
      }
      if (struct.isSetMinors()) {
        struct.minors.write(oprot);
      }
      if (struct.isSetNumEntries()) {
        oprot.writeI64(struct.numEntries);
      }
      if (struct.isSetIngestRate()) {
        oprot.writeDouble(struct.ingestRate);
      }
      if (struct.isSetQueryRate()) {
        oprot.writeDouble(struct.queryRate);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TabletStats struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.extent = new org.apache.accumulo.core.dataImpl.thrift.TKeyExtent();
        struct.extent.read(iprot);
        struct.setExtentIsSet(true);
      }
      if (incoming.get(1)) {
        struct.minors = new ActionStats();
        struct.minors.read(iprot);
        struct.setMinorsIsSet(true);
      }
      if (incoming.get(2)) {
        struct.numEntries = iprot.readI64();
        struct.setNumEntriesIsSet(true);
      }
      if (incoming.get(3)) {
        struct.ingestRate = iprot.readDouble();
        struct.setIngestRateIsSet(true);
      }
      if (incoming.get(4)) {
        struct.queryRate = iprot.readDouble();
        struct.setQueryRateIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
  private static void unusedMethod() {}
}

