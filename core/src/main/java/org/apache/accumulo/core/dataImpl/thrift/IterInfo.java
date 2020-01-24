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
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.dataImpl.thrift;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
public class IterInfo implements org.apache.thrift.TBase<IterInfo, IterInfo._Fields>, java.io.Serializable, Cloneable, Comparable<IterInfo> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("IterInfo");

  private static final org.apache.thrift.protocol.TField PRIORITY_FIELD_DESC = new org.apache.thrift.protocol.TField("priority", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField CLASS_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("className", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField ITER_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("iterName", org.apache.thrift.protocol.TType.STRING, (short)3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new IterInfoStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new IterInfoTupleSchemeFactory();

  public int priority; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String className; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String iterName; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    PRIORITY((short)1, "priority"),
    CLASS_NAME((short)2, "className"),
    ITER_NAME((short)3, "iterName");

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
        case 1: // PRIORITY
          return PRIORITY;
        case 2: // CLASS_NAME
          return CLASS_NAME;
        case 3: // ITER_NAME
          return ITER_NAME;
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
  private static final int __PRIORITY_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.PRIORITY, new org.apache.thrift.meta_data.FieldMetaData("priority", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.CLASS_NAME, new org.apache.thrift.meta_data.FieldMetaData("className", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.ITER_NAME, new org.apache.thrift.meta_data.FieldMetaData("iterName", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(IterInfo.class, metaDataMap);
  }

  public IterInfo() {
  }

  public IterInfo(
    int priority,
    java.lang.String className,
    java.lang.String iterName)
  {
    this();
    this.priority = priority;
    setPriorityIsSet(true);
    this.className = className;
    this.iterName = iterName;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public IterInfo(IterInfo other) {
    __isset_bitfield = other.__isset_bitfield;
    this.priority = other.priority;
    if (other.isSetClassName()) {
      this.className = other.className;
    }
    if (other.isSetIterName()) {
      this.iterName = other.iterName;
    }
  }

  public IterInfo deepCopy() {
    return new IterInfo(this);
  }

  @Override
  public void clear() {
    setPriorityIsSet(false);
    this.priority = 0;
    this.className = null;
    this.iterName = null;
  }

  public int getPriority() {
    return this.priority;
  }

  public IterInfo setPriority(int priority) {
    this.priority = priority;
    setPriorityIsSet(true);
    return this;
  }

  public void unsetPriority() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __PRIORITY_ISSET_ID);
  }

  /** Returns true if field priority is set (has been assigned a value) and false otherwise */
  public boolean isSetPriority() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __PRIORITY_ISSET_ID);
  }

  public void setPriorityIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __PRIORITY_ISSET_ID, value);
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getClassName() {
    return this.className;
  }

  public IterInfo setClassName(@org.apache.thrift.annotation.Nullable java.lang.String className) {
    this.className = className;
    return this;
  }

  public void unsetClassName() {
    this.className = null;
  }

  /** Returns true if field className is set (has been assigned a value) and false otherwise */
  public boolean isSetClassName() {
    return this.className != null;
  }

  public void setClassNameIsSet(boolean value) {
    if (!value) {
      this.className = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getIterName() {
    return this.iterName;
  }

  public IterInfo setIterName(@org.apache.thrift.annotation.Nullable java.lang.String iterName) {
    this.iterName = iterName;
    return this;
  }

  public void unsetIterName() {
    this.iterName = null;
  }

  /** Returns true if field iterName is set (has been assigned a value) and false otherwise */
  public boolean isSetIterName() {
    return this.iterName != null;
  }

  public void setIterNameIsSet(boolean value) {
    if (!value) {
      this.iterName = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case PRIORITY:
      if (value == null) {
        unsetPriority();
      } else {
        setPriority((java.lang.Integer)value);
      }
      break;

    case CLASS_NAME:
      if (value == null) {
        unsetClassName();
      } else {
        setClassName((java.lang.String)value);
      }
      break;

    case ITER_NAME:
      if (value == null) {
        unsetIterName();
      } else {
        setIterName((java.lang.String)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case PRIORITY:
      return getPriority();

    case CLASS_NAME:
      return getClassName();

    case ITER_NAME:
      return getIterName();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case PRIORITY:
      return isSetPriority();
    case CLASS_NAME:
      return isSetClassName();
    case ITER_NAME:
      return isSetIterName();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof IterInfo)
      return this.equals((IterInfo)that);
    return false;
  }

  public boolean equals(IterInfo that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_priority = true;
    boolean that_present_priority = true;
    if (this_present_priority || that_present_priority) {
      if (!(this_present_priority && that_present_priority))
        return false;
      if (this.priority != that.priority)
        return false;
    }

    boolean this_present_className = true && this.isSetClassName();
    boolean that_present_className = true && that.isSetClassName();
    if (this_present_className || that_present_className) {
      if (!(this_present_className && that_present_className))
        return false;
      if (!this.className.equals(that.className))
        return false;
    }

    boolean this_present_iterName = true && this.isSetIterName();
    boolean that_present_iterName = true && that.isSetIterName();
    if (this_present_iterName || that_present_iterName) {
      if (!(this_present_iterName && that_present_iterName))
        return false;
      if (!this.iterName.equals(that.iterName))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + priority;

    hashCode = hashCode * 8191 + ((isSetClassName()) ? 131071 : 524287);
    if (isSetClassName())
      hashCode = hashCode * 8191 + className.hashCode();

    hashCode = hashCode * 8191 + ((isSetIterName()) ? 131071 : 524287);
    if (isSetIterName())
      hashCode = hashCode * 8191 + iterName.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(IterInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetPriority()).compareTo(other.isSetPriority());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPriority()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.priority, other.priority);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetClassName()).compareTo(other.isSetClassName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetClassName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.className, other.className);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetIterName()).compareTo(other.isSetIterName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIterName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.iterName, other.iterName);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("IterInfo(");
    boolean first = true;

    sb.append("priority:");
    sb.append(this.priority);
    first = false;
    if (!first) sb.append(", ");
    sb.append("className:");
    if (this.className == null) {
      sb.append("null");
    } else {
      sb.append(this.className);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("iterName:");
    if (this.iterName == null) {
      sb.append("null");
    } else {
      sb.append(this.iterName);
    }
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

  private static class IterInfoStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public IterInfoStandardScheme getScheme() {
      return new IterInfoStandardScheme();
    }
  }

  private static class IterInfoStandardScheme extends org.apache.thrift.scheme.StandardScheme<IterInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, IterInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // PRIORITY
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.priority = iprot.readI32();
              struct.setPriorityIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // CLASS_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.className = iprot.readString();
              struct.setClassNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // ITER_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.iterName = iprot.readString();
              struct.setIterNameIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, IterInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(PRIORITY_FIELD_DESC);
      oprot.writeI32(struct.priority);
      oprot.writeFieldEnd();
      if (struct.className != null) {
        oprot.writeFieldBegin(CLASS_NAME_FIELD_DESC);
        oprot.writeString(struct.className);
        oprot.writeFieldEnd();
      }
      if (struct.iterName != null) {
        oprot.writeFieldBegin(ITER_NAME_FIELD_DESC);
        oprot.writeString(struct.iterName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class IterInfoTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public IterInfoTupleScheme getScheme() {
      return new IterInfoTupleScheme();
    }
  }

  private static class IterInfoTupleScheme extends org.apache.thrift.scheme.TupleScheme<IterInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, IterInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetPriority()) {
        optionals.set(0);
      }
      if (struct.isSetClassName()) {
        optionals.set(1);
      }
      if (struct.isSetIterName()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetPriority()) {
        oprot.writeI32(struct.priority);
      }
      if (struct.isSetClassName()) {
        oprot.writeString(struct.className);
      }
      if (struct.isSetIterName()) {
        oprot.writeString(struct.iterName);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, IterInfo struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.priority = iprot.readI32();
        struct.setPriorityIsSet(true);
      }
      if (incoming.get(1)) {
        struct.className = iprot.readString();
        struct.setClassNameIsSet(true);
      }
      if (incoming.get(2)) {
        struct.iterName = iprot.readString();
        struct.setIterNameIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
  private static void unusedMethod() {}
}

