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
/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.accumulo.core.replication.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;

@SuppressWarnings({"unchecked", "serial", "rawtypes", "unused"}) public class KeyValues implements org.apache.thrift.TBase<KeyValues, KeyValues._Fields>, java.io.Serializable, Cloneable, Comparable<KeyValues> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("KeyValues");

  private static final org.apache.thrift.protocol.TField KEY_VALUES_FIELD_DESC = new org.apache.thrift.protocol.TField("keyValues", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new KeyValuesStandardSchemeFactory());
    schemes.put(TupleScheme.class, new KeyValuesTupleSchemeFactory());
  }

  public List<org.apache.accumulo.core.data.thrift.TKeyValue> keyValues; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    KEY_VALUES((short)1, "keyValues");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // KEY_VALUES
          return KEY_VALUES;
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
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    @Override
    public short getThriftFieldId() {
      return _thriftId;
    }

    @Override
    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.KEY_VALUES, new org.apache.thrift.meta_data.FieldMetaData("keyValues", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, org.apache.accumulo.core.data.thrift.TKeyValue.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(KeyValues.class, metaDataMap);
  }

  public KeyValues() {
  }

  public KeyValues(
    List<org.apache.accumulo.core.data.thrift.TKeyValue> keyValues)
  {
    this();
    this.keyValues = keyValues;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public KeyValues(KeyValues other) {
    if (other.isSetKeyValues()) {
      List<org.apache.accumulo.core.data.thrift.TKeyValue> __this__keyValues = new ArrayList<org.apache.accumulo.core.data.thrift.TKeyValue>(other.keyValues.size());
      for (org.apache.accumulo.core.data.thrift.TKeyValue other_element : other.keyValues) {
        __this__keyValues.add(new org.apache.accumulo.core.data.thrift.TKeyValue(other_element));
      }
      this.keyValues = __this__keyValues;
    }
  }

  @Override
  public KeyValues deepCopy() {
    return new KeyValues(this);
  }

  @Override
  public void clear() {
    this.keyValues = null;
  }

  public int getKeyValuesSize() {
    return (this.keyValues == null) ? 0 : this.keyValues.size();
  }

  public java.util.Iterator<org.apache.accumulo.core.data.thrift.TKeyValue> getKeyValuesIterator() {
    return (this.keyValues == null) ? null : this.keyValues.iterator();
  }

  public void addToKeyValues(org.apache.accumulo.core.data.thrift.TKeyValue elem) {
    if (this.keyValues == null) {
      this.keyValues = new ArrayList<org.apache.accumulo.core.data.thrift.TKeyValue>();
    }
    this.keyValues.add(elem);
  }

  public List<org.apache.accumulo.core.data.thrift.TKeyValue> getKeyValues() {
    return this.keyValues;
  }

  public KeyValues setKeyValues(List<org.apache.accumulo.core.data.thrift.TKeyValue> keyValues) {
    this.keyValues = keyValues;
    return this;
  }

  public void unsetKeyValues() {
    this.keyValues = null;
  }

  /** Returns true if field keyValues is set (has been assigned a value) and false otherwise */
  public boolean isSetKeyValues() {
    return this.keyValues != null;
  }

  public void setKeyValuesIsSet(boolean value) {
    if (!value) {
      this.keyValues = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case KEY_VALUES:
      if (value == null) {
        unsetKeyValues();
      } else {
        setKeyValues((List<org.apache.accumulo.core.data.thrift.TKeyValue>)value);
      }
      break;

    }
  }

  @Override
  public Object getFieldValue(_Fields field) {
    switch (field) {
    case KEY_VALUES:
      return getKeyValues();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  @Override
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case KEY_VALUES:
      return isSetKeyValues();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof KeyValues)
      return this.equals((KeyValues)that);
    return false;
  }

  public boolean equals(KeyValues that) {
    if (that == null)
      return false;

    boolean this_present_keyValues = true && this.isSetKeyValues();
    boolean that_present_keyValues = true && that.isSetKeyValues();
    if (this_present_keyValues || that_present_keyValues) {
      if (!(this_present_keyValues && that_present_keyValues))
        return false;
      if (!this.keyValues.equals(that.keyValues))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(KeyValues other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetKeyValues()).compareTo(other.isSetKeyValues());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetKeyValues()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.keyValues, other.keyValues);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @Override
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  @Override
  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  @Override
  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("KeyValues(");
    boolean first = true;

    sb.append("keyValues:");
    if (this.keyValues == null) {
      sb.append("null");
    } else {
      sb.append(this.keyValues);
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

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class KeyValuesStandardSchemeFactory implements SchemeFactory {
    @Override
    public KeyValuesStandardScheme getScheme() {
      return new KeyValuesStandardScheme();
    }
  }

  private static class KeyValuesStandardScheme extends StandardScheme<KeyValues> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, KeyValues struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // KEY_VALUES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list8 = iprot.readListBegin();
                struct.keyValues = new ArrayList<org.apache.accumulo.core.data.thrift.TKeyValue>(_list8.size);
                for (int _i9 = 0; _i9 < _list8.size; ++_i9)
                {
                  org.apache.accumulo.core.data.thrift.TKeyValue _elem10;
                  _elem10 = new org.apache.accumulo.core.data.thrift.TKeyValue();
                  _elem10.read(iprot);
                  struct.keyValues.add(_elem10);
                }
                iprot.readListEnd();
              }
              struct.setKeyValuesIsSet(true);
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
    public void write(org.apache.thrift.protocol.TProtocol oprot, KeyValues struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.keyValues != null) {
        oprot.writeFieldBegin(KEY_VALUES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.keyValues.size()));
          for (org.apache.accumulo.core.data.thrift.TKeyValue _iter11 : struct.keyValues)
          {
            _iter11.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class KeyValuesTupleSchemeFactory implements SchemeFactory {
    @Override
    public KeyValuesTupleScheme getScheme() {
      return new KeyValuesTupleScheme();
    }
  }

  private static class KeyValuesTupleScheme extends TupleScheme<KeyValues> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, KeyValues struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetKeyValues()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetKeyValues()) {
        {
          oprot.writeI32(struct.keyValues.size());
          for (org.apache.accumulo.core.data.thrift.TKeyValue _iter12 : struct.keyValues)
          {
            _iter12.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, KeyValues struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list13 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.keyValues = new ArrayList<org.apache.accumulo.core.data.thrift.TKeyValue>(_list13.size);
          for (int _i14 = 0; _i14 < _list13.size; ++_i14)
          {
            org.apache.accumulo.core.data.thrift.TKeyValue _elem15;
            _elem15 = new org.apache.accumulo.core.data.thrift.TKeyValue();
            _elem15.read(iprot);
            struct.keyValues.add(_elem15);
          }
        }
        struct.setKeyValuesIsSet(true);
      }
    }
  }

}

