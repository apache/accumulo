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
package org.apache.accumulo.core.security.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;

@SuppressWarnings({"unchecked", "serial", "rawtypes", "unused"}) public class TCredentials implements org.apache.thrift.TBase<TCredentials, TCredentials._Fields>, java.io.Serializable, Cloneable, Comparable<TCredentials> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TCredentials");

  private static final org.apache.thrift.protocol.TField PRINCIPAL_FIELD_DESC = new org.apache.thrift.protocol.TField("principal", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField TOKEN_CLASS_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("tokenClassName", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField TOKEN_FIELD_DESC = new org.apache.thrift.protocol.TField("token", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField INSTANCE_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("instanceId", org.apache.thrift.protocol.TType.STRING, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TCredentialsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TCredentialsTupleSchemeFactory());
  }

  public String principal; // required
  public String tokenClassName; // required
  public ByteBuffer token; // required
  public String instanceId; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    PRINCIPAL((short)1, "principal"),
    TOKEN_CLASS_NAME((short)2, "tokenClassName"),
    TOKEN((short)3, "token"),
    INSTANCE_ID((short)4, "instanceId");

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
        case 1: // PRINCIPAL
          return PRINCIPAL;
        case 2: // TOKEN_CLASS_NAME
          return TOKEN_CLASS_NAME;
        case 3: // TOKEN
          return TOKEN;
        case 4: // INSTANCE_ID
          return INSTANCE_ID;
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
    tmpMap.put(_Fields.PRINCIPAL, new org.apache.thrift.meta_data.FieldMetaData("principal", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TOKEN_CLASS_NAME, new org.apache.thrift.meta_data.FieldMetaData("tokenClassName", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TOKEN, new org.apache.thrift.meta_data.FieldMetaData("token", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    tmpMap.put(_Fields.INSTANCE_ID, new org.apache.thrift.meta_data.FieldMetaData("instanceId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TCredentials.class, metaDataMap);
  }

  public TCredentials() {
  }

  public TCredentials(
    String principal,
    String tokenClassName,
    ByteBuffer token,
    String instanceId)
  {
    this();
    this.principal = principal;
    this.tokenClassName = tokenClassName;
    this.token = token;
    this.instanceId = instanceId;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TCredentials(TCredentials other) {
    if (other.isSetPrincipal()) {
      this.principal = other.principal;
    }
    if (other.isSetTokenClassName()) {
      this.tokenClassName = other.tokenClassName;
    }
    if (other.isSetToken()) {
      this.token = org.apache.thrift.TBaseHelper.copyBinary(other.token);
;
    }
    if (other.isSetInstanceId()) {
      this.instanceId = other.instanceId;
    }
  }

  @Override
  public TCredentials deepCopy() {
    return new TCredentials(this);
  }

  @Override
  public void clear() {
    this.principal = null;
    this.tokenClassName = null;
    this.token = null;
    this.instanceId = null;
  }

  public String getPrincipal() {
    return this.principal;
  }

  public TCredentials setPrincipal(String principal) {
    this.principal = principal;
    return this;
  }

  public void unsetPrincipal() {
    this.principal = null;
  }

  /** Returns true if field principal is set (has been assigned a value) and false otherwise */
  public boolean isSetPrincipal() {
    return this.principal != null;
  }

  public void setPrincipalIsSet(boolean value) {
    if (!value) {
      this.principal = null;
    }
  }

  public String getTokenClassName() {
    return this.tokenClassName;
  }

  public TCredentials setTokenClassName(String tokenClassName) {
    this.tokenClassName = tokenClassName;
    return this;
  }

  public void unsetTokenClassName() {
    this.tokenClassName = null;
  }

  /** Returns true if field tokenClassName is set (has been assigned a value) and false otherwise */
  public boolean isSetTokenClassName() {
    return this.tokenClassName != null;
  }

  public void setTokenClassNameIsSet(boolean value) {
    if (!value) {
      this.tokenClassName = null;
    }
  }

  public byte[] getToken() {
    setToken(org.apache.thrift.TBaseHelper.rightSize(token));
    return token == null ? null : token.array();
  }

  public ByteBuffer bufferForToken() {
    return token;
  }

  public TCredentials setToken(byte[] token) {
    setToken(token == null ? (ByteBuffer)null : ByteBuffer.wrap(token));
    return this;
  }

  public TCredentials setToken(ByteBuffer token) {
    this.token = token;
    return this;
  }

  public void unsetToken() {
    this.token = null;
  }

  /** Returns true if field token is set (has been assigned a value) and false otherwise */
  public boolean isSetToken() {
    return this.token != null;
  }

  public void setTokenIsSet(boolean value) {
    if (!value) {
      this.token = null;
    }
  }

  public String getInstanceId() {
    return this.instanceId;
  }

  public TCredentials setInstanceId(String instanceId) {
    this.instanceId = instanceId;
    return this;
  }

  public void unsetInstanceId() {
    this.instanceId = null;
  }

  /** Returns true if field instanceId is set (has been assigned a value) and false otherwise */
  public boolean isSetInstanceId() {
    return this.instanceId != null;
  }

  public void setInstanceIdIsSet(boolean value) {
    if (!value) {
      this.instanceId = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case PRINCIPAL:
      if (value == null) {
        unsetPrincipal();
      } else {
        setPrincipal((String)value);
      }
      break;

    case TOKEN_CLASS_NAME:
      if (value == null) {
        unsetTokenClassName();
      } else {
        setTokenClassName((String)value);
      }
      break;

    case TOKEN:
      if (value == null) {
        unsetToken();
      } else {
        setToken((ByteBuffer)value);
      }
      break;

    case INSTANCE_ID:
      if (value == null) {
        unsetInstanceId();
      } else {
        setInstanceId((String)value);
      }
      break;

    }
  }

  @Override
  public Object getFieldValue(_Fields field) {
    switch (field) {
    case PRINCIPAL:
      return getPrincipal();

    case TOKEN_CLASS_NAME:
      return getTokenClassName();

    case TOKEN:
      return getToken();

    case INSTANCE_ID:
      return getInstanceId();

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
    case PRINCIPAL:
      return isSetPrincipal();
    case TOKEN_CLASS_NAME:
      return isSetTokenClassName();
    case TOKEN:
      return isSetToken();
    case INSTANCE_ID:
      return isSetInstanceId();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TCredentials)
      return this.equals((TCredentials)that);
    return false;
  }

  public boolean equals(TCredentials that) {
    if (that == null)
      return false;

    boolean this_present_principal = true && this.isSetPrincipal();
    boolean that_present_principal = true && that.isSetPrincipal();
    if (this_present_principal || that_present_principal) {
      if (!(this_present_principal && that_present_principal))
        return false;
      if (!this.principal.equals(that.principal))
        return false;
    }

    boolean this_present_tokenClassName = true && this.isSetTokenClassName();
    boolean that_present_tokenClassName = true && that.isSetTokenClassName();
    if (this_present_tokenClassName || that_present_tokenClassName) {
      if (!(this_present_tokenClassName && that_present_tokenClassName))
        return false;
      if (!this.tokenClassName.equals(that.tokenClassName))
        return false;
    }

    boolean this_present_token = true && this.isSetToken();
    boolean that_present_token = true && that.isSetToken();
    if (this_present_token || that_present_token) {
      if (!(this_present_token && that_present_token))
        return false;
      if (!this.token.equals(that.token))
        return false;
    }

    boolean this_present_instanceId = true && this.isSetInstanceId();
    boolean that_present_instanceId = true && that.isSetInstanceId();
    if (this_present_instanceId || that_present_instanceId) {
      if (!(this_present_instanceId && that_present_instanceId))
        return false;
      if (!this.instanceId.equals(that.instanceId))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(TCredentials other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetPrincipal()).compareTo(other.isSetPrincipal());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPrincipal()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.principal, other.principal);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTokenClassName()).compareTo(other.isSetTokenClassName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTokenClassName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tokenClassName, other.tokenClassName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetToken()).compareTo(other.isSetToken());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetToken()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.token, other.token);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetInstanceId()).compareTo(other.isSetInstanceId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetInstanceId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.instanceId, other.instanceId);
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
    StringBuilder sb = new StringBuilder("TCredentials(");
    boolean first = true;

    sb.append("principal:");
    if (this.principal == null) {
      sb.append("null");
    } else {
      sb.append(this.principal);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("tokenClassName:");
    if (this.tokenClassName == null) {
      sb.append("null");
    } else {
      sb.append(this.tokenClassName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("token:");
    if (this.token == null) {
      sb.append("null");
    } else {
      org.apache.thrift.TBaseHelper.toString(this.token, sb);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("instanceId:");
    if (this.instanceId == null) {
      sb.append("null");
    } else {
      sb.append(this.instanceId);
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

  private static class TCredentialsStandardSchemeFactory implements SchemeFactory {
    @Override
    public TCredentialsStandardScheme getScheme() {
      return new TCredentialsStandardScheme();
    }
  }

  private static class TCredentialsStandardScheme extends StandardScheme<TCredentials> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, TCredentials struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // PRINCIPAL
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.principal = iprot.readString();
              struct.setPrincipalIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TOKEN_CLASS_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.tokenClassName = iprot.readString();
              struct.setTokenClassNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // TOKEN
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.token = iprot.readBinary();
              struct.setTokenIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // INSTANCE_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.instanceId = iprot.readString();
              struct.setInstanceIdIsSet(true);
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
    public void write(org.apache.thrift.protocol.TProtocol oprot, TCredentials struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.principal != null) {
        oprot.writeFieldBegin(PRINCIPAL_FIELD_DESC);
        oprot.writeString(struct.principal);
        oprot.writeFieldEnd();
      }
      if (struct.tokenClassName != null) {
        oprot.writeFieldBegin(TOKEN_CLASS_NAME_FIELD_DESC);
        oprot.writeString(struct.tokenClassName);
        oprot.writeFieldEnd();
      }
      if (struct.token != null) {
        oprot.writeFieldBegin(TOKEN_FIELD_DESC);
        oprot.writeBinary(struct.token);
        oprot.writeFieldEnd();
      }
      if (struct.instanceId != null) {
        oprot.writeFieldBegin(INSTANCE_ID_FIELD_DESC);
        oprot.writeString(struct.instanceId);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TCredentialsTupleSchemeFactory implements SchemeFactory {
    @Override
    public TCredentialsTupleScheme getScheme() {
      return new TCredentialsTupleScheme();
    }
  }

  private static class TCredentialsTupleScheme extends TupleScheme<TCredentials> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TCredentials struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetPrincipal()) {
        optionals.set(0);
      }
      if (struct.isSetTokenClassName()) {
        optionals.set(1);
      }
      if (struct.isSetToken()) {
        optionals.set(2);
      }
      if (struct.isSetInstanceId()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetPrincipal()) {
        oprot.writeString(struct.principal);
      }
      if (struct.isSetTokenClassName()) {
        oprot.writeString(struct.tokenClassName);
      }
      if (struct.isSetToken()) {
        oprot.writeBinary(struct.token);
      }
      if (struct.isSetInstanceId()) {
        oprot.writeString(struct.instanceId);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TCredentials struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.principal = iprot.readString();
        struct.setPrincipalIsSet(true);
      }
      if (incoming.get(1)) {
        struct.tokenClassName = iprot.readString();
        struct.setTokenClassNameIsSet(true);
      }
      if (incoming.get(2)) {
        struct.token = iprot.readBinary();
        struct.setTokenIsSet(true);
      }
      if (incoming.get(3)) {
        struct.instanceId = iprot.readString();
        struct.setInstanceIdIsSet(true);
      }
    }
  }

}

