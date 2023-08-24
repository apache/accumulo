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
public class TSummaries implements org.apache.thrift.TBase<TSummaries, TSummaries._Fields>, java.io.Serializable, Cloneable, Comparable<TSummaries> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TSummaries");

  private static final org.apache.thrift.protocol.TField FINISHED_FIELD_DESC = new org.apache.thrift.protocol.TField("finished", org.apache.thrift.protocol.TType.BOOL, (short)1);
  private static final org.apache.thrift.protocol.TField SESSION_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("sessionId", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField TOTAL_FILES_FIELD_DESC = new org.apache.thrift.protocol.TField("totalFiles", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField DELETED_FILES_FIELD_DESC = new org.apache.thrift.protocol.TField("deletedFiles", org.apache.thrift.protocol.TType.I64, (short)4);
  private static final org.apache.thrift.protocol.TField SUMMARIES_FIELD_DESC = new org.apache.thrift.protocol.TField("summaries", org.apache.thrift.protocol.TType.LIST, (short)5);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TSummariesStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TSummariesTupleSchemeFactory();

  private boolean finished; // required
  private long sessionId; // required
  private long totalFiles; // required
  private long deletedFiles; // required
  private @org.apache.thrift.annotation.Nullable java.util.List<TSummary> summaries; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    FINISHED((short)1, "finished"),
    SESSION_ID((short)2, "sessionId"),
    TOTAL_FILES((short)3, "totalFiles"),
    DELETED_FILES((short)4, "deletedFiles"),
    SUMMARIES((short)5, "summaries");

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
        case 1: // FINISHED
          return FINISHED;
        case 2: // SESSION_ID
          return SESSION_ID;
        case 3: // TOTAL_FILES
          return TOTAL_FILES;
        case 4: // DELETED_FILES
          return DELETED_FILES;
        case 5: // SUMMARIES
          return SUMMARIES;
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
  private static final int __FINISHED_ISSET_ID = 0;
  private static final int __SESSIONID_ISSET_ID = 1;
  private static final int __TOTALFILES_ISSET_ID = 2;
  private static final int __DELETEDFILES_ISSET_ID = 3;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.FINISHED, new org.apache.thrift.meta_data.FieldMetaData("finished", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.SESSION_ID, new org.apache.thrift.meta_data.FieldMetaData("sessionId", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.TOTAL_FILES, new org.apache.thrift.meta_data.FieldMetaData("totalFiles", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.DELETED_FILES, new org.apache.thrift.meta_data.FieldMetaData("deletedFiles", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.SUMMARIES, new org.apache.thrift.meta_data.FieldMetaData("summaries", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TSummary.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TSummaries.class, metaDataMap);
  }

  public TSummaries() {
  }

  public TSummaries(
    boolean finished,
    long sessionId,
    long totalFiles,
    long deletedFiles,
    java.util.List<TSummary> summaries)
  {
    this();
    this.finished = finished;
    setFinishedIsSet(true);
    this.sessionId = sessionId;
    setSessionIdIsSet(true);
    this.totalFiles = totalFiles;
    setTotalFilesIsSet(true);
    this.deletedFiles = deletedFiles;
    setDeletedFilesIsSet(true);
    this.summaries = summaries;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TSummaries(TSummaries other) {
    __isset_bitfield = other.__isset_bitfield;
    this.finished = other.finished;
    this.sessionId = other.sessionId;
    this.totalFiles = other.totalFiles;
    this.deletedFiles = other.deletedFiles;
    if (other.isSetSummaries()) {
      java.util.List<TSummary> __this__summaries = new java.util.ArrayList<TSummary>(other.summaries.size());
      for (TSummary other_element : other.summaries) {
        __this__summaries.add(new TSummary(other_element));
      }
      this.summaries = __this__summaries;
    }
  }

  @Override
  public TSummaries deepCopy() {
    return new TSummaries(this);
  }

  @Override
  public void clear() {
    setFinishedIsSet(false);
    this.finished = false;
    setSessionIdIsSet(false);
    this.sessionId = 0;
    setTotalFilesIsSet(false);
    this.totalFiles = 0;
    setDeletedFilesIsSet(false);
    this.deletedFiles = 0;
    this.summaries = null;
  }

  public boolean isFinished() {
    return this.finished;
  }

  public TSummaries setFinished(boolean finished) {
    this.finished = finished;
    setFinishedIsSet(true);
    return this;
  }

  public void unsetFinished() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __FINISHED_ISSET_ID);
  }

  /** Returns true if field finished is set (has been assigned a value) and false otherwise */
  public boolean isSetFinished() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __FINISHED_ISSET_ID);
  }

  public void setFinishedIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __FINISHED_ISSET_ID, value);
  }

  public long getSessionId() {
    return this.sessionId;
  }

  public TSummaries setSessionId(long sessionId) {
    this.sessionId = sessionId;
    setSessionIdIsSet(true);
    return this;
  }

  public void unsetSessionId() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __SESSIONID_ISSET_ID);
  }

  /** Returns true if field sessionId is set (has been assigned a value) and false otherwise */
  public boolean isSetSessionId() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __SESSIONID_ISSET_ID);
  }

  public void setSessionIdIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __SESSIONID_ISSET_ID, value);
  }

  public long getTotalFiles() {
    return this.totalFiles;
  }

  public TSummaries setTotalFiles(long totalFiles) {
    this.totalFiles = totalFiles;
    setTotalFilesIsSet(true);
    return this;
  }

  public void unsetTotalFiles() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __TOTALFILES_ISSET_ID);
  }

  /** Returns true if field totalFiles is set (has been assigned a value) and false otherwise */
  public boolean isSetTotalFiles() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __TOTALFILES_ISSET_ID);
  }

  public void setTotalFilesIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __TOTALFILES_ISSET_ID, value);
  }

  public long getDeletedFiles() {
    return this.deletedFiles;
  }

  public TSummaries setDeletedFiles(long deletedFiles) {
    this.deletedFiles = deletedFiles;
    setDeletedFilesIsSet(true);
    return this;
  }

  public void unsetDeletedFiles() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __DELETEDFILES_ISSET_ID);
  }

  /** Returns true if field deletedFiles is set (has been assigned a value) and false otherwise */
  public boolean isSetDeletedFiles() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __DELETEDFILES_ISSET_ID);
  }

  public void setDeletedFilesIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __DELETEDFILES_ISSET_ID, value);
  }

  public int getSummariesSize() {
    return (this.summaries == null) ? 0 : this.summaries.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<TSummary> getSummariesIterator() {
    return (this.summaries == null) ? null : this.summaries.iterator();
  }

  public void addToSummaries(TSummary elem) {
    if (this.summaries == null) {
      this.summaries = new java.util.ArrayList<TSummary>();
    }
    this.summaries.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<TSummary> getSummaries() {
    return this.summaries;
  }

  public TSummaries setSummaries(@org.apache.thrift.annotation.Nullable java.util.List<TSummary> summaries) {
    this.summaries = summaries;
    return this;
  }

  public void unsetSummaries() {
    this.summaries = null;
  }

  /** Returns true if field summaries is set (has been assigned a value) and false otherwise */
  public boolean isSetSummaries() {
    return this.summaries != null;
  }

  public void setSummariesIsSet(boolean value) {
    if (!value) {
      this.summaries = null;
    }
  }

  @Override
  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case FINISHED:
      if (value == null) {
        unsetFinished();
      } else {
        setFinished((java.lang.Boolean)value);
      }
      break;

    case SESSION_ID:
      if (value == null) {
        unsetSessionId();
      } else {
        setSessionId((java.lang.Long)value);
      }
      break;

    case TOTAL_FILES:
      if (value == null) {
        unsetTotalFiles();
      } else {
        setTotalFiles((java.lang.Long)value);
      }
      break;

    case DELETED_FILES:
      if (value == null) {
        unsetDeletedFiles();
      } else {
        setDeletedFiles((java.lang.Long)value);
      }
      break;

    case SUMMARIES:
      if (value == null) {
        unsetSummaries();
      } else {
        setSummaries((java.util.List<TSummary>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  @Override
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case FINISHED:
      return isFinished();

    case SESSION_ID:
      return getSessionId();

    case TOTAL_FILES:
      return getTotalFiles();

    case DELETED_FILES:
      return getDeletedFiles();

    case SUMMARIES:
      return getSummaries();

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
    case FINISHED:
      return isSetFinished();
    case SESSION_ID:
      return isSetSessionId();
    case TOTAL_FILES:
      return isSetTotalFiles();
    case DELETED_FILES:
      return isSetDeletedFiles();
    case SUMMARIES:
      return isSetSummaries();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TSummaries)
      return this.equals((TSummaries)that);
    return false;
  }

  public boolean equals(TSummaries that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_finished = true;
    boolean that_present_finished = true;
    if (this_present_finished || that_present_finished) {
      if (!(this_present_finished && that_present_finished))
        return false;
      if (this.finished != that.finished)
        return false;
    }

    boolean this_present_sessionId = true;
    boolean that_present_sessionId = true;
    if (this_present_sessionId || that_present_sessionId) {
      if (!(this_present_sessionId && that_present_sessionId))
        return false;
      if (this.sessionId != that.sessionId)
        return false;
    }

    boolean this_present_totalFiles = true;
    boolean that_present_totalFiles = true;
    if (this_present_totalFiles || that_present_totalFiles) {
      if (!(this_present_totalFiles && that_present_totalFiles))
        return false;
      if (this.totalFiles != that.totalFiles)
        return false;
    }

    boolean this_present_deletedFiles = true;
    boolean that_present_deletedFiles = true;
    if (this_present_deletedFiles || that_present_deletedFiles) {
      if (!(this_present_deletedFiles && that_present_deletedFiles))
        return false;
      if (this.deletedFiles != that.deletedFiles)
        return false;
    }

    boolean this_present_summaries = true && this.isSetSummaries();
    boolean that_present_summaries = true && that.isSetSummaries();
    if (this_present_summaries || that_present_summaries) {
      if (!(this_present_summaries && that_present_summaries))
        return false;
      if (!this.summaries.equals(that.summaries))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((finished) ? 131071 : 524287);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(sessionId);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(totalFiles);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(deletedFiles);

    hashCode = hashCode * 8191 + ((isSetSummaries()) ? 131071 : 524287);
    if (isSetSummaries())
      hashCode = hashCode * 8191 + summaries.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TSummaries other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetFinished(), other.isSetFinished());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFinished()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.finished, other.finished);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetSessionId(), other.isSetSessionId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSessionId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sessionId, other.sessionId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetTotalFiles(), other.isSetTotalFiles());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTotalFiles()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.totalFiles, other.totalFiles);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetDeletedFiles(), other.isSetDeletedFiles());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDeletedFiles()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.deletedFiles, other.deletedFiles);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetSummaries(), other.isSetSummaries());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSummaries()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.summaries, other.summaries);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TSummaries(");
    boolean first = true;

    sb.append("finished:");
    sb.append(this.finished);
    first = false;
    if (!first) sb.append(", ");
    sb.append("sessionId:");
    sb.append(this.sessionId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("totalFiles:");
    sb.append(this.totalFiles);
    first = false;
    if (!first) sb.append(", ");
    sb.append("deletedFiles:");
    sb.append(this.deletedFiles);
    first = false;
    if (!first) sb.append(", ");
    sb.append("summaries:");
    if (this.summaries == null) {
      sb.append("null");
    } else {
      sb.append(this.summaries);
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

  private static class TSummariesStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TSummariesStandardScheme getScheme() {
      return new TSummariesStandardScheme();
    }
  }

  private static class TSummariesStandardScheme extends org.apache.thrift.scheme.StandardScheme<TSummaries> {

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot, TSummaries struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // FINISHED
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.finished = iprot.readBool();
              struct.setFinishedIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // SESSION_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.sessionId = iprot.readI64();
              struct.setSessionIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // TOTAL_FILES
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.totalFiles = iprot.readI64();
              struct.setTotalFilesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // DELETED_FILES
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.deletedFiles = iprot.readI64();
              struct.setDeletedFilesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // SUMMARIES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list114 = iprot.readListBegin();
                struct.summaries = new java.util.ArrayList<TSummary>(_list114.size);
                @org.apache.thrift.annotation.Nullable TSummary _elem115;
                for (int _i116 = 0; _i116 < _list114.size; ++_i116)
                {
                  _elem115 = new TSummary();
                  _elem115.read(iprot);
                  struct.summaries.add(_elem115);
                }
                iprot.readListEnd();
              }
              struct.setSummariesIsSet(true);
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
    public void write(org.apache.thrift.protocol.TProtocol oprot, TSummaries struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(FINISHED_FIELD_DESC);
      oprot.writeBool(struct.finished);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(SESSION_ID_FIELD_DESC);
      oprot.writeI64(struct.sessionId);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(TOTAL_FILES_FIELD_DESC);
      oprot.writeI64(struct.totalFiles);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(DELETED_FILES_FIELD_DESC);
      oprot.writeI64(struct.deletedFiles);
      oprot.writeFieldEnd();
      if (struct.summaries != null) {
        oprot.writeFieldBegin(SUMMARIES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.summaries.size()));
          for (TSummary _iter117 : struct.summaries)
          {
            _iter117.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TSummariesTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    @Override
    public TSummariesTupleScheme getScheme() {
      return new TSummariesTupleScheme();
    }
  }

  private static class TSummariesTupleScheme extends org.apache.thrift.scheme.TupleScheme<TSummaries> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TSummaries struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetFinished()) {
        optionals.set(0);
      }
      if (struct.isSetSessionId()) {
        optionals.set(1);
      }
      if (struct.isSetTotalFiles()) {
        optionals.set(2);
      }
      if (struct.isSetDeletedFiles()) {
        optionals.set(3);
      }
      if (struct.isSetSummaries()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetFinished()) {
        oprot.writeBool(struct.finished);
      }
      if (struct.isSetSessionId()) {
        oprot.writeI64(struct.sessionId);
      }
      if (struct.isSetTotalFiles()) {
        oprot.writeI64(struct.totalFiles);
      }
      if (struct.isSetDeletedFiles()) {
        oprot.writeI64(struct.deletedFiles);
      }
      if (struct.isSetSummaries()) {
        {
          oprot.writeI32(struct.summaries.size());
          for (TSummary _iter118 : struct.summaries)
          {
            _iter118.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TSummaries struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.finished = iprot.readBool();
        struct.setFinishedIsSet(true);
      }
      if (incoming.get(1)) {
        struct.sessionId = iprot.readI64();
        struct.setSessionIdIsSet(true);
      }
      if (incoming.get(2)) {
        struct.totalFiles = iprot.readI64();
        struct.setTotalFilesIsSet(true);
      }
      if (incoming.get(3)) {
        struct.deletedFiles = iprot.readI64();
        struct.setDeletedFilesIsSet(true);
      }
      if (incoming.get(4)) {
        {
          org.apache.thrift.protocol.TList _list119 = iprot.readListBegin(org.apache.thrift.protocol.TType.STRUCT);
          struct.summaries = new java.util.ArrayList<TSummary>(_list119.size);
          @org.apache.thrift.annotation.Nullable TSummary _elem120;
          for (int _i121 = 0; _i121 < _list119.size; ++_i121)
          {
            _elem120 = new TSummary();
            _elem120.read(iprot);
            struct.summaries.add(_elem120);
          }
        }
        struct.setSummariesIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
  private static void unusedMethod() {}
}

