/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.continuuity.data.transaction.thrift;

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
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TTransaction implements org.apache.thrift.TBase<TTransaction, TTransaction._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TTransaction");

  private static final org.apache.thrift.protocol.TField READ_POINTER_FIELD_DESC = new org.apache.thrift.protocol.TField("readPointer", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField WRITE_POINTER_FIELD_DESC = new org.apache.thrift.protocol.TField("writePointer", org.apache.thrift.protocol.TType.I64, (short)2);
  private static final org.apache.thrift.protocol.TField EXCLUDED_LIST_FIELD_DESC = new org.apache.thrift.protocol.TField("excludedList", org.apache.thrift.protocol.TType.STRING, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TTransactionStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TTransactionTupleSchemeFactory());
  }

  public long readPointer; // required
  public long writePointer; // required
  public ByteBuffer excludedList; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    READ_POINTER((short)1, "readPointer"),
    WRITE_POINTER((short)2, "writePointer"),
    EXCLUDED_LIST((short)3, "excludedList");

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
        case 1: // READ_POINTER
          return READ_POINTER;
        case 2: // WRITE_POINTER
          return WRITE_POINTER;
        case 3: // EXCLUDED_LIST
          return EXCLUDED_LIST;
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

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __READPOINTER_ISSET_ID = 0;
  private static final int __WRITEPOINTER_ISSET_ID = 1;
  private BitSet __isset_bit_vector = new BitSet(2);
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.READ_POINTER, new org.apache.thrift.meta_data.FieldMetaData("readPointer", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.WRITE_POINTER, new org.apache.thrift.meta_data.FieldMetaData("writePointer", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.EXCLUDED_LIST, new org.apache.thrift.meta_data.FieldMetaData("excludedList", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TTransaction.class, metaDataMap);
  }

  public TTransaction() {
  }

  public TTransaction(
    long readPointer,
    long writePointer,
    ByteBuffer excludedList)
  {
    this();
    this.readPointer = readPointer;
    setReadPointerIsSet(true);
    this.writePointer = writePointer;
    setWritePointerIsSet(true);
    this.excludedList = excludedList;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TTransaction(TTransaction other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    this.readPointer = other.readPointer;
    this.writePointer = other.writePointer;
    if (other.isSetExcludedList()) {
      this.excludedList = org.apache.thrift.TBaseHelper.copyBinary(other.excludedList);
;
    }
  }

  public TTransaction deepCopy() {
    return new TTransaction(this);
  }

  @Override
  public void clear() {
    setReadPointerIsSet(false);
    this.readPointer = 0;
    setWritePointerIsSet(false);
    this.writePointer = 0;
    this.excludedList = null;
  }

  public long getReadPointer() {
    return this.readPointer;
  }

  public TTransaction setReadPointer(long readPointer) {
    this.readPointer = readPointer;
    setReadPointerIsSet(true);
    return this;
  }

  public void unsetReadPointer() {
    __isset_bit_vector.clear(__READPOINTER_ISSET_ID);
  }

  /** Returns true if field readPointer is set (has been assigned a value) and false otherwise */
  public boolean isSetReadPointer() {
    return __isset_bit_vector.get(__READPOINTER_ISSET_ID);
  }

  public void setReadPointerIsSet(boolean value) {
    __isset_bit_vector.set(__READPOINTER_ISSET_ID, value);
  }

  public long getWritePointer() {
    return this.writePointer;
  }

  public TTransaction setWritePointer(long writePointer) {
    this.writePointer = writePointer;
    setWritePointerIsSet(true);
    return this;
  }

  public void unsetWritePointer() {
    __isset_bit_vector.clear(__WRITEPOINTER_ISSET_ID);
  }

  /** Returns true if field writePointer is set (has been assigned a value) and false otherwise */
  public boolean isSetWritePointer() {
    return __isset_bit_vector.get(__WRITEPOINTER_ISSET_ID);
  }

  public void setWritePointerIsSet(boolean value) {
    __isset_bit_vector.set(__WRITEPOINTER_ISSET_ID, value);
  }

  public byte[] getExcludedList() {
    setExcludedList(org.apache.thrift.TBaseHelper.rightSize(excludedList));
    return excludedList == null ? null : excludedList.array();
  }

  public ByteBuffer bufferForExcludedList() {
    return excludedList;
  }

  public TTransaction setExcludedList(byte[] excludedList) {
    setExcludedList(excludedList == null ? (ByteBuffer)null : ByteBuffer.wrap(excludedList));
    return this;
  }

  public TTransaction setExcludedList(ByteBuffer excludedList) {
    this.excludedList = excludedList;
    return this;
  }

  public void unsetExcludedList() {
    this.excludedList = null;
  }

  /** Returns true if field excludedList is set (has been assigned a value) and false otherwise */
  public boolean isSetExcludedList() {
    return this.excludedList != null;
  }

  public void setExcludedListIsSet(boolean value) {
    if (!value) {
      this.excludedList = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case READ_POINTER:
      if (value == null) {
        unsetReadPointer();
      } else {
        setReadPointer((Long)value);
      }
      break;

    case WRITE_POINTER:
      if (value == null) {
        unsetWritePointer();
      } else {
        setWritePointer((Long)value);
      }
      break;

    case EXCLUDED_LIST:
      if (value == null) {
        unsetExcludedList();
      } else {
        setExcludedList((ByteBuffer)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case READ_POINTER:
      return Long.valueOf(getReadPointer());

    case WRITE_POINTER:
      return Long.valueOf(getWritePointer());

    case EXCLUDED_LIST:
      return getExcludedList();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case READ_POINTER:
      return isSetReadPointer();
    case WRITE_POINTER:
      return isSetWritePointer();
    case EXCLUDED_LIST:
      return isSetExcludedList();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TTransaction)
      return this.equals((TTransaction)that);
    return false;
  }

  public boolean equals(TTransaction that) {
    if (that == null)
      return false;

    boolean this_present_readPointer = true;
    boolean that_present_readPointer = true;
    if (this_present_readPointer || that_present_readPointer) {
      if (!(this_present_readPointer && that_present_readPointer))
        return false;
      if (this.readPointer != that.readPointer)
        return false;
    }

    boolean this_present_writePointer = true;
    boolean that_present_writePointer = true;
    if (this_present_writePointer || that_present_writePointer) {
      if (!(this_present_writePointer && that_present_writePointer))
        return false;
      if (this.writePointer != that.writePointer)
        return false;
    }

    boolean this_present_excludedList = true && this.isSetExcludedList();
    boolean that_present_excludedList = true && that.isSetExcludedList();
    if (this_present_excludedList || that_present_excludedList) {
      if (!(this_present_excludedList && that_present_excludedList))
        return false;
      if (!this.excludedList.equals(that.excludedList))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(TTransaction other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TTransaction typedOther = (TTransaction)other;

    lastComparison = Boolean.valueOf(isSetReadPointer()).compareTo(typedOther.isSetReadPointer());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetReadPointer()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.readPointer, typedOther.readPointer);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetWritePointer()).compareTo(typedOther.isSetWritePointer());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetWritePointer()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.writePointer, typedOther.writePointer);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetExcludedList()).compareTo(typedOther.isSetExcludedList());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetExcludedList()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.excludedList, typedOther.excludedList);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TTransaction(");
    boolean first = true;

    sb.append("readPointer:");
    sb.append(this.readPointer);
    first = false;
    if (!first) sb.append(", ");
    sb.append("writePointer:");
    sb.append(this.writePointer);
    first = false;
    if (!first) sb.append(", ");
    sb.append("excludedList:");
    if (this.excludedList == null) {
      sb.append("null");
    } else {
      org.apache.thrift.TBaseHelper.toString(this.excludedList, sb);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bit_vector = new BitSet(1);
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TTransactionStandardSchemeFactory implements SchemeFactory {
    public TTransactionStandardScheme getScheme() {
      return new TTransactionStandardScheme();
    }
  }

  private static class TTransactionStandardScheme extends StandardScheme<TTransaction> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TTransaction struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // READ_POINTER
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.readPointer = iprot.readI64();
              struct.setReadPointerIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // WRITE_POINTER
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.writePointer = iprot.readI64();
              struct.setWritePointerIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // EXCLUDED_LIST
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.excludedList = iprot.readBinary();
              struct.setExcludedListIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TTransaction struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(READ_POINTER_FIELD_DESC);
      oprot.writeI64(struct.readPointer);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(WRITE_POINTER_FIELD_DESC);
      oprot.writeI64(struct.writePointer);
      oprot.writeFieldEnd();
      if (struct.excludedList != null) {
        oprot.writeFieldBegin(EXCLUDED_LIST_FIELD_DESC);
        oprot.writeBinary(struct.excludedList);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TTransactionTupleSchemeFactory implements SchemeFactory {
    public TTransactionTupleScheme getScheme() {
      return new TTransactionTupleScheme();
    }
  }

  private static class TTransactionTupleScheme extends TupleScheme<TTransaction> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TTransaction struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetReadPointer()) {
        optionals.set(0);
      }
      if (struct.isSetWritePointer()) {
        optionals.set(1);
      }
      if (struct.isSetExcludedList()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetReadPointer()) {
        oprot.writeI64(struct.readPointer);
      }
      if (struct.isSetWritePointer()) {
        oprot.writeI64(struct.writePointer);
      }
      if (struct.isSetExcludedList()) {
        oprot.writeBinary(struct.excludedList);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TTransaction struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.readPointer = iprot.readI64();
        struct.setReadPointerIsSet(true);
      }
      if (incoming.get(1)) {
        struct.writePointer = iprot.readI64();
        struct.setWritePointerIsSet(true);
      }
      if (incoming.get(2)) {
        struct.excludedList = iprot.readBinary();
        struct.setExcludedListIsSet(true);
      }
    }
  }

}

