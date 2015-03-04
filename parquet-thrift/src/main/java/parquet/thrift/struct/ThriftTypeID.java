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
package parquet.thrift.struct;

import org.apache.thrift.protocol.TType;

import parquet.thrift.struct.ThriftType.BoolType;
import parquet.thrift.struct.ThriftType.ByteType;
import parquet.thrift.struct.ThriftType.DoubleType;
import parquet.thrift.struct.ThriftType.EnumType;
import parquet.thrift.struct.ThriftType.I16Type;
import parquet.thrift.struct.ThriftType.I32Type;
import parquet.thrift.struct.ThriftType.I64Type;
import parquet.thrift.struct.ThriftType.ListType;
import parquet.thrift.struct.ThriftType.MapType;
import parquet.thrift.struct.ThriftType.SetType;
import parquet.thrift.struct.ThriftType.StringType;
import parquet.thrift.struct.ThriftType.StructType;
/**
 * The list of thrift types
 *
 * @author Julien Le Dem
 *
 */
public enum ThriftTypeID {
  STOP (TType.STOP),
  VOID (TType.VOID),
  BOOL (TType.BOOL, BoolType.class),
  BYTE (TType.BYTE, ByteType.class),
  DOUBLE (TType.DOUBLE, DoubleType.class),
  I16 (TType.I16, I16Type.class),
  I32 (TType.I32, I32Type.class),
  I64 (TType.I64, I64Type.class),
  STRING (TType.STRING, StringType.class),
  STRUCT (TType.STRUCT, true, StructType.class),
  MAP (TType.MAP, true, MapType.class),
  SET (TType.SET, true, SetType.class),
  LIST (TType.LIST, true, ListType.class),
  ENUM (TType.ENUM, TType.I32, EnumType.class);

  private static ThriftTypeID[] types = new ThriftTypeID[17];
  static {
    for (ThriftTypeID t : ThriftTypeID.values()) {
      types[t.thriftType] = t;
    }
  }

  private final byte thriftType;
  private final boolean complex;
  private final Class<? extends ThriftType> clss;
  private final byte serializedThriftType;

  private ThriftTypeID(byte thriftType, Class<? extends ThriftType> clss) {
    this(thriftType, thriftType, clss);
  }

  private ThriftTypeID(byte thriftType, byte serializedThriftType, Class<? extends ThriftType> clss) {
    this(thriftType, serializedThriftType, false, clss);
  }

  private ThriftTypeID(byte thriftType) {
    this(thriftType, thriftType, false, null);
  }

  private ThriftTypeID(byte thriftType, boolean complex, Class<? extends ThriftType> clss) {
    this(thriftType, thriftType, complex, clss);
  }

  private ThriftTypeID(byte thriftType, byte serializedThriftType, boolean complex, Class<? extends ThriftType> clss) {
    this.thriftType = thriftType;
    this.serializedThriftType = serializedThriftType;
    this.complex = complex;
    this.clss = clss;
  }

  public byte getThriftType() {
    return thriftType;
  }

  public boolean isComplex() {
    return complex;
  }

  public Class<? extends ThriftType> getType() {
    return clss;
  }

  public static ThriftTypeID fromByte(byte type) {
    return types[type];
  }

  public byte getSerializedThriftType() {
    return serializedThriftType;
  }
}
