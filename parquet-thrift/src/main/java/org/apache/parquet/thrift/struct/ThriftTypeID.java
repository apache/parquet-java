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
package org.apache.parquet.thrift.struct;

import org.apache.parquet.thrift.struct.ThriftType.BoolType;
import org.apache.parquet.thrift.struct.ThriftType.ByteType;
import org.apache.parquet.thrift.struct.ThriftType.DoubleType;
import org.apache.parquet.thrift.struct.ThriftType.EnumType;
import org.apache.parquet.thrift.struct.ThriftType.I16Type;
import org.apache.parquet.thrift.struct.ThriftType.I32Type;
import org.apache.parquet.thrift.struct.ThriftType.I64Type;
import org.apache.parquet.thrift.struct.ThriftType.ListType;
import org.apache.parquet.thrift.struct.ThriftType.MapType;
import org.apache.parquet.thrift.struct.ThriftType.SetType;
import org.apache.parquet.thrift.struct.ThriftType.StringType;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.parquet.thrift.struct.ThriftType.UUIDType;
import org.apache.thrift.protocol.TType;

/**
 * The list of thrift types
 */
public enum ThriftTypeID {
  STOP(TType.STOP),
  VOID(TType.VOID),
  BOOL(TType.BOOL, BoolType.class),
  BYTE(TType.BYTE, ByteType.class),
  DOUBLE(TType.DOUBLE, DoubleType.class),
  I16(TType.I16, I16Type.class),
  I32(TType.I32, I32Type.class),
  I64(TType.I64, I64Type.class),
  STRING(TType.STRING, StringType.class),
  STRUCT(TType.STRUCT, true, StructType.class),
  MAP(TType.MAP, true, MapType.class),
  SET(TType.SET, true, SetType.class),
  LIST(TType.LIST, true, ListType.class),
  ENUM(TType.ENUM, TType.I32, EnumType.class),
  UUID(TType.UUID, UUIDType.class);

  private static final ThriftTypeID[] types;

  static {
    types = new ThriftTypeID[18];
    for (ThriftTypeID t : ThriftTypeID.values()) {
      // The Thrift Type for Enum is not part of the spec, but is as a Java implementation detail:
      // https://github.com/apache/thrift/blob/5cf71b2beec3c67a4c8452ddabbbc6ae43fff16f/lib/java/src/main/java/org/apache/thrift/protocol/TType.java#L39-L40
      // So we put it at the very end
      if (t.thriftType == -1) {
        types[17] = t;
      } else {
        types[t.thriftType] = t;
      }
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

  private ThriftTypeID(
      byte thriftType, byte serializedThriftType, boolean complex, Class<? extends ThriftType> clss) {
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
