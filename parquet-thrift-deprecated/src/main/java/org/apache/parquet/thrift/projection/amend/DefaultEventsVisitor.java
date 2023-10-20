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
package org.apache.parquet.thrift.projection.amend;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.parquet.thrift.ParquetProtocol;
import org.apache.parquet.thrift.struct.ThriftField;
import org.apache.parquet.thrift.struct.ThriftType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Create a dummy events for all required fields according to thrift definition
 */
@Deprecated
class DefaultEventsVisitor implements ThriftType.StateVisitor<Void, Void> {
  List<ParquetProtocol> dummyEvents= new ArrayList<ParquetProtocol>();
  @Override
  public Void visit(ThriftType.MapType mapType, Void v) {
     dummyEvents.add(new ParquetProtocol("readMapBegin()") {
       @Override
       public TMap readMapBegin() throws TException {
         return new TMap();
       }
     });

    dummyEvents.add(new ParquetProtocol("readMapEnd()") {
      @Override
      public void readMapEnd() throws TException {
      }
    });
    return null;
  }

  @Override
  public Void visit(final ThriftType.SetType setType, Void v) {
    dummyEvents.add(new ParquetProtocol("readSetBegin()") {
      @Override
      public TSet readSetBegin() throws TException {
        return new TSet();
      }
    });

    dummyEvents.add(new ParquetProtocol("readSetEnd()") {
      @Override
      public void readSetEnd() throws TException {
      }
    });

    return null;
  }


  @Override
  public Void visit(final ThriftType.ListType listType, Void v) {
    dummyEvents.add(new ParquetProtocol("readListBegin()") {
      @Override
      public TList readListBegin() throws TException {
        return new TList();
      }
    });

    dummyEvents.add(new ParquetProtocol("readListEnd()") {
      @Override
      public void readListEnd() throws TException {
      }
    });

    return null;
  }

  @Override
  public Void visit(ThriftType.StructType structType, Void v) {
    dummyEvents.add(new StructBeginProtocol("struct"));
    List<ThriftField> children = structType.getChildren();
    for (ThriftField child : children) {
      dummyEvents.add(new ReadFieldBeginProtocol(child));
      child.getType().accept(this, null); //currently will create all the attributes in struct, it's safer
      dummyEvents.add(DefaultProtocolEventsGenerator.READ_FIELD_END);
    }
    dummyEvents.add(DefaultProtocolEventsGenerator.READ_FIELD_STOP);
    dummyEvents.add(DefaultProtocolEventsGenerator.READ_STRUCT_END);

    return null;
  }

  @Override
  public Void visit(ThriftType.EnumType enumType, Void v) {
    dummyEvents.add(new ParquetProtocol("readI32() enum") {
      @Override
      public int readI32() throws TException {
        return 0;
      }
    });
    return null;
  }

  @Override
  public Void visit(ThriftType.BoolType boolType, Void v) {
    dummyEvents.add(new ParquetProtocol("readBool()") {
      @Override
      public boolean readBool() throws TException {
        return false;
      }
    });
    return null;
  }


  @Override
  public Void visit(ThriftType.ByteType byteType, Void v) {
    dummyEvents.add(new ParquetProtocol("readByte() int") {
      @Override
      public byte readByte() throws TException {
        return (byte) 0;
      }
    });
    return null;
  }

  @Override
  public Void visit(ThriftType.DoubleType doubleType, Void v) {
    dummyEvents.add(new ParquetProtocol("readDouble()") {
      @Override
      public double readDouble() throws TException {
        return 0.0;
      }
    });
    return null;
  }

  @Override
  public Void visit(ThriftType.I16Type i16Type, Void v) {
    dummyEvents.add(new ParquetProtocol("readI16()") {
      @Override
      public short readI16() throws TException {
        return (short) 0;
      }
    });
    return null;
  }

  @Override
  public Void visit(ThriftType.I32Type i32Type, Void v) {
    dummyEvents.add(new ParquetProtocol("readI32()") {
      @Override
      public int readI32() throws TException {
        return 0;
      }
    });
    return null;
  }

  @Override
  public Void visit(ThriftType.I64Type i64Type, Void v) {
    dummyEvents.add(new ParquetProtocol("readI64()") {
      @Override
      public long readI64() throws TException {
        return 0;
      }
    });
    return null;
  }

  @Override
  public Void visit(ThriftType.StringType stringType, Void v) {
    dummyEvents.add(new StringProtocol(""));
    return null;
  }

  public List<ParquetProtocol> getEvents() {
    return dummyEvents;
  }

  private static class StructBeginProtocol extends ParquetProtocol {
    private final String structName;

    public StructBeginProtocol(String structName) {
      super("readStructBegin()");
      this.structName=structName;
    }
    @Override
    public TStruct readStructBegin() throws TException {
      return new TStruct(structName);
    }
  }

  public static class StringProtocol extends ParquetProtocol {

    private final String str;

    public StringProtocol(String str) {
      super("readString() binary");
      this.str = str;
    }

    @Override
    public String readString() throws TException {
      return str;
    }

    @Override
    public ByteBuffer readBinary() throws TException {
      return ByteBuffer.wrap("str".getBytes());
    }
  }
}
