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
package org.apache.parquet.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

/**
 * Class to read from one protocol and write to another one
 *
 * @author Julien Le Dem
 *
 */
public class ProtocolReadToWrite implements ProtocolPipe {

  /**
   * reads one record from in and writes it to out
   * exceptions are not recoverable as record might be halfway written
   * @param in input protocol
   * @param out output protocol
   * @throws TException
   */
  @Override
  public void readOne(TProtocol in, TProtocol out) throws TException {
    readOneStruct(in, out);
  }

  void readOneValue(TProtocol in, TProtocol out, byte type)
      throws TException {
    switch (type) {
    case TType.LIST:
      readOneList(in, out);
      break;
    case TType.MAP:
      readOneMap(in, out);
      break;
    case TType.SET:
      readOneSet(in, out);
      break;
    case TType.STRUCT:
      readOneStruct(in, out);
      break;
    case TType.STOP:
      break;
    case TType.BOOL:
      out.writeBool(in.readBool());
      break;
    case TType.BYTE:
      out.writeByte(in.readByte());
      break;
    case TType.DOUBLE:
      out.writeDouble(in.readDouble());
      break;
    case TType.I16:
      out.writeI16(in.readI16());
      break;
    case TType.ENUM: // same as i32 => actually never seen in the protocol layer as enums are written as a i32 field
    case TType.I32:
      out.writeI32(in.readI32());
      break;
    case TType.I64:
      out.writeI64(in.readI64());
      break;
    case TType.STRING:
      out.writeBinary(in.readBinary());
      break;
    case TType.VOID:
      break;
    default:
      throw new TException("Unknown type: " + type);
    }
  }

  private void readOneStruct(TProtocol in, TProtocol out) throws TException {
    final TStruct struct = in.readStructBegin();
    out.writeStructBegin(struct);
    TField field;
    while ((field = in.readFieldBegin()).type != TType.STOP) {
      out.writeFieldBegin(field);
      readOneValue(in, out, field.type);
      in.readFieldEnd();
      out.writeFieldEnd();
    }
    out.writeFieldStop();
    in.readStructEnd();
    out.writeStructEnd();
  }

  private void readOneMap(TProtocol in, TProtocol out) throws TException {
    final TMap map = in.readMapBegin();
    out.writeMapBegin(map);
    for (int i = 0; i < map.size; i++) {
      readOneValue(in, out, map.keyType);
      readOneValue(in, out, map.valueType);
    }
    in.readMapEnd();
    out.writeMapEnd();
  }

  private void readOneSet(TProtocol in, TProtocol out) throws TException {
    final TSet set = in.readSetBegin();
    out.writeSetBegin(set);
    readCollectionElements(in, out, set.size, set.elemType);
    in.readSetEnd();
    out.writeSetEnd();
  }

  private void readOneList(TProtocol in, TProtocol out) throws TException {
    final TList list = in.readListBegin();
    out.writeListBegin(list);
    readCollectionElements(in, out, list.size, list.elemType);
    in.readListEnd();
    out.writeListEnd();
  }

  private void readCollectionElements(TProtocol in, TProtocol out,
      final int size, final byte elemType) throws TException {
    for (int i = 0; i < size; i++) {
      readOneValue(in, out, elemType);
    }
  }

}
