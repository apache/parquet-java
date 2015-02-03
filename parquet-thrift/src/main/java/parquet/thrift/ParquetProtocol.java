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
package parquet.thrift;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;

/**
 * Allows simple implementation of partial protocols
 *
 * @author Julien Le Dem
 *
 */
public abstract class ParquetProtocol extends TProtocol {

  private String name;

  ParquetProtocol() {
    super(null);
  }

  private String getClassInfo() {
    final Class<? extends ParquetProtocol> clazz = getClass();
    final Method enclosingMethod = clazz.getEnclosingMethod();
    if (enclosingMethod != null) {
      return clazz.getName() + " in " + enclosingMethod.toGenericString();
    }
    return clazz.getName();
  }

  /**
   * @param name a meaningful debugging name for anonymous inner classes
   */
  public ParquetProtocol(String name) {
    super(null);
    this.name = name;
  }

  private UnsupportedOperationException exception() {
    String message = name == null ?
        "in " + getClassInfo() :
        "when we expected " + name + " in " + getClassInfo();
    return new UnsupportedOperationException(new Exception().getStackTrace()[1].getMethodName() + " was called " + message);
  }

  /** WRITE */

  @Override
  public void writeMessageBegin(TMessage message) throws TException {
    throw exception();
  }

  @Override
  public void writeMessageEnd() throws TException {
    throw exception();
  }

  @Override
  public void writeStructBegin(TStruct struct) throws TException {
    throw exception();
  }

  @Override
  public void writeStructEnd() throws TException {
    throw exception();
  }

  @Override
  public void writeFieldBegin(TField field) throws TException {
    throw exception();
  }

  @Override
  public void writeFieldEnd() throws TException {
    throw exception();
  }

  @Override
  public void writeFieldStop() throws TException {
    throw exception();
  }

  @Override
  public void writeMapBegin(TMap map) throws TException {
    throw exception();
  }

  @Override
  public void writeMapEnd() throws TException {
    throw exception();
  }

  @Override
  public void writeListBegin(TList list) throws TException {
    throw exception();
  }

  @Override
  public void writeListEnd() throws TException {
    throw exception();
  }

  @Override
  public void writeSetBegin(TSet set) throws TException {
    throw exception();
  }

  @Override
  public void writeSetEnd() throws TException {
    throw exception();
  }

  @Override
  public void writeBool(boolean b) throws TException {
    throw exception();
  }

  @Override
  public void writeByte(byte b) throws TException {
    throw exception();
  }

  @Override
  public void writeI16(short i16) throws TException {
    throw exception();
  }

  @Override
  public void writeI32(int i32) throws TException {
    throw exception();
  }

  @Override
  public void writeI64(long i64) throws TException {
    throw exception();
  }

  @Override
  public void writeDouble(double dub) throws TException {
    throw exception();
  }

  @Override
  public void writeString(String str) throws TException {
    throw exception();
  }

  @Override
  public void writeBinary(ByteBuffer buf) throws TException {
    throw exception();
  }

  /** READ */

  @Override
  public TMessage readMessageBegin() throws TException {
    throw exception();
  }

  @Override
  public void readMessageEnd() throws TException {
    throw exception();
  }

  @Override
  public TStruct readStructBegin() throws TException {
    throw exception();
  }

  @Override
  public void readStructEnd() throws TException {
    throw exception();
  }

  @Override
  public TField readFieldBegin() throws TException {
    throw exception();
  }

  @Override
  public void readFieldEnd() throws TException {
    throw exception();
  }

  @Override
  public TMap readMapBegin() throws TException {
    throw exception();
  }

  @Override
  public void readMapEnd() throws TException {
    throw exception();
  }

  @Override
  public TList readListBegin() throws TException {
    throw exception();
  }

  @Override
  public void readListEnd() throws TException {
    throw exception();
  }

  @Override
  public TSet readSetBegin() throws TException {
    throw exception();
  }

  @Override
  public void readSetEnd() throws TException {
    throw exception();
  }

  @Override
  public boolean readBool() throws TException {
    throw exception();
  }

  @Override
  public byte readByte() throws TException {
    throw exception();
  }

  @Override
  public short readI16() throws TException {
    throw exception();
  }

  @Override
  public int readI32() throws TException {
    throw exception();
  }

  @Override
  public long readI64() throws TException {
    throw exception();
  }

  @Override
  public double readDouble() throws TException {
    throw exception();
  }

  @Override
  public String readString() throws TException {
    throw exception();
  }

  @Override
  public ByteBuffer readBinary() throws TException {
    throw exception();
  }

}
