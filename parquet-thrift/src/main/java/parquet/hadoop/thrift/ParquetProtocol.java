/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop.thrift;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;

abstract class ParquetProtocol extends TProtocol {

  ParquetProtocol() {
    super(null);
  }

  /** WRITE */

  @Override
  public void writeMessageBegin(TMessage message) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeMessageEnd() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeStructBegin(TStruct struct) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeStructEnd() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeFieldBegin(TField field) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeFieldEnd() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeFieldStop() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeMapBegin(TMap map) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeMapEnd() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeListBegin(TList list) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeListEnd() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeSetBegin(TSet set) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeSetEnd() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeBool(boolean b) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeByte(byte b) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeI16(short i16) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeI32(int i32) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeI64(long i64) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeDouble(double dub) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeString(String str) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void writeBinary(ByteBuffer buf) throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  /** READ */

  @Override
  public TMessage readMessageBegin() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void readMessageEnd() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public TStruct readStructBegin() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void readStructEnd() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public TField readFieldBegin() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void readFieldEnd() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public TMap readMapBegin() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void readMapEnd() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public TList readListBegin() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void readListEnd() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public TSet readSetBegin() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public void readSetEnd() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public boolean readBool() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public byte readByte() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public short readI16() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public int readI32() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public long readI64() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public double readDouble() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public String readString() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

  @Override
  public ByteBuffer readBinary() throws TException {
    throw new UnsupportedOperationException("NYI " + getClass());
  }

}
