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

package org.apache.parquet.format;

import java.nio.ByteBuffer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TTransport;

/**
 * TProtocol that interns the strings.
 */
public class InterningProtocol extends TProtocol {

  private final TProtocol delegate;

  public InterningProtocol(TProtocol delegate) {
    super(delegate.getTransport());
    this.delegate = delegate;
  }

  public TTransport getTransport() {
    return delegate.getTransport();
  }

  public void writeMessageBegin(TMessage message) throws TException {
    delegate.writeMessageBegin(message);
  }

  public void writeMessageEnd() throws TException {
    delegate.writeMessageEnd();
  }

  public int hashCode() {
    return delegate.hashCode();
  }

  public void writeStructBegin(TStruct struct) throws TException {
    delegate.writeStructBegin(struct);
  }

  public void writeStructEnd() throws TException {
    delegate.writeStructEnd();
  }

  public void writeFieldBegin(TField field) throws TException {
    delegate.writeFieldBegin(field);
  }

  public void writeFieldEnd() throws TException {
    delegate.writeFieldEnd();
  }

  public void writeFieldStop() throws TException {
    delegate.writeFieldStop();
  }

  public void writeMapBegin(TMap map) throws TException {
    delegate.writeMapBegin(map);
  }

  public void writeMapEnd() throws TException {
    delegate.writeMapEnd();
  }

  public void writeListBegin(TList list) throws TException {
    delegate.writeListBegin(list);
  }

  public void writeListEnd() throws TException {
    delegate.writeListEnd();
  }

  public void writeSetBegin(TSet set) throws TException {
    delegate.writeSetBegin(set);
  }

  public void writeSetEnd() throws TException {
    delegate.writeSetEnd();
  }

  public void writeBool(boolean b) throws TException {
    delegate.writeBool(b);
  }

  public void writeByte(byte b) throws TException {
    delegate.writeByte(b);
  }

  public void writeI16(short i16) throws TException {
    delegate.writeI16(i16);
  }

  public void writeI32(int i32) throws TException {
    delegate.writeI32(i32);
  }

  public void writeI64(long i64) throws TException {
    delegate.writeI64(i64);
  }

  public void writeDouble(double dub) throws TException {
    delegate.writeDouble(dub);
  }

  public void writeString(String str) throws TException {
    delegate.writeString(str);
  }

  public void writeBinary(ByteBuffer buf) throws TException {
    delegate.writeBinary(buf);
  }

  public TMessage readMessageBegin() throws TException {
    return delegate.readMessageBegin();
  }

  public void readMessageEnd() throws TException {
    delegate.readMessageEnd();
  }

  public TStruct readStructBegin() throws TException {
    return delegate.readStructBegin();
  }

  public void readStructEnd() throws TException {
    delegate.readStructEnd();
  }

  public TField readFieldBegin() throws TException {
    return delegate.readFieldBegin();
  }

  public void readFieldEnd() throws TException {
    delegate.readFieldEnd();
  }

  public TMap readMapBegin() throws TException {
    return delegate.readMapBegin();
  }

  public void readMapEnd() throws TException {
    delegate.readMapEnd();
  }

  public TList readListBegin() throws TException {
    return delegate.readListBegin();
  }

  public void readListEnd() throws TException {
    delegate.readListEnd();
  }

  public TSet readSetBegin() throws TException {
    return delegate.readSetBegin();
  }

  public void readSetEnd() throws TException {
    delegate.readSetEnd();
  }

  public boolean equals(Object obj) {
    return delegate.equals(obj);
  }

  public boolean readBool() throws TException {
    return delegate.readBool();
  }

  public byte readByte() throws TException {
    return delegate.readByte();
  }

  public short readI16() throws TException {
    return delegate.readI16();
  }

  public int readI32() throws TException {
    return delegate.readI32();
  }

  public long readI64() throws TException {
    return delegate.readI64();
  }

  public double readDouble() throws TException {
    return delegate.readDouble();
  }

  public String readString() throws TException {
    // this is where we intern the strings
    return delegate.readString().intern();
  }

  public ByteBuffer readBinary() throws TException {
    return delegate.readBinary();
  }

  public void reset() {
    delegate.reset();
  }

  public String toString() {
    return delegate.toString();
  }

  @Override
  public int getMinSerializedSize(byte type) throws TException {
    return delegate.getMinSerializedSize(type);
  }
}
