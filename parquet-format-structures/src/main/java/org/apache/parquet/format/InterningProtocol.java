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
import java.util.UUID;
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

  @Override
  public TTransport getTransport() {
    return delegate.getTransport();
  }

  @Override
  public void writeMessageBegin(TMessage message) throws TException {
    delegate.writeMessageBegin(message);
  }

  @Override
  public void writeMessageEnd() throws TException {
    delegate.writeMessageEnd();
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public void writeStructBegin(TStruct struct) throws TException {
    delegate.writeStructBegin(struct);
  }

  @Override
  public void writeStructEnd() throws TException {
    delegate.writeStructEnd();
  }

  @Override
  public void writeFieldBegin(TField field) throws TException {
    delegate.writeFieldBegin(field);
  }

  @Override
  public void writeFieldEnd() throws TException {
    delegate.writeFieldEnd();
  }

  @Override
  public void writeFieldStop() throws TException {
    delegate.writeFieldStop();
  }

  @Override
  public void writeMapBegin(TMap map) throws TException {
    delegate.writeMapBegin(map);
  }

  @Override
  public void writeMapEnd() throws TException {
    delegate.writeMapEnd();
  }

  @Override
  public void writeListBegin(TList list) throws TException {
    delegate.writeListBegin(list);
  }

  @Override
  public void writeListEnd() throws TException {
    delegate.writeListEnd();
  }

  @Override
  public void writeSetBegin(TSet set) throws TException {
    delegate.writeSetBegin(set);
  }

  @Override
  public void writeSetEnd() throws TException {
    delegate.writeSetEnd();
  }

  @Override
  public void writeBool(boolean b) throws TException {
    delegate.writeBool(b);
  }

  @Override
  public void writeByte(byte b) throws TException {
    delegate.writeByte(b);
  }

  @Override
  public void writeI16(short i16) throws TException {
    delegate.writeI16(i16);
  }

  @Override
  public void writeI32(int i32) throws TException {
    delegate.writeI32(i32);
  }

  @Override
  public void writeI64(long i64) throws TException {
    delegate.writeI64(i64);
  }

  @Override
  public void writeUuid(UUID uuid) throws TException {
    delegate.writeUuid(uuid);
  }

  @Override
  public void writeDouble(double dub) throws TException {
    delegate.writeDouble(dub);
  }

  @Override
  public void writeString(String str) throws TException {
    delegate.writeString(str);
  }

  @Override
  public void writeBinary(ByteBuffer buf) throws TException {
    delegate.writeBinary(buf);
  }

  @Override
  public TMessage readMessageBegin() throws TException {
    return delegate.readMessageBegin();
  }

  @Override
  public void readMessageEnd() throws TException {
    delegate.readMessageEnd();
  }

  @Override
  public TStruct readStructBegin() throws TException {
    return delegate.readStructBegin();
  }

  @Override
  public void readStructEnd() throws TException {
    delegate.readStructEnd();
  }

  @Override
  public TField readFieldBegin() throws TException {
    return delegate.readFieldBegin();
  }

  @Override
  public void readFieldEnd() throws TException {
    delegate.readFieldEnd();
  }

  @Override
  public TMap readMapBegin() throws TException {
    return delegate.readMapBegin();
  }

  @Override
  public void readMapEnd() throws TException {
    delegate.readMapEnd();
  }

  @Override
  public TList readListBegin() throws TException {
    return delegate.readListBegin();
  }

  @Override
  public void readListEnd() throws TException {
    delegate.readListEnd();
  }

  @Override
  public TSet readSetBegin() throws TException {
    return delegate.readSetBegin();
  }

  @Override
  public void readSetEnd() throws TException {
    delegate.readSetEnd();
  }

  @Override
  public boolean equals(Object obj) {
    return delegate.equals(obj);
  }

  @Override
  public boolean readBool() throws TException {
    return delegate.readBool();
  }

  @Override
  public byte readByte() throws TException {
    return delegate.readByte();
  }

  @Override
  public short readI16() throws TException {
    return delegate.readI16();
  }

  @Override
  public int readI32() throws TException {
    return delegate.readI32();
  }

  @Override
  public long readI64() throws TException {
    return delegate.readI64();
  }

  @Override
  public UUID readUuid() throws TException {
    return delegate.readUuid();
  }

  @Override
  public double readDouble() throws TException {
    return delegate.readDouble();
  }

  @Override
  public String readString() throws TException {
    // this is where we intern the strings
    return delegate.readString().intern();
  }

  @Override
  public ByteBuffer readBinary() throws TException {
    return delegate.readBinary();
  }

  @Override
  public void reset() {
    delegate.reset();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  @Override
  public int getMinSerializedSize(byte type) throws TException {
    return delegate.getMinSerializedSize(type);
  }
}
