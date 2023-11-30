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
package org.apache.parquet.format.event;

import static org.apache.thrift.protocol.TType.BOOL;
import static org.apache.thrift.protocol.TType.BYTE;
import static org.apache.thrift.protocol.TType.DOUBLE;
import static org.apache.thrift.protocol.TType.I16;
import static org.apache.thrift.protocol.TType.I32;
import static org.apache.thrift.protocol.TType.I64;
import static org.apache.thrift.protocol.TType.LIST;
import static org.apache.thrift.protocol.TType.MAP;
import static org.apache.thrift.protocol.TType.SET;
import static org.apache.thrift.protocol.TType.STRING;
import static org.apache.thrift.protocol.TType.STRUCT;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;

/**
 * receive thrift events of a given type
 */
public abstract class TypedConsumer {

  public abstract static class DoubleConsumer extends TypedConsumer {
    protected DoubleConsumer() {
      super(DOUBLE);
    }

    @Override
    final void read(TProtocol protocol, EventBasedThriftReader reader) throws TException {
      this.consume(protocol.readDouble());
    }

    public abstract void consume(double value);
  }

  public abstract static class ByteConsumer extends TypedConsumer {
    protected ByteConsumer() {
      super(BYTE);
    }

    @Override
    final void read(TProtocol protocol, EventBasedThriftReader reader) throws TException {
      this.consume(protocol.readByte());
    }

    public abstract void consume(byte value);
  }

  public abstract static class BoolConsumer extends TypedConsumer {
    protected BoolConsumer() {
      super(BOOL);
    }

    @Override
    final void read(TProtocol protocol, EventBasedThriftReader reader) throws TException {
      this.consume(protocol.readBool());
    }

    public abstract void consume(boolean value);
  }

  public abstract static class I32Consumer extends TypedConsumer {
    protected I32Consumer() {
      super(I32);
    }

    @Override
    final void read(TProtocol protocol, EventBasedThriftReader reader) throws TException {
      this.consume(protocol.readI32());
    }

    public abstract void consume(int value);
  }

  public abstract static class I64Consumer extends TypedConsumer {
    protected I64Consumer() {
      super(I64);
    }

    final void read(TProtocol protocol, EventBasedThriftReader reader) throws TException {
      this.consume(protocol.readI64());
    }

    public abstract void consume(long value);
  }

  public abstract static class I16Consumer extends TypedConsumer {
    protected I16Consumer() {
      super(I16);
    }

    @Override
    final void read(TProtocol protocol, EventBasedThriftReader reader) throws TException {
      this.consume(protocol.readI16());
    }

    public abstract void consume(short value);
  }

  public abstract static class StringConsumer extends TypedConsumer {
    protected StringConsumer() {
      super(STRING);
    }

    @Override
    final void read(TProtocol protocol, EventBasedThriftReader reader) throws TException {
      this.consume(protocol.readString());
    }

    public abstract void consume(String value);
  }

  public abstract static class StructConsumer extends TypedConsumer {
    protected StructConsumer() {
      super(STRUCT);
    }

    @Override
    final void read(TProtocol protocol, EventBasedThriftReader reader) throws TException {
      this.consumeStruct(protocol, reader);
    }

    /**
     * can either delegate to the reader or read the struct from the protocol
     * reader.readStruct(fieldConsumer);
     *
     * @param protocol the underlying protocol
     * @param reader   the reader to delegate to
     * @throws TException if any thrift related error occurs during the reading
     */
    public abstract void consumeStruct(TProtocol protocol, EventBasedThriftReader reader) throws TException;
  }

  public abstract static class ListConsumer extends TypedConsumer {
    protected ListConsumer() {
      super(LIST);
    }

    @Override
    final void read(TProtocol protocol, EventBasedThriftReader reader) throws TException {
      this.consumeList(protocol, reader, protocol.readListBegin());
      protocol.readListEnd();
    }

    public void consumeList(TProtocol protocol, EventBasedThriftReader reader, TList tList) throws TException {
      reader.readListContent(this, tList);
    }

    /**
     * can either delegate to the reader or read the element from the protocol
     *
     * @param protocol the underlying protocol
     * @param reader   the reader to delegate to
     * @param elemType the type of the element
     * @throws TException if any thrift related error occurs during the reading
     */
    public abstract void consumeElement(TProtocol protocol, EventBasedThriftReader reader, byte elemType)
        throws TException;
  }

  public abstract static class SetConsumer extends TypedConsumer {
    protected SetConsumer() {
      super(SET);
    }

    @Override
    final void read(TProtocol protocol, EventBasedThriftReader reader) throws TException {
      this.consumeSet(protocol, reader, protocol.readSetBegin());
      protocol.readSetEnd();
    }

    public void consumeSet(TProtocol protocol, EventBasedThriftReader reader, TSet tSet) throws TException {
      reader.readSetContent(this, tSet);
    }

    /**
     * can either delegate to the reader or read the set from the protocol
     *
     * @param protocol the underlying protocol
     * @param reader   the reader to delegate to
     * @param elemType the type of the element
     * @throws TException if any thrift related error occurs during the reading
     */
    public abstract void consumeElement(TProtocol protocol, EventBasedThriftReader reader, byte elemType)
        throws TException;
  }

  public abstract static class MapConsumer extends TypedConsumer {
    protected MapConsumer() {
      super(MAP);
    }

    @Override
    final void read(TProtocol protocol, EventBasedThriftReader reader) throws TException {
      this.consumeMap(protocol, reader, protocol.readMapBegin());
      protocol.readMapEnd();
    }

    public void consumeMap(TProtocol protocol, EventBasedThriftReader reader, TMap tMap) throws TException {
      reader.readMapContent(this, tMap);
    }

    /**
     * can either delegate to the reader or read the map entry from the protocol
     *
     * @param protocol  the underlying protocol
     * @param reader    the reader to delegate to
     * @param keyType   the type of the key
     * @param valueType the type of the value
     * @throws TException if any thrift related error occurs during the reading
     */
    public abstract void consumeEntry(
        TProtocol protocol, EventBasedThriftReader reader, byte keyType, byte valueType) throws TException;
  }

  public final byte type;

  private TypedConsumer(byte type) {
    this.type = type;
  }

  public final void read(TProtocol protocol, EventBasedThriftReader reader, byte type) throws TException {
    if (this.type != type) {
      throw new TException("Incorrect type in stream. " + "Expected " + this.type + " but got " + type);
    }
    this.read(protocol, reader);
  }

  abstract void read(TProtocol protocol, EventBasedThriftReader reader) throws TException;
}
