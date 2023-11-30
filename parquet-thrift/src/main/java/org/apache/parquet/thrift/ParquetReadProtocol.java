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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ParquetReadProtocol extends ParquetProtocol {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetReadProtocol.class);

  ParquetReadProtocol() {
    super("read");
  }

  private Deque<TProtocol> events = new ArrayDeque<TProtocol>();

  public void add(TProtocol p) {
    events.addLast(p);
  }

  public void addAll(Collection<TProtocol> events) {
    this.events.addAll(events);
  }

  public void clear() {
    this.events.clear();
  }

  private TProtocol next() {
    return events.removeFirst();
  }

  public TMessage readMessageBegin() throws TException {
    LOG.debug("readMessageBegin()");
    return next().readMessageBegin();
  }

  public void readMessageEnd() throws TException {
    LOG.debug("readMessageEnd()");
    next().readMessageEnd();
  }

  public TStruct readStructBegin() throws TException {
    LOG.debug("readStructBegin()");
    return next().readStructBegin();
  }

  public void readStructEnd() throws TException {
    LOG.debug("readStructEnd()");
    next().readStructEnd();
  }

  public TField readFieldBegin() throws TException {
    LOG.debug("readFieldBegin()");
    return next().readFieldBegin();
  }

  public void readFieldEnd() throws TException {
    LOG.debug("readFieldEnd()");
    next().readFieldEnd();
  }

  public TMap readMapBegin() throws TException {
    LOG.debug("readMapBegin()");
    return next().readMapBegin();
  }

  public void readMapEnd() throws TException {
    LOG.debug("readMapEnd()");
    next().readMapEnd();
  }

  public TList readListBegin() throws TException {
    LOG.debug("readListBegin()");
    return next().readListBegin();
  }

  public void readListEnd() throws TException {
    LOG.debug("readListEnd()");
    next().readListEnd();
  }

  public TSet readSetBegin() throws TException {
    LOG.debug("readSetBegin()");
    return next().readSetBegin();
  }

  public void readSetEnd() throws TException {
    LOG.debug("readSetEnd()");
    next().readSetEnd();
  }

  public boolean readBool() throws TException {
    LOG.debug("readBool()");
    return next().readBool();
  }

  public byte readByte() throws TException {
    LOG.debug("readByte()");
    return next().readByte();
  }

  public short readI16() throws TException {
    LOG.debug("readI16()");
    return next().readI16();
  }

  public int readI32() throws TException {
    LOG.debug("readI32()");
    return next().readI32();
  }

  public long readI64() throws TException {
    LOG.debug("readI64()");
    return next().readI64();
  }

  public double readDouble() throws TException {
    LOG.debug("readDouble()");
    return next().readDouble();
  }

  public String readString() throws TException {
    LOG.debug("readString()");
    return next().readString();
  }

  public ByteBuffer readBinary() throws TException {
    LOG.debug("readBinary()");
    return next().readBinary();
  }
}
