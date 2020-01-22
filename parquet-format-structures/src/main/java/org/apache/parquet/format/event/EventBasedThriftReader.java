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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TType;

import org.apache.parquet.format.event.TypedConsumer.ListConsumer;
import org.apache.parquet.format.event.TypedConsumer.MapConsumer;
import org.apache.parquet.format.event.TypedConsumer.SetConsumer;

/**
 * Event based reader for Thrift
 */
public final class EventBasedThriftReader {

  private final TProtocol protocol;

  /**
   * @param protocol the protocol to read from
   */
  public EventBasedThriftReader(TProtocol protocol) {
    this.protocol = protocol;
  }

  /**
   * reads a Struct from the underlying protocol and passes the field events to
   * the FieldConsumer
   * 
   * @param c the field consumer
   * @throws TException if any thrift related error occurs during the reading
   */
  public void readStruct(FieldConsumer c) throws TException {
    protocol.readStructBegin();
    readStructContent(c);
    protocol.readStructEnd();
  }

  /**
   * reads the content of a struct (fields) from the underlying protocol and
   * passes the events to c
   * 
   * @param c the field consumer
   * @throws TException if any thrift related error occurs during the reading
   */
  public void readStructContent(FieldConsumer c) throws TException {
    TField field;
    while (true) {
      field = protocol.readFieldBegin();
      if (field.type == TType.STOP) {
        break;
      }
      c.consumeField(protocol, this, field.id, field.type);
    }
  }

  /**
   * reads the set content (elements) from the underlying protocol and passes the
   * events to the set event consumer
   * 
   * @param eventConsumer the consumer
   * @param tSet the set descriptor
   * @throws TException if any thrift related error occurs during the reading
   */
  public void readSetContent(SetConsumer eventConsumer, TSet tSet) throws TException {
    for (int i = 0; i < tSet.size; i++) {
      eventConsumer.consumeElement(protocol, this, tSet.elemType);
    }
  }

  /**
   * reads the map content (key values) from the underlying protocol and passes
   * the events to the map event consumer
   * 
   * @param eventConsumer the consumer
   * @param tMap the map descriptor
   * @throws TException if any thrift related error occurs during the reading
   */
  public void readMapContent(MapConsumer eventConsumer, TMap tMap) throws TException {
    for (int i = 0; i < tMap.size; i++) {
      eventConsumer.consumeEntry(protocol, this, tMap.keyType, tMap.valueType);
    }
  }

  /**
   * reads a key-value pair
   * 
   * @param keyType the type of the key
   * @param keyConsumer the consumer for the key
   * @param valueType the type of the value
   * @param valueConsumer the consumer for the value
   * @throws TException if any thrift related error occurs during the reading
   */
  public void readMapEntry(byte keyType, TypedConsumer keyConsumer, byte valueType, TypedConsumer valueConsumer)
      throws TException {
    keyConsumer.read(protocol, this, keyType);
    valueConsumer.read(protocol, this, valueType);
  }

  /**
   * reads the list content (elements) from the underlying protocol and passes the
   * events to the list event consumer
   * 
   * @param eventConsumer the consumer
   * @param tList the list descriptor
   * @throws TException if any thrift related error occurs during the reading
   */
  public void readListContent(ListConsumer eventConsumer, TList tList) throws TException {
    for (int i = 0; i < tList.size; i++) {
      eventConsumer.consumeElement(protocol, this, tList.elemType);
    }
  }
}