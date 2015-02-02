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
package parquet.thrift.projection.amend;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftType.StructType;

import java.util.*;

/**
 * fill in default value for required fields in TProtocols after projection is specified.
 *
 * @author Tianshuo Deng
 */
public class ProtocolEventsAmender {
  List<TProtocol> rootEvents;
  List<TProtocol> fixedEvents = new ArrayList<TProtocol>();

  public ProtocolEventsAmender(List<TProtocol> rootEvents) {
    this.rootEvents = rootEvents;
  }

  /**
   * Given a thrift definition, protocols events, it checks all the required fields,
   * and create default value if a required field is missing
   *
   * @param recordThriftType the Thrift Struct definition for events
   * @return
   * @throws TException
   */
  public List<TProtocol> amendMissingRequiredFields(StructType recordThriftType) throws TException {
    Iterator<TProtocol> protocolIter = rootEvents.iterator();
    checkStruct(protocolIter, recordThriftType);
    return fixedEvents;
  }

  private TProtocol acceptProtocol(TProtocol p) {
    this.fixedEvents.add(p);
    return p;
  }

  private void checkStruct(Iterator<TProtocol> eventIter, ThriftType.StructType thriftStructType) throws TException {
    TStruct tStruct = acceptProtocol(eventIter.next()).readStructBegin();

    List<ThriftField> childrenFields = thriftStructType.getChildren();

    Set<Short> includedFieldsIds = new HashSet<Short>();

    while (true) {
      TProtocol next = eventIter.next();
      TField field = next.readFieldBegin();

      if (isStopField(field))
        break;
      acceptProtocol(next);
      includedFieldsIds.add(field.id);
      ThriftField fieldDefinition = thriftStructType.getChildById(field.id);
      checkField(field.type, eventIter, fieldDefinition);
      acceptProtocol(eventIter.next()).readFieldEnd();
    }

    for (ThriftField requiredField : childrenFields) {
      if (!isRequired(requiredField)) {
        continue;
      }
      if (!includedFieldsIds.contains(requiredField.getFieldId())) {
        fixedEvents.addAll(new DefaultProtocolEventsGenerator().createProtocolEventsForField(requiredField));
      }
    }

    acceptProtocol(DefaultProtocolEventsGenerator.READ_FIELD_STOP);
    acceptProtocol(eventIter.next()).readStructEnd();
  }

  private void checkField(byte type, Iterator<TProtocol> eventIter, ThriftField fieldDefinition) throws TException {
    switch (type) {
      case TType.STRUCT:
        checkStruct(eventIter, (ThriftType.StructType) fieldDefinition.getType());
        return;
      case TType.LIST:
        checkList(eventIter, fieldDefinition);
        return;
      case TType.MAP:
        checkMap(eventIter, fieldDefinition);
        return;
      case TType.SET:
        checkSet(eventIter, fieldDefinition);
        return;
    }
    checkPrimitiveField(type, eventIter);
  }

  /**
   * check each element of the Set, make sure all the element contain required fields
   *
   * @param eventIter
   * @param setFieldDefinition
   * @throws TException
   */
  private void checkSet(Iterator<TProtocol> eventIter, ThriftField setFieldDefinition) throws TException {
    TSet thriftSet = acceptProtocol(eventIter.next()).readSetBegin();
    ThriftField elementFieldDefinition = ((ThriftType.SetType) setFieldDefinition.getType()).getValues();
    int setSize = thriftSet.size;
    for (int i = 0; i < setSize; i++) {
      checkField(thriftSet.elemType, eventIter, elementFieldDefinition);
    }
    acceptProtocol(eventIter.next()).readSetEnd();
  }

  private void checkMap(Iterator<TProtocol> eventIter, ThriftField mapFieldForWriting) throws TException {
    TMap thriftMap = acceptProtocol(eventIter.next()).readMapBegin();
    ThriftField keyFieldForWriting = ((ThriftType.MapType) mapFieldForWriting.getType()).getKey();
    ThriftField valueFieldForWriting = ((ThriftType.MapType) mapFieldForWriting.getType()).getValue();

    int mapSize = thriftMap.size;
    for (int i = 0; i < mapSize; i++) {
      //readkey
      checkField(thriftMap.keyType, eventIter, keyFieldForWriting);
      //readValue
      checkField(thriftMap.valueType, eventIter, valueFieldForWriting);
    }
    acceptProtocol(eventIter.next()).readMapEnd();
  }

  private void checkList(Iterator<TProtocol> eventIter, ThriftField listFieldUsedForWriting) throws TException {
    ThriftField valueFieldForWriting = ((ThriftType.ListType) listFieldUsedForWriting.getType()).getValues();
    TList thriftList = acceptProtocol(eventIter.next()).readListBegin();
    int listSize = thriftList.size;
    for (int i = 0; i < listSize; i++) {
      checkField(thriftList.elemType, eventIter, valueFieldForWriting);
    }
    acceptProtocol(eventIter.next()).readListEnd();
  }

  /**
   * Once reached primitive field, just the copy the event.
   *
   * @param type
   * @param eventIter
   * @throws TException
   */
  private void checkPrimitiveField(byte type, Iterator<TProtocol> eventIter) throws TException {
    acceptProtocol(eventIter.next());
  }

  private boolean isStopField(TField field) {
    return field.type == TType.STOP;
  }

  private boolean isRequired(ThriftField requiredField) {
    return requiredField.getRequirement() == ThriftField.Requirement.REQUIRED;
  }

}
