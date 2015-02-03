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
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;
import parquet.thrift.ParquetProtocol;
import parquet.thrift.struct.ThriftField;

import java.util.ArrayList;
import java.util.List;

/**
 * Generate events for Parquet Protocol for a Thrift Field. The generated events are used for default values
 * when required fields are missing.
 * {@link ProtocolEventsAmender} will use this class to generate events for missing required fields
 */
class DefaultProtocolEventsGenerator {
  public static ParquetProtocol READ_FIELD_STOP = new ParquetProtocol("readFieldBegin() => STOP") {
    final TField stop = new TField("", TType.STOP, (short) 0);

    @Override
    public TField readFieldBegin() throws TException {
      return stop;
    }
  };
  public static ParquetProtocol READ_STRUCT_END = new ParquetProtocol("readStructEnd()") {
    @Override
    public void readStructEnd() throws TException {
    }
  };
  public static ParquetProtocol READ_FIELD_END = new ParquetProtocol("readFieldEnd()") {
    @Override
    public void readFieldEnd() throws TException {
    }
  };
  List createdEvents = new ArrayList<TProtocol>();

  public List<TProtocol> createProtocolEventsForField(ThriftField missingField) {
    TProtocol fieldBegin = new ReadFieldBeginProtocol(missingField);
    createdEvents.add(fieldBegin);

    DefaultEventsVisitor dummyCreatorvisitor = new DefaultEventsVisitor();
    missingField.getType().accept(dummyCreatorvisitor);
    createdEvents.addAll(dummyCreatorvisitor.getEvents());
    createdEvents.add(READ_FIELD_END);
    return createdEvents;
  }

}
