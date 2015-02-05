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
import parquet.thrift.ParquetProtocol;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftTypeID;

public class ReadFieldBeginProtocol extends ParquetProtocol {
  private final ThriftField field;
  private final byte thriftType;

  public ReadFieldBeginProtocol(ThriftField missingField) {
    super("readFieldBegin()");
    this.field = missingField;
    this.thriftType =
            missingField.getType().getType() == ThriftTypeID.ENUM ?
                    ThriftTypeID.I32.getThriftType() : // enums are serialized as I32
                    missingField.getType().getType().getThriftType();
  }

  @Override
  public TField readFieldBegin() throws TException {
    return new TField(field.getName(), thriftType, field.getFieldId());
  }
}
